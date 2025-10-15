/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * Copyright (c) 2014-2021,  Regents of the University of California,
 *                           Arizona Board of Regents,
 *                           Colorado State University,
 *                           University Pierre & Marie Curie, Sorbonne University,
 *                           Washington University in St. Louis,
 *                           Beijing Institute of Technology,
 *                           The University of Memphis.
 *
 * This file is part of NFD (Named Data Networking Forwarding Daemon).
 * See AUTHORS.md for complete list of NFD authors and contributors.
 *
 * NFD is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * NFD is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * NFD, e.g., in COPYING.md file.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "forwarder.hpp"

#include "algorithm.hpp"
#include "best-route-strategy.hpp"
#include "scope-prefix.hpp"
#include "strategy.hpp"
#include "common/global.hpp"
#include "common/logger.hpp"
#include "table/cleanup.hpp"

#include <ndn-cxx/lp/pit-token.hpp>
#include <ndn-cxx/lp/tags.hpp>

#include "face/null-face.hpp"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

#include "ns3/simulator.h"


namespace nfd {

NFD_LOG_INIT(Forwarder);

const std::string CFG_FORWARDER = "forwarder";

static Name
getDefaultStrategyName()
{
  return fw::BestRouteStrategy::getStrategyName();
}

Forwarder::Forwarder(FaceTable& faceTable)
  : m_faceTable(faceTable)
  , m_unsolicitedDataPolicy(make_unique<fw::DefaultUnsolicitedDataPolicy>())
  , m_fib(m_nameTree)
  , m_pit(m_nameTree)
  , m_measurements(m_nameTree)
  , m_strategyChoice(*this)
  , m_csFace(face::makeNullFace(FaceUri("contentstore://")))
{
  m_faceTable.addReserved(m_csFace, face::FACEID_CONTENT_STORE);

  m_faceTable.afterAdd.connect([this] (const Face& face) {
    face.afterReceiveInterest.connect(
      [this, &face] (const Interest& interest, const EndpointId& endpointId) {
        this->onIncomingInterest(interest, FaceEndpoint(const_cast<Face&>(face), endpointId));
      });
    face.afterReceiveData.connect(
      [this, &face] (const Data& data, const EndpointId& endpointId) {
        this->onIncomingData(data, FaceEndpoint(const_cast<Face&>(face), endpointId));
      });
    face.afterReceiveNack.connect(
      [this, &face] (const lp::Nack& nack, const EndpointId& endpointId) {
        this->onIncomingNack(nack, FaceEndpoint(const_cast<Face&>(face), endpointId));
      });
    face.onDroppedInterest.connect(
      [this, &face] (const Interest& interest) {
        this->onDroppedInterest(interest, const_cast<Face&>(face));
      });
  });

  m_faceTable.beforeRemove.connect([this] (const Face& face) {
    cleanupOnFaceRemoval(m_nameTree, m_fib, m_pit, face);
  });

  m_fib.afterNewNextHop.connect([this] (const Name& prefix, const fib::NextHop& nextHop) {
    this->onNewNextHop(prefix, nextHop);
  });

  m_strategyChoice.setDefaultStrategy(getDefaultStrategyName());

  m_SDservTracker.clear();
}

Forwarder::~Forwarder() = default;

void
Forwarder::onIncomingInterest(const Interest& interest, const FaceEndpoint& ingress)
{
  // get first part of the name, if it equals /nesco or /nescoscopt or /orchA or /orchB and it's coming from a local face (our application), then print INFO message
  // this effectively counts the number of interest packets that are generated at the consumer (including the custom forwarders)
  ndn::Name simpleName;
  simpleName = (interest.getName()).getPrefix(1); // get just the first component of the name, and convert to Uri string
  std::string simpleStringName = simpleName.toUri();
  if (simpleStringName == "/nesco" || simpleStringName == "/nescoSCOPT" || simpleStringName == "/orchA" || simpleStringName == "/orchB")
  {
    if (ingress.face.getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
    {
      NFD_LOG_INFO("     CABEEE: onIncomingInterestFromApp (from consuming application only) =" << " name=" << interest.getName());
    }
    else
    {
      NFD_LOG_INFO("     CABEEE: onIncomingInterestFromFace (from another NFD node on a physical face) =" << " name=" << interest.getName());
    }
  }

  // receive Interest
  NFD_LOG_DEBUG("onIncomingInterest in=" << ingress << " interest=" << interest.getName());
  interest.setTag(make_shared<lp::IncomingFaceIdTag>(ingress.face.getId()));
  ++m_counters.nInInterests;

  // drop if HopLimit zero, decrement otherwise (if present)
  if (interest.getHopLimit()) {
    if (*interest.getHopLimit() == 0) {
      NFD_LOG_DEBUG("onIncomingInterest in=" << ingress << " interest=" << interest.getName()
                    << " hop-limit=0");
      ++ingress.face.getCounters().nInHopLimitZero;
      // drop
      return;
    }
    const_cast<Interest&>(interest).setHopLimit(*interest.getHopLimit() - 1);
  }

  // /localhost scope control
  bool isViolatingLocalhost = ingress.face.getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL &&
                              scope_prefix::LOCALHOST.isPrefixOf(interest.getName());
  if (isViolatingLocalhost) {
    NFD_LOG_DEBUG("onIncomingInterest in=" << ingress
                  << " interest=" << interest.getName() << " violates /localhost");
    // drop
    return;
  }

  // detect duplicate Nonce with Dead Nonce List
  bool hasDuplicateNonceInDnl = m_deadNonceList.has(interest.getName(), interest.getNonce());
  if (hasDuplicateNonceInDnl) {
    // goto Interest loop pipeline
    this->onInterestLoop(interest, ingress);
    return;
  }

  // strip forwarding hint if Interest has reached producer region
  if (!interest.getForwardingHint().empty() &&
      m_networkRegionTable.isInProducerRegion(interest.getForwardingHint())) {
    NFD_LOG_DEBUG("onIncomingInterest in=" << ingress
                  << " interest=" << interest.getName() << " reaching-producer-region");
    const_cast<Interest&>(interest).setForwardingHint({});
  }

  // PIT insert
  shared_ptr<pit::Entry> pitEntry = m_pit.insert(interest).first;

  // detect duplicate Nonce in PIT entry
  int dnw = fw::findDuplicateNonce(*pitEntry, interest.getNonce(), ingress.face);
  bool hasDuplicateNonceInPit = dnw != fw::DUPLICATE_NONCE_NONE;
  if (ingress.face.getLinkType() == ndn::nfd::LINK_TYPE_POINT_TO_POINT) {
    // for p2p face: duplicate Nonce from same incoming face is not loop
    hasDuplicateNonceInPit = hasDuplicateNonceInPit && !(dnw & fw::DUPLICATE_NONCE_IN_SAME);
  }
  if (hasDuplicateNonceInPit) {
    // goto Interest loop pipeline
    this->onInterestLoop(interest, ingress);
    m_strategyChoice.findEffectiveStrategy(*pitEntry).afterReceiveLoopedInterest(ingress, interest, *pitEntry);
    return;
  }

  // is pending?
  if (!pitEntry->hasInRecords()) {
    m_cs.find(interest,
              [=] (const Interest& i, const Data& d) { onContentStoreHit(i, ingress, pitEntry, d); },
              [=] (const Interest& i) { onContentStoreMiss(i, ingress, pitEntry); });
  }
  else {
    this->onContentStoreMiss(interest, ingress, pitEntry);
  }
}

void
Forwarder::onInterestLoop(const Interest& interest, const FaceEndpoint& ingress)
{
  // if multi-access or ad hoc face, drop
  if (ingress.face.getLinkType() != ndn::nfd::LINK_TYPE_POINT_TO_POINT) {
    NFD_LOG_DEBUG("onInterestLoop in=" << ingress
                  << " interest=" << interest.getName() << " drop");
    return;
  }

  NFD_LOG_DEBUG("onInterestLoop in=" << ingress << " interest=" << interest.getName()
                << " send-Nack-duplicate");

  // send Nack with reason=DUPLICATE
  // note: Don't enter outgoing Nack pipeline because it needs an in-record.
  lp::Nack nack(interest);
  nack.setReason(lp::NackReason::DUPLICATE);
  ingress.face.sendNack(nack);
}

void
Forwarder::onContentStoreMiss(const Interest& interest, const FaceEndpoint& ingress,
                              const shared_ptr<pit::Entry>& pitEntry)
{
  NFD_LOG_DEBUG("onContentStoreMiss interest=" << interest.getName());
  ++m_counters.nCsMisses;
  afterCsMiss(interest);

  ndn::Name simpleName;
  simpleName = (interest.getName()).getPrefix(1); // get just the first component of the name
  std::string simpleStringName = simpleName.toUri(); // and covert to Uri string


  // attach HopLimit if configured and not present in Interest
  if (m_config.defaultHopLimit > 0 && !interest.getHopLimit()) {
    const_cast<Interest&>(interest).setHopLimit(m_config.defaultHopLimit);
  }

  // insert in-record
  pitEntry->insertOrUpdateInRecord(ingress.face, interest);

  // set PIT expiry timer to the time that the last PIT in-record expires
  auto lastExpiring = std::max_element(pitEntry->in_begin(), pitEntry->in_end(),
                                       [] (const auto& a, const auto& b) {
                                         return a.getExpiry() < b.getExpiry();
                                       });
  auto lastExpiryFromNow = lastExpiring->getExpiry() - time::steady_clock::now();
  this->setExpiryTimer(pitEntry, time::duration_cast<time::milliseconds>(lastExpiryFromNow));


  if (interest.getName().getPrefix(-1).getSubName(1,1).toUri() == "/serviceDiscovery") // remove last comopnent (app parameter), then starting at component 1, get 1 component (where /serviceDiscovery would be)
  {
    // Determine which interfaces can be used to reach the named service, and generate a new interest for the name service out of each of the faces (even local ones for teh APP).
    // keep track of which ones have left, so that when data packets arrive, I can evaluate their EFTs. Once all have returned, generate data packet with lowest EFT.
    ndn::Name simpleName;
    simpleName = (interest.getName()).getPrefix(-1); // remove the last component of the name (the parameter digest) so we have just the raw name
    simpleName = simpleName.getSubName(2,1); // remove the zeroeth component of the name (/nesco), and the first component of the name (/serviceDiscovery). starting at component 2, keep 1 component
    std::string rxedInterestName = simpleName.toUri();
    //NFD_LOG_DEBUG("NFD ServiceDiscovery rxedInterestName -> simpleName: " << rxedInterestName << '\n');
    NFD_LOG_DEBUG("NFD ServiceDiscovery received an interest on face " << ingress.face.getId() << " that needs to be distributed to other faces.\n");



    //look at FIB, and see if this service is reachable out of any other faces. If so, send interest out through each face.
    for (fib::Fib::const_iterator fib_iterator = m_fib.begin(); fib_iterator != m_fib.end(); ++fib_iterator)
    {
      /*
      NFD_LOG_DEBUG("\nCABEEEserviceDiscovery, looking at new fib entry");
      ndn::Name entryName;
      entryName = fib_iterator->getPrefix();
      //entryName = entryName.getSubName(1,1); // starting at component 1, get 1 component (/serviceDiscovery only)
      std::string entryString = entryName.toUri();
      NFD_LOG_DEBUG("CABEEEserviceDiscovery, fib entryString: " << entryString << " can be reached on following faceIDs: ");
      const fib::NextHopList& hopList = fib_iterator->getNextHops();
      for (nfd::fib::NextHopList::const_iterator hop_iterator = hopList.begin(); hop_iterator != hopList.end(); ++hop_iterator)
          NFD_LOG_DEBUG("faceID: " << hop_iterator->getFace().getId() << ", ");
      */


      ndn::Name name1;
      name1 = fib_iterator->getPrefix();
      name1 = name1.getSubName(1,1); // starting at component 1, get 1 component (/serviceDiscovery only)
      std::string name1String = name1.toUri();
      //NFD_LOG_DEBUG("CABEEEserviceDiscovery, fib name1String component 1 is " << name1String);

      auto dagParameterFromInterest = interest.getApplicationParameters();
      std::string dagString = std::string(reinterpret_cast<const char*>(dagParameterFromInterest.value()), dagParameterFromInterest.value_size());
      json dagObject = json::parse(dagString);
      ndn::Name name2;
      name2 = fib_iterator->getPrefix();
      name2 = name2.getSubName(2,1); // starting at component 2, get 1 component (name2 name only)
      std::string name2String = name2.toUri();
      //NFD_LOG_DEBUG("CABEEEserviceDiscovery, fib name2String component 2 is "<< name2String << '\n');

      //NFD_LOG_DEBUG("CABEEEserviceDiscovery, interest head is "<< dagObject["head"]);
      // only generate new serviceDiscovery interest if the incoming interest is for /serviceDiscovery, and this fib entry is for the service the interest is for
      if (name1String == "/serviceDiscovery" && name2String == dagObject["head"])
      {
        //NFD_LOG_DEBUG("CABEEEserviceDiscovery, fib entry has matching /serviceDiscovery/serviceX name\n");
        if (fib_iterator->hasNextHops())
        {
          //NFD_LOG_DEBUG("CABEEEserviceDiscovery, fib_iterator has nextHops, iterating to all faces...\n");
          // figure out the faceID of all the nexthops in the list, ?and send interest to ones that are NOT local?
          //fib::NextHopList hopList = fib_iterator->getNextHops();
          const fib::NextHopList& hopList = fib_iterator->getNextHops();
          //for (auto &hop_iterator : hopList)
          for (nfd::fib::NextHopList::const_iterator hop_iterator = hopList.begin(); hop_iterator != hopList.end(); ++hop_iterator)
          //for (nfd::fib::NextHopList::const_iterator hop_iterator = fib_iterator->getNextHops().begin(); hop_iterator != fib_iterator->getNextHops().end(); ++hop_iterator)
          {
            //NFD_LOG_DEBUG("CABEEEserviceDiscovery, looking at all hops for this fib entry\n");
            //Face thisFace = hop_iterator->getFace();
            //if (thisFace.getScope() != ndn::nfd::FACE_SCOPE_NON_LOCAL)
            //{
              //thisFace.sendInterest(interestOPT);
            //}
            if (hop_iterator->getFace().getId() != ingress.face.getId()) // do not send new interest out of the incoming face (avoid loops).
            {
              // keep track of which interests have left with a JSON data structure
              // name, faceid, interest generated, data received, delay & EFT out of that faceid.
/*
SDservTracker = {
  "service1": {
      "faceID1": {
          "intTx": 0,
          "dataRx": 0,
          "linkDelay": -1,
          "EFT": -1
      }
      "faceID2": {
          "intTx": 0,
          "dataRx": 0,
          "linkDelay": -1,
          "EFT": -1
      }
  }
}
*/
/*
SDservTracker = {
  "service1 (or use full pDAG name?)": {
      "faceIN": faceID123, -> but this could be a list of faces, as we just saw! The interests could come from more than one place! We need to eventually generate data packets to all recorded input (downstream) faces!
      "faceOUT": {
          "faceID1": {
              "intTx": 0,
              "dataRx": 0,
              "linkDelay": -1,
              "EFT": -1
          }
          "faceID2": {
              "intTx": 0,
              "dataRx": 0,
              "linkDelay": -1,
              "EFT": -1
          }
      }
  }
}
*/
              // if name entry doesn't exist, create it
              if (!m_SDservTracker.contains(name2String))
              {
                m_SDservTracker[name2String]["faceIN"] = ingress.face.getId();
                m_SDservTracker[name2String]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["intTx"] = 0;
                m_SDservTracker[name2String]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["dataRx"] = 0;
                m_SDservTracker[name2String]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["linkDelay"] = -1;
                m_SDservTracker[name2String]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["EFT"] = -1;
              }
              // if faceID in this name entry doesn't exist, create it (mark interest generated as False and data received as False, delay as -1, EFT as -1).
              if (!m_SDservTracker[name2String]["faceOUT"].contains(std::to_string(hop_iterator->getFace().getId())))
              {
                m_SDservTracker[name2String]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["intTx"] = 0;
                m_SDservTracker[name2String]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["dataRx"] = 0;
                m_SDservTracker[name2String]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["linkDelay"] = -1;
                m_SDservTracker[name2String]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["EFT"] = -1;
              }
              if (m_SDservTracker[name2String]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["intTx"] != 0)
              {
                // if faceID exists, and interest is marked as generated, report an error (should never happen? I'm seeing cases where this DOES happen, and it seems to be expected).
                // for example, when N1/S3 is requestion S1, and we already had received interests for S1 from N2/S3.
                // Just let it add the PIT entry and drop the new interest.
                NFD_LOG_DEBUG("CABEEEserviceDiscovery, ERROR??? Maybe not. We are trying to send out this interest through this face again: " << name2String << ", for face with faceID: " << hop_iterator->getFace().getId());
              }
              else
              {
                NFD_LOG_DEBUG("CABEEEserviceDiscovery, generating interest " << interest.getName().toUri() << ", for face with faceID: " << hop_iterator->getFace().getId());
                hop_iterator->getFace().sendInterest(interest);
                // mark this interest as generated.
                m_SDservTracker[name2String]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["intTx"] = 1;
              }
            }
          }
        }
        //else
          //NFD_LOG_DEBUG("CABEEEserviceDiscovery, fib_iterator does not have nextHops\n");
      }
    }
    NS_LOG_DEBUG("\n\nNFD forwarder service discovery - m_SDservTracker data structure: " << std::setw(2) << m_SDservTracker << '\n');
    return;
  }


  else // regular interest processing
  {
    // has NextHopFaceId?
    auto nextHopTag = interest.getTag<lp::NextHopFaceIdTag>();
    if (nextHopTag != nullptr) {
      // chosen NextHop face exists?
      Face* nextHopFace = m_faceTable.get(*nextHopTag);
      if (nextHopFace != nullptr) {
        NFD_LOG_DEBUG("onContentStoreMiss interest=" << interest.getName()
                      << " nexthop-faceid=" << nextHopFace->getId());
        // go to outgoing Interest pipeline
        // scope control is unnecessary, because privileged app explicitly wants to forward
        this->onOutgoingInterest(interest, *nextHopFace, pitEntry);
        if (simpleStringName == "/nescoSCOPT" && ingress.face.getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL)
          this->sendShortcutOPTinterests(interest, ingress, pitEntry);
      }
      return;
    }

    // dispatch to strategy: after receive Interest
    m_strategyChoice.findEffectiveStrategy(*pitEntry)
      .afterReceiveInterest(interest, FaceEndpoint(ingress.face, 0), pitEntry);
    if (simpleStringName == "/nescoSCOPT" && ingress.face.getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL)
      this->sendShortcutOPTinterests(interest, ingress, pitEntry);
  }

}

void
Forwarder::sendShortcutOPTinterests(const Interest& interest, const FaceEndpoint& ingress,
                              const shared_ptr<pit::Entry>& pitEntry)
{
  // generate interest (/nescoSCOPT/shortcutOPT) to all local application faces, containing DAG (application parameters).
  // forwarder applications will look for this name, and generate interests early if they are hosting any upstream services from the one in this interest
  shared_ptr<Interest> interestOPT = make_shared<Interest>();
  interestOPT->setName("/nescoSCOPT/shortcutOPT");
  if (interest.hasApplicationParameters())
  {
    interestOPT->setApplicationParameters(interest.getApplicationParameters());
  }
  
  char method = 2;
  if(method==1) // itereate through all faces of this router, send interest to all local faces
  {
    for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it) {
      Face* localFace = &*it;
      if (localFace->getScope() != ndn::nfd::FACE_SCOPE_NON_LOCAL) {
        NFD_LOG_DEBUG("cabeee CABEEEshortcutOPT, generating interest " << interestOPT << ", for local face " << localFace);
        localFace->sendInterest(*interestOPT);
      }
    }

  }
  if (method==2) // iterate through all fib entries, then through all faces(hops) for each entry, and if entry is for /nescoSCOPT AND it is a local face, then send interest.
  {
    //NFD_LOG_DEBUG("CABEEEshortcutOPT, sending /shortcutOPT interest to apps on local faces to generate new interests for inputs into locally hosted services.");

    //look at FIB, and see if any services are hosted on a local face. If so, send interestOPT out through that face.
    for (fib::Fib::const_iterator fib_iterator = m_fib.begin(); fib_iterator != m_fib.end(); ++fib_iterator)
    {
      //NFD_LOG_DEBUG("CABEEEshortcutOPT, looking at fib entry\n");
      ndn::Name entryName;
      entryName = fib_iterator->getPrefix();
      entryName = entryName.getSubName(0,1); // starting at component 0, get 1 component (/nescoSCOPT only)
      std::string entryString = entryName.toUri();
      //NFD_LOG_DEBUG("CABEEEshortcutOPT, fib entry name component 0 is "<< entryString);

      auto dagParameterFromInterest = interest.getApplicationParameters();
      std::string dagString = std::string(reinterpret_cast<const char*>(dagParameterFromInterest.value()), dagParameterFromInterest.value_size());
      json dagObject = json::parse(dagString);
      ndn::Name serviceName;
      serviceName = fib_iterator->getPrefix();
      serviceName = serviceName.getSubName(1,1); // starting at component 1, get 1 component (service name only)
      std::string serviceString = serviceName.toUri();
      //NFD_LOG_DEBUG("CABEEEshortcutOPT, fib entry name component 1 is "<< serviceString);
      //NFD_LOG_DEBUG("CABEEEshortcutOPT, interest head is "<< dagObject["head"]);

      // only generate shorcutOPT interest if the incoming interest is for /nescoSCOPT, and this fib entry is not for the service the interest is for (in which case the interest is forwarded to the service normally later on) 
      if (entryString == "/nescoSCOPT" && serviceString != dagObject["head"])
      {
        //NFD_LOG_DEBUG("CABEEEshortcutOPT, fib entry has nescoSCOPT name\n");
        if (fib_iterator->hasNextHops())
        {
          // figure out the faceID of all the nexthops in the list, and send interest to ones that are local
          //fib::NextHopList hopList = fib_iterator->getNextHops();
          const fib::NextHopList& hopList = fib_iterator->getNextHops();
          //for (auto &hop_iterator : hopList)
          for (nfd::fib::NextHopList::const_iterator hop_iterator = hopList.begin(); hop_iterator != hopList.end(); ++hop_iterator)
          //for (nfd::fib::NextHopList::const_iterator hop_iterator = fib_iterator->getNextHops().begin(); hop_iterator != fib_iterator->getNextHops().end(); ++hop_iterator)
          {
            //NFD_LOG_DEBUG("CABEEEshortcutOPT, looking at all hops for this fib entry\n");
            //Face thisFace = hop_iterator->getFace();
            //if (thisFace.getScope() != ndn::nfd::FACE_SCOPE_NON_LOCAL)
            //{
              //thisFace.sendInterest(interestOPT);
            //}
            if (hop_iterator->getFace().getScope() != ndn::nfd::FACE_SCOPE_NON_LOCAL)
            {
              //interestOPT->setName(fib_iterator->getPrefix()); // give it the hosted service name, instead of /nescoSCOPT/shortcutOPT
              ndn::Name scoptFullName;
              scoptFullName = "/nescoSCOPT/shortcutOPT" + fib_iterator->getPrefix().getSubName(1,1).toUri();
              interestOPT->setName(scoptFullName); // add the hosted service name to the full name: /nescoSCOPT/shortcutOPT/<serviceName>
              NFD_LOG_DEBUG("CABEEEshortcutOPT, generating interest " << interestOPT->getName().toUri() << ", for local face with faceID: " << hop_iterator->getFace().getId());
              hop_iterator->getFace().sendInterest(*interestOPT);
            }
          }
        }
      }
    }
  }
}

void
Forwarder::onContentStoreHit(const Interest& interest, const FaceEndpoint& ingress,
                             const shared_ptr<pit::Entry>& pitEntry, const Data& data)
{
  NFD_LOG_DEBUG("onContentStoreHit interest=" << interest.getName());
  ++m_counters.nCsHits;
  afterCsHit(interest, data);

  data.setTag(make_shared<lp::IncomingFaceIdTag>(face::FACEID_CONTENT_STORE));
  data.setTag(interest.getTag<lp::PitToken>());
  // FIXME Should we lookup PIT for other Interests that also match the data?

  pitEntry->isSatisfied = true;
  pitEntry->dataFreshnessPeriod = data.getFreshnessPeriod();

  // set PIT expiry timer to now
  this->setExpiryTimer(pitEntry, 0_ms);

  beforeSatisfyInterest(*pitEntry, *m_csFace, data);
  m_strategyChoice.findEffectiveStrategy(*pitEntry).beforeSatisfyInterest(data, FaceEndpoint(*m_csFace, 0), pitEntry);

  // dispatch to strategy: after Content Store hit
  m_strategyChoice.findEffectiveStrategy(*pitEntry).afterContentStoreHit(data, ingress, pitEntry);
}

pit::OutRecord*
Forwarder::onOutgoingInterest(const Interest& interest, Face& egress,
                              const shared_ptr<pit::Entry>& pitEntry)
{
  // drop if HopLimit == 0 but sending on non-local face
  if (interest.getHopLimit() == 0 && egress.getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL) {
    NFD_LOG_DEBUG("onOutgoingInterest out=" << egress.getId() << " interest=" << pitEntry->getName()
                  << " non-local hop-limit=0");
    ++egress.getCounters().nOutHopLimitZero;
    return nullptr;
  }

  NFD_LOG_DEBUG("onOutgoingInterest out=" << egress.getId() << " interest=" << pitEntry->getName());

  // insert out-record
  auto it = pitEntry->insertOrUpdateOutRecord(egress, interest);
  BOOST_ASSERT(it != pitEntry->out_end());

  // send Interest
  egress.sendInterest(interest);
  ++m_counters.nOutInterests;
  return &*it;
}

void
Forwarder::onInterestFinalize(const shared_ptr<pit::Entry>& pitEntry)
{
  NFD_LOG_DEBUG("onInterestFinalize interest=" << pitEntry->getName()
                << (pitEntry->isSatisfied ? " satisfied" : " unsatisfied"));

  if (!pitEntry->isSatisfied) {
    beforeExpirePendingInterest(*pitEntry);
  }

  // Dead Nonce List insert if necessary
  this->insertDeadNonceList(*pitEntry, nullptr);

  // Increment satisfied/unsatisfied Interests counter
  if (pitEntry->isSatisfied) {
    ++m_counters.nSatisfiedInterests;
  }
  else {
    ++m_counters.nUnsatisfiedInterests;
  }

  // PIT delete
  pitEntry->expiryTimer.cancel();
  m_pit.erase(pitEntry.get());
}



void
Forwarder::sendCsUpdateInterest(const Data& data)
{
  // generate interest (/PREFIX/csUpdate) to the local application face where the csUpdater app is running, containing cached data name (not data content) as application parameters.
  // csUpdate application will look for this name, and upon receiving will register the cached data name into RIB/FIB

  shared_ptr<Interest> interestCsUpdate = make_shared<Interest>();
  interestCsUpdate->setName("/nesco/csUpdate");

  std::string csNameString = data.getName().toUri();
  //std::cout << "csNameString: " << csNameString << std::endl;

  // in order to convert from std::string to a char[] datatype we do the following (https://stackoverflow.com/questions/7352099/stdstring-to-char):
  char *newCsNameString = new char[csNameString.length() + 1];
  strcpy(newCsNameString, csNameString.c_str());
  size_t length = strlen(newCsNameString);

  //std::shared_ptr<ndn::Buffer> csNameApplicationParameters;
  //std::istringstream is(csNameString);
  //csNameApplicationParameters = ndn::io::loadBuffer(is, ndn::io::NO_ENCODING);
  //interestCsUpdate->setApplicationParameters(csNameApplicationParameters);

  interestCsUpdate->setApplicationParameters((const uint8_t *)newCsNameString, length);


  char method = 1;
  if(method==1) // iterate through all faces of this router, send interest to all local faces
  {
    for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it) {
      Face* localFace = &*it;
      if (localFace->getScope() != ndn::nfd::FACE_SCOPE_NON_LOCAL) {
        NFD_LOG_DEBUG("cabeee csUpdate, generating interest " << interestCsUpdate->getName() << ", for local face " << localFace->getId() << std::endl);
        //NFD_LOG_INFO("cabeee csUpdate, generating interest " << interestCsUpdate << ", for local face " << localFace << std::endl);
        localFace->sendInterest(*interestCsUpdate);
      }
    }

  }
  /*
  if (method==2) // iterate through all fib entries, then through all faces(hops) for each entry, and if entry is for /nescoSCOPT AND it is a local face, then send interest.
  {
    //NFD_LOG_DEBUG("cabeee csUpdate, sending /shortcutOPT interest to apps on local faces to generate new interests for inputs into locally hosted services.");

    //look at FIB, and see if any services are hosted on a local face. If so, send interestCsUpdate out through that face.
    for (fib::Fib::const_iterator fib_iterator = m_fib.begin(); fib_iterator != m_fib.end(); ++fib_iterator)
    {
      //NFD_LOG_DEBUG("cabeee csUpdate, looking at fib entry\n");
      ndn::Name entryName;
      entryName = fib_iterator->getPrefix();
      entryName = entryName.getSubName(0,1); // starting at component 0, get 1 component (/nesco only)
      std::string entryString = entryName.toUri();
      //NFD_LOG_DEBUG("cabeee csUpdate, fib entry name component 0 is "<< entryString);

      auto dagParameterFromInterest = interest.getApplicationParameters();
      std::string dagString = std::string(reinterpret_cast<const char*>(dagParameterFromInterest.value()), dagParameterFromInterest.value_size());
      json dagObject = json::parse(dagString);
      ndn::Name serviceName;
      serviceName = fib_iterator->getPrefix();
      serviceName = serviceName.getSubName(1,1); // starting at component 1, get 1 component (service name only)
      std::string serviceString = serviceName.toUri();
      //NFD_LOG_DEBUG("cabeee csUpdate, fib entry name component 1 is "<< serviceString);
      //NFD_LOG_DEBUG("cabeee csUpdate, interest head is "<< dagObject["head"]);

      // only generate shorcutOPT interest if the incoming interest is for /nesco, and this fib entry is not for the service the interest is for (in which case the interest is forwarded to the service normally later on) 
      if (entryString == "/nesco" && serviceString != dagObject["head"])
      {
        //NFD_LOG_DEBUG("cabeee csUpdate, fib entry has nesco name, and entry service name is not dagObject head!\n");
        if (fib_iterator->hasNextHops())
        {
          // figure out the faceID of all the nexthops in the list, and send interest to ones that are local
          //fib::NextHopList hopList = fib_iterator->getNextHops();
          const fib::NextHopList& hopList = fib_iterator->getNextHops();
          //for (auto &hop_iterator : hopList)
          for (nfd::fib::NextHopList::const_iterator hop_iterator = hopList.begin(); hop_iterator != hopList.end(); ++hop_iterator)
          //for (nfd::fib::NextHopList::const_iterator hop_iterator = fib_iterator->getNextHops().begin(); hop_iterator != fib_iterator->getNextHops().end(); ++hop_iterator)
          {
            //NFD_LOG_DEBUG("cabeee csUpdate, looking at all hops for this fib entry\n");
            //Face thisFace = hop_iterator->getFace();
            //if (thisFace.getScope() != ndn::nfd::FACE_SCOPE_NON_LOCAL)
            //{
              //thisFace.sendInterest(interestCsUpdate);
            //}
            if (hop_iterator->getFace().getScope() != ndn::nfd::FACE_SCOPE_NON_LOCAL)
            {
              //interestCsUpdate->setName(fib_iterator->getPrefix()); // give it the hosted service name, instead of /nesco/csUpdate
              ndn::Name scoptFullName;
              scoptFullName = "/nesco/csUpdate" + fib_iterator->getPrefix().getSubName(1,1).toUri();
              interestCsUpdate->setName(scoptFullName); // add the hosted service name to the full name: /nesco/csUpdate/<serviceName>
              NFD_LOG_DEBUG("cabeee csUpdate, generating interest " << interestCsUpdate->getName().toUri() << ", for local face with faceID: " << hop_iterator->getFace().getId());
              hop_iterator->getFace().sendInterest(*interestCsUpdate);
            }
          }
        }
      }
    }
  }
  */
}


void
Forwarder::onIncomingData(const Data& data, const FaceEndpoint& ingress)
{
  // receive Data
  NFD_LOG_DEBUG("onIncomingData in=" << ingress << " data=" << data.getName());
  data.setTag(make_shared<lp::IncomingFaceIdTag>(ingress.face.getId()));
  ++m_counters.nInData;

  // /localhost scope control
  bool isViolatingLocalhost = ingress.face.getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL &&
                              scope_prefix::LOCALHOST.isPrefixOf(data.getName());
  if (isViolatingLocalhost) {
    NFD_LOG_DEBUG("onIncomingData in=" << ingress << " data=" << data.getName() << " violates /localhost");
    // drop
    return;
  }

  // PIT match
  pit::DataMatchResult pitMatches = m_pit.findAllDataMatches(data);
  if (pitMatches.size() == 0) {
    // goto Data unsolicited pipeline
    this->onDataUnsolicited(data, ingress);
    return;
  }


  // don't cache SD data packets
  if (data.getName().getPrefix(-1).getSubName(1,1).toUri() != "/serviceDiscovery") // remove last comopnent (app parameter), then starting at component 1, get 1 component (where /serviceDiscovery would be)
  {
    NFD_LOG_DEBUG("Attempting to insert into content store, data=" << data.getName() << "\n");
    // CS insert
    m_cs.insert(data);
/*
    if (data.getName().getPrefix(1).toUri() == "/nesco")
    {
      if (ingress.face.getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL) { // only if data is coming from non-local face. (if coming from local, it's from a service, and thus there is no need to advertise)
        this->sendCsUpdateInterest(data);
      }
    }
*/
  }
  else
  {
    NFD_LOG_DEBUG("Skipping content store. Service Discovery data packets should not be cached! data=" << data.getName() << "\n");
  }

/*
  // register prefix for this new CS content
  Ptr<GlobalRouter> gr = node->GetObject<GlobalRouter>();
  NS_ASSERT_MSG(gr != 0, "GlobalRouter is not installed on the node");
  auto name = make_shared<Name>(prefix);
  gr->AddLocalPrefix(name);

   //Implementation of route calculation is heavily based on Boost Graph Library
   //See http://www.boost.org/doc/libs/1_49_0/libs/graph/doc/table_of_contents.html for more details

  BOOST_CONCEPT_ASSERT((boost::VertexListGraphConcept<boost::NdnGlobalRouterGraph>));
  BOOST_CONCEPT_ASSERT((boost::IncidenceGraphConcept<boost::NdnGlobalRouterGraph>));

  boost::NdnGlobalRouterGraph graph;
  // typedef graph_traits < NdnGlobalRouterGraph >::vertex_descriptor vertex_descriptor;

  // For now we doing Dijkstra for every node.  Can be replaced with Bellman-Ford or Floyd-Warshall.
  // Other algorithms should be faster, but they need additional EdgeListGraph concept provided by
  // the graph, which
  // is not obviously how implement in an efficient manner
  for (NodeList::Iterator node = NodeList::Begin(); node != NodeList::End(); node++) {
    Ptr<GlobalRouter> source = (*node)->GetObject<GlobalRouter>();
    if (source == 0) {
      NS_LOG_DEBUG("Node " << (*node)->GetId() << " does not export GlobalRouter interface");
      continue;
    }

    boost::DistancesMap distances;

    dijkstra_shortest_paths(graph, source,
                            // predecessor_map (boost::ref(predecessors))
                            // .
                            distance_map(boost::ref(distances))
                              .distance_inf(boost::WeightInf)
                              .distance_zero(boost::WeightZero)
                              .distance_compare(boost::WeightCompare())
                              .distance_combine(boost::WeightCombine()));

    // NS_LOG_DEBUG (predecessors.size () << ", " << distances.size ());

    Ptr<L3Protocol> L3protocol = (*node)->GetObject<L3Protocol>();
    shared_ptr<nfd::Forwarder> forwarder = L3protocol->getForwarder();

    NS_LOG_DEBUG("Reachability from Node: " << source->GetObject<Node>()->GetId());
    for (const auto& dist : distances) {
      if (dist.first == source)
        continue;
      else {
        // cout << "  Node " << dist.first->GetObject<Node> ()->GetId ();
        if (std::get<0>(dist.second) == 0) {
          // cout << " is unreachable" << endl;
        }
        else {
          for (const auto& prefix : dist.first->GetLocalPrefixes()) {
            NS_LOG_DEBUG(" prefix " << prefix << " reachable via face " << *std::get<0>(dist.second)
                         << " with distance " << std::get<1>(dist.second) << " with delay "
                         << std::get<2>(dist.second));

            FibHelper::AddRoute(*node, *prefix, std::get<0>(dist.second),
                                std::get<1>(dist.second));
          }
        }
      }
    }
  }
*/







  std::set<std::pair<Face*, EndpointId>> satisfiedDownstreams;
  std::multimap<std::pair<Face*, EndpointId>, std::shared_ptr<pit::Entry>> unsatisfiedPitEntries;

  for (const auto& pitEntry : pitMatches) {
    NFD_LOG_DEBUG("onIncomingData matching=" << pitEntry->getName());

    // invoke PIT satisfy callback
    beforeSatisfyInterest(*pitEntry, ingress.face, data);

    std::set<std::pair<Face*, EndpointId>> unsatisfiedDownstreams;
    m_strategyChoice.findEffectiveStrategy(*pitEntry).satisfyInterest(pitEntry, ingress, data,
                                                                      satisfiedDownstreams, unsatisfiedDownstreams);
    for (const auto& endpoint : unsatisfiedDownstreams) {
      unsatisfiedPitEntries.emplace(endpoint, pitEntry);
    }

    if (unsatisfiedDownstreams.empty()) {
      // set PIT expiry timer to now
      this->setExpiryTimer(pitEntry, 0_ms);

      // mark PIT satisfied
      pitEntry->isSatisfied = true;
    }

    // Dead Nonce List insert if necessary (for out-record of inFace)
    this->insertDeadNonceList(*pitEntry, &ingress.face);

    pitEntry->dataFreshnessPeriod = data.getFreshnessPeriod();

    // clear PIT entry's in and out records
    for (const auto& endpoint : satisfiedDownstreams) {
      pitEntry->deleteInRecord(*endpoint.first);
    }
    pitEntry->deleteOutRecord(ingress.face);
  }

  // now check all unsatisfied entries against to be satisfied downstreams, in case there is
  // intersect, and those PIT entries will be actually satisfied regardless strategy's choice
  for (const auto& unsatisfied : unsatisfiedPitEntries) {
    auto downstreamIt = satisfiedDownstreams.find(unsatisfied.first);
    if (downstreamIt != satisfiedDownstreams.end()) {
      auto pitEntry = unsatisfied.second;
      pitEntry->deleteInRecord(*unsatisfied.first.first);

      if (pitEntry->getInRecords().empty()) { // if nothing left, "closing down" the entry
        // set PIT expiry timer to now
        this->setExpiryTimer(pitEntry, 0_ms);

        // mark PIT satisfied
        pitEntry->isSatisfied = true;
      }
    }
  }


/*
SDservTracker = {
  "service1 (or use full pDAG name?)": {
      "faceIN": faceID123,
      "faceOUT": {
          "faceID1": {
              "intTx": 0,
              "dataRx": 0,
              "linkDelay": -1,
              "EFT": -1
          }
          "faceID2": {
              "intTx": 0,
              "dataRx": 0,
              "linkDelay": -1,
              "EFT": -1
          }
      }
  }
}
*/



  // TODO: I think this is where we want to keep track of data packets returning.

  ndn::Name name1;
  name1 = (data.getName()).getPrefix(-1); // remove the last component of the name (the parameter digest) so we have just the raw name
  name1 = name1.getSubName(1,1); // remove the zeroeth component of the name (/nesco), starting at component 1, keep 1 component
  std::string name1String = name1.toUri();

  ndn::Name name2;
  name2 = (data.getName()).getPrefix(-1); // remove the last component of the name (the parameter digest) so we have just the raw name
  name2 = name2.getSubName(2,1); // remove the zeroeth and first component of the name (/nesco/serviceDiscovery), starting at component 2, keep 1 component
  std::string name2String = name2.toUri();

  if (name1 == "/serviceDiscovery") // remove last comopnent (app parameter), then starting at component 1, get 1 component (where /serviceDiscovery would be)
  {
    // Upon receiving an SD data packet through a particular face, 
    // mark it as received through this face.
    if (m_SDservTracker[name2String]["faceOUT"][std::to_string(ingress.face.getId())]["dataRx"] != 0)
      NFD_LOG_DEBUG("CABEEEserviceDiscovery, ERROR??? Should this happen? We received a data packet through this face again: " << name2String << ", for face with faceID: " << ingress.face.getId());
    m_SDservTracker[name2String]["faceOUT"][std::to_string(ingress.face.getId())]["dataRx"] = 1;


    // Then the node NFD will need to calculate the new EFT for that face.
      //It will use the data RX time to calculate the link delay upstream, and add that to the EFT for that face.


    //unsigned char myBuffer[1024];
    //pServiceInput = (uint8_t *)(data->getContent().data()); // this points to the first byte, which is the TLV-TYPE (21 for data packet content)

    //memcpy(myBuffer, (uint8_t*)(data->getContent().data()), 1024);

    NS_LOG_DEBUG("Data packet received: " << data);
    NS_LOG_DEBUG("Data packet contents received: " << data.getContent());
    NS_LOG_DEBUG("Data packet contents size: " << data.getContent().value_size());

    NS_LOG_DEBUG("Now reading it into string...");

    std::string dataPacketString;
    //uint8_t *pData = 0;
    //pData = (uint8_t *)data.getContent().data(); // assume data packet content is ONLY the json string, not 1024 bytes
    //NS_LOG_DEBUG("Calculating size...");
    //size_t length = data.getContent().value_size();
    //NS_LOG_DEBUG("Doing memcpy");
    //memcpy(dataPacketString, pData, length);
    dataPacketString = (const char *)data.getContent().value();

/*
    const ndn::Block& contentBlock = data.getContent();
    const uint8_t* rawData = contentBlock.value();
    size_t dataSize = contentBlock.value_size();

    // Now 'rawData' points to the content and 'dataSize' is its length
    unsigned char myBuffer[1024];
    for (size_t i = 0; i < dataSize; ++i) {
        myBuffer[i] = static_cast<char>(rawData[i]);
    }
 */

    NS_LOG_DEBUG("Data string received: " << dataPacketString);


    NS_LOG_DEBUG("Now parsing it into JSON...");

    json dataPacketContents = json::parse(dataPacketString);

    NS_LOG_DEBUG("Data received - EFT: " << dataPacketContents["EFT"] << ", txTime: " << dataPacketContents["txTime"]);


    //std::string dataTxTimeString = dataPacketContents["txTime"];  // if data comes as string
    //uint64_t dataTxTime = std::stoi(dataTxTimeString);            // if data comes as string
    uint64_t dataTxTime = dataPacketContents["txTime"];             // if data comes as number

    ns3::Time timeNow;
    timeNow = ns3::Simulator::Now();
    ns3::Time timeTx;
    timeTx = ns3::Time::FromInteger(dataTxTime, ns3::Time::MS);
    ns3::Time linkDelay;
    linkDelay = timeNow - timeTx;
    NS_LOG_DEBUG("Calculated link delay is: time.now " << timeNow.ToInteger(ns3::Time::MS) << " - timeTx " << timeTx.ToInteger(ns3::Time::MS) << " = " << linkDelay.ToInteger(ns3::Time::MS) << "ms");


    //std::string dataEFTString = dataPacketContents["EFT"];      // if data comes as string
    //uint64_t dataEFT = std::stoi(dataEFTString);                // if data comes as string
    uint64_t dataEFT = dataPacketContents["EFT"];                 // if data comes as number

    ns3::Time eft;
    eft = ns3::Time::FromInteger(dataEFT, ns3::Time::MS);
    ns3::Time newEFT;
    newEFT = eft + linkDelay;
    NS_LOG_DEBUG("Calculated EFT out of this face is: EFT " << eft.ToInteger(ns3::Time::MS) << " + upstreamLinkDelay " << linkDelay.ToInteger(ns3::Time::MS) << " = " << newEFT.ToInteger(ns3::Time::MS) << "ms");




    // Convert Time to integer in milliseconds and then to string
    int64_t linkDelayMS = linkDelay.ToInteger(ns3::Time::MS);
    std::string linkDelayStringMS = std::to_string(linkDelayMS);
    int64_t eftMS = newEFT.ToInteger(ns3::Time::MS);
    std::string eftStringMS = std::to_string(eftMS);

    //m_SDservTracker[name2String]["faceOUT"][std::to_string(ingress.face.getId())]["linkDelay"] = linkDelayStringMS;   // if stored as string
    //m_SDservTracker[name2String]["faceOUT"][std::to_string(ingress.face.getId())]["EFT"] = eftStringMS;               // if stored as string
    m_SDservTracker[name2String]["faceOUT"][std::to_string(ingress.face.getId())]["linkDelay"] = linkDelayMS;           // if stored as number
    m_SDservTracker[name2String]["faceOUT"][std::to_string(ingress.face.getId())]["EFT"] = eftMS;                       // if stored as number







    int allRxed = 1;
    uint64_t lowestEFT = -1;  // initialize to invalid EFT
    for (auto& faceIterator : m_SDservTracker[name2String]["faceOUT"].items())
    {
      if (m_SDservTracker[name2String]["faceOUT"][faceIterator.key()]["dataRx"] != 1)
      {
        allRxed = 0;
      }

      // figure out which is the lowest EFT of all the upstream faces that we've received packets for so far
      uint64_t thisEFT = m_SDservTracker[name2String]["faceOUT"][faceIterator.key()]["EFT"];
      if (lowestEFT == -1)
      {
        lowestEFT = thisEFT; // initialize to the first one
      }
      if (thisEFT != -1 && thisEFT < lowestEFT)
      {
        lowestEFT = thisEFT; // this becomes the lowest EFT found so far
      }
    }
    // Only when ALL interests have been satisfied (out of all the faces where we sent them out), will we generate the data packet(s) downstream with the overall lowest EFT.
    if (allRxed == 1)
    {
      NFD_LOG_DEBUG("CABEEEserviceDiscovery, all data packets for " << name2String << " have been received on all faces!!! Calculating best EFT and generating data packet downstream");
      // The new data packet going back downstream will contain: 
      // "Pruned DAG (pDAG) Service name" that it is being hosted and requested (serviceS/PWFH).
      // Calculate EFT (earliest finish time) and include it (lowest EFT of all the faces).
      // Timestamp of when data packet leaves (to measure delay to downstream nodes).

      // TODO: The node will then record the lowest EFT cost in the FIB by creating a new table entry using the pDAG name.
        // The cost will be EFT in micro-seconds? This EFT is units of time after the initial interest is generated.

       
      Face* downFace = m_faceTable.get(m_SDservTracker[name2String]["faceIN"]);
      NFD_LOG_DEBUG("CABEEEserviceDiscovery, data packet for " << name2String << " is being sent downstream through face " << m_SDservTracker[name2String]["faceIN"]);
      // IF above doesn't work, then try this:
/*
      Face* downFace;
      for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it) {
        downFace = &*it;
        if (downFace.getId() == m_SDservTracker[name2String]["faceIN"])
          break;
        }
      }
*/


      auto new_data = std::make_shared<ndn::Data>(data.getName());
      new_data->setFreshnessPeriod(data.getFreshnessPeriod());

      unsigned char myBuffer[1024];
      json dataPacketContents;
      ns3::Time timeNow;
      timeNow = ns3::Simulator::Now();
      // Convert to integer in milliseconds and then to string
      int64_t timeNowMS = timeNow.ToInteger(ns3::Time::MS);
      std::string timeStringMS = std::to_string(timeNowMS);
      //dataPacketContents["txTime"] = timeStringMS;                // if we send it as a string
      dataPacketContents["txTime"] = timeNowMS;                     // if we send it as a number
      //std::string lowestEFTStringMS = std::to_string(lowestEFT);  // if we send it as a string
      //dataPacketContents["EFT"] = lowestEFTStringMS;              // if we send it as a string
      dataPacketContents["EFT"] = lowestEFT;                        // if we send it as a number

      std::string dataPacketString = dataPacketContents.dump();

      // instead of just writing a single value to the buffer, now we write the JSON data structure containing EFT and tx timestamp
      // write to the buffer, after making sure it's big enough
      if (strlen(dataPacketString.c_str())+1 > 1024) // string length plus NULL terminating character
      {
        NS_LOG_DEBUG("NFD SD Forwarding ERROR!! The data packet size is larger than 1024!!!");
      }
      else
      {
        NS_LOG_DEBUG("The data packet size using strlen+1 is " << strlen(dataPacketString.c_str())+1);
        NS_LOG_DEBUG("The data packet size using length+1 operator is " << dataPacketString.length()+1);
      }
      memcpy(myBuffer, dataPacketString.c_str(), strlen(dataPacketString.c_str())+1);
      //new_data->setContent(myBuffer, 1024); // make the data always 1024 bytes long
      new_data->setContent(myBuffer, strlen(dataPacketString.c_str())+1); // make the data just big enough to fit the json object

      //ndn::StackHelper::getKeyChain().sign(*new_data); // TODO: do we need to sign the data packet? How? I've never done this in NFD, only in applications
      NS_LOG_DEBUG("Sending Data packet for " << new_data->getName());


      this->onOutgoingData(*new_data, *downFace);

    }

  }
  else // regular data packet processing
  {

    // foreach pending downstream
    for (const auto& downstream : satisfiedDownstreams) {
      if (downstream.first->getId() == ingress.face.getId() &&
          downstream.second == ingress.endpoint &&
          downstream.first->getLinkType() != ndn::nfd::LINK_TYPE_AD_HOC) {
        continue;
      }

      this->onOutgoingData(data, *downstream.first);
    }
  }
}

void
Forwarder::onDataUnsolicited(const Data& data, const FaceEndpoint& ingress)
{
  // accept to cache?
  auto decision = m_unsolicitedDataPolicy->decide(ingress.face, data);
  if (decision == fw::UnsolicitedDataDecision::CACHE) {
    // CS insert
    m_cs.insert(data, true);
  }

  NFD_LOG_DEBUG("onDataUnsolicited in=" << ingress << " data=" << data.getName()
                << " decision=" << decision);
  ++m_counters.nUnsolicitedData;
}

bool
Forwarder::onOutgoingData(const Data& data, Face& egress)
{
  // get first part of the name, if it equals /nesco or /nescoscopt or /orchA or /orchB and it's going to a local face (our application), then print INFO message
  // this effectively counts the number of data packets that are arriving at their consumer
  ndn::Name simpleName;
  simpleName = (data.getName()).getPrefix(1); // get just the first component of the name, and convert to Uri string
  std::string simpleStringName = simpleName.toUri();
  if (simpleStringName == "/nesco" || simpleStringName == "/nescoSCOPT" || simpleStringName == "/orchA" || simpleStringName == "/orchB")
  {
    if (egress.getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
    {
      NFD_LOG_INFO("     CABEEE: onOutgoingDataToApp (to the consuming application only) =" << " name=" << data.getName());
    }
    else
    {
      NFD_LOG_INFO("     CABEEE: onOutgoingDataToFace (to another NFD node on a physical face) =" << " name=" << data.getName());
    }
  }
  if (egress.getId() == face::INVALID_FACEID) {
    NFD_LOG_WARN("onOutgoingData out=(invalid) data=" << data.getName());
    return false;
  }
  NFD_LOG_DEBUG("onOutgoingData out=" << egress.getId() << " data=" << data.getName());

  // /localhost scope control
  bool isViolatingLocalhost = egress.getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL &&
                              scope_prefix::LOCALHOST.isPrefixOf(data.getName());
  if (isViolatingLocalhost) {
    NFD_LOG_DEBUG("onOutgoingData out=" << egress.getId() << " data=" << data.getName()
                  << " violates /localhost");
    // drop
    return false;
  }

  // TODO traffic manager

  // send Data
  egress.sendData(data);
  ++m_counters.nOutData;

  return true;
}

void
Forwarder::onIncomingNack(const lp::Nack& nack, const FaceEndpoint& ingress)
{
  // receive Nack
  nack.setTag(make_shared<lp::IncomingFaceIdTag>(ingress.face.getId()));
  ++m_counters.nInNacks;

  // if multi-access or ad hoc face, drop
  if (ingress.face.getLinkType() != ndn::nfd::LINK_TYPE_POINT_TO_POINT) {
    NFD_LOG_DEBUG("onIncomingNack in=" << ingress
                  << " nack=" << nack.getInterest().getName() << "~" << nack.getReason()
                  << " link-type=" << ingress.face.getLinkType());
    return;
  }

  // PIT match
  shared_ptr<pit::Entry> pitEntry = m_pit.find(nack.getInterest());
  // if no PIT entry found, drop
  if (pitEntry == nullptr) {
    NFD_LOG_DEBUG("onIncomingNack in=" << ingress << " nack=" << nack.getInterest().getName()
                  << "~" << nack.getReason() << " no-PIT-entry");
    return;
  }

  // has out-record?
  auto outRecord = pitEntry->getOutRecord(ingress.face);
  // if no out-record found, drop
  if (outRecord == pitEntry->out_end()) {
    NFD_LOG_DEBUG("onIncomingNack in=" << ingress << " nack=" << nack.getInterest().getName()
                  << "~" << nack.getReason() << " no-out-record");
    return;
  }

  // if out-record has different Nonce, drop
  if (nack.getInterest().getNonce() != outRecord->getLastNonce()) {
    NFD_LOG_DEBUG("onIncomingNack in=" << ingress << " nack=" << nack.getInterest().getName()
                  << "~" << nack.getReason() << " wrong-Nonce " << nack.getInterest().getNonce()
                  << "!=" << outRecord->getLastNonce());
    return;
  }

  NFD_LOG_DEBUG("onIncomingNack in=" << ingress << " nack=" << nack.getInterest().getName()
                << "~" << nack.getReason() << " OK");

  // record Nack on out-record
  outRecord->setIncomingNack(nack);

  // set PIT expiry timer to now when all out-record receive Nack
  if (!fw::hasPendingOutRecords(*pitEntry)) {
    this->setExpiryTimer(pitEntry, 0_ms);
  }

  // trigger strategy: after receive NACK
  m_strategyChoice.findEffectiveStrategy(*pitEntry).afterReceiveNack(nack, ingress, pitEntry);
}

bool
Forwarder::onOutgoingNack(const lp::NackHeader& nack, Face& egress,
                          const shared_ptr<pit::Entry>& pitEntry)
{
  if (egress.getId() == face::INVALID_FACEID) {
    NFD_LOG_WARN("onOutgoingNack out=(invalid)"
                 << " nack=" << pitEntry->getInterest().getName() << "~" << nack.getReason());
    return false;
  }

  // has in-record?
  auto inRecord = pitEntry->getInRecord(egress);

  // if no in-record found, drop
  if (inRecord == pitEntry->in_end()) {
    NFD_LOG_DEBUG("onOutgoingNack out=" << egress.getId()
                  << " nack=" << pitEntry->getInterest().getName()
                  << "~" << nack.getReason() << " no-in-record");
    return false;
  }

  // if multi-access or ad hoc face, drop
  if (egress.getLinkType() != ndn::nfd::LINK_TYPE_POINT_TO_POINT) {
    NFD_LOG_DEBUG("onOutgoingNack out=" << egress.getId()
                  << " nack=" << pitEntry->getInterest().getName() << "~" << nack.getReason()
                  << " link-type=" << egress.getLinkType());
    return false;
  }

  NFD_LOG_DEBUG("onOutgoingNack out=" << egress.getId()
                << " nack=" << pitEntry->getInterest().getName()
                << "~" << nack.getReason() << " OK");

  // create Nack packet with the Interest from in-record
  lp::Nack nackPkt(inRecord->getInterest());
  nackPkt.setHeader(nack);

  // erase in-record
  pitEntry->deleteInRecord(egress);

  // send Nack on face
  egress.sendNack(nackPkt);
  ++m_counters.nOutNacks;

  return true;
}

void
Forwarder::onDroppedInterest(const Interest& interest, Face& egress)
{
  m_strategyChoice.findEffectiveStrategy(interest.getName()).onDroppedInterest(interest, egress);
}

void
Forwarder::onNewNextHop(const Name& prefix, const fib::NextHop& nextHop)
{
  const auto affectedEntries = this->getNameTree().partialEnumerate(prefix,
    [&] (const name_tree::Entry& nte) -> std::pair<bool, bool> {
      // we ignore an NTE and skip visiting its descendants if that NTE has an
      // associated FIB entry (1st condition), since in that case the new nexthop
      // won't affect any PIT entries anywhere in that subtree, *unless* this is
      // the initial NTE from which the enumeration started (2nd condition), which
      // must always be considered
      if (nte.getFibEntry() != nullptr && nte.getName().size() > prefix.size()) {
        return {false, false};
      }
      return {nte.hasPitEntries(), true};
    });

  for (const auto& nte : affectedEntries) {
    for (const auto& pitEntry : nte.getPitEntries()) {
      m_strategyChoice.findEffectiveStrategy(*pitEntry).afterNewNextHop(nextHop, pitEntry);
    }
  }
}

void
Forwarder::setExpiryTimer(const shared_ptr<pit::Entry>& pitEntry, time::milliseconds duration)
{
  BOOST_ASSERT(pitEntry);
  duration = std::max(duration, 0_ms);

  pitEntry->expiryTimer.cancel();
  pitEntry->expiryTimer = getScheduler().schedule(duration, [=] { onInterestFinalize(pitEntry); });
}

void
Forwarder::insertDeadNonceList(pit::Entry& pitEntry, const Face* upstream)
{
  // need Dead Nonce List insert?
  bool needDnl = true;
  if (pitEntry.isSatisfied) {
    BOOST_ASSERT(pitEntry.dataFreshnessPeriod >= 0_ms);
    needDnl = pitEntry.getInterest().getMustBeFresh() &&
              pitEntry.dataFreshnessPeriod < m_deadNonceList.getLifetime();
  }

  if (!needDnl) {
    return;
  }

  // Dead Nonce List insert
  if (upstream == nullptr) {
    // insert all outgoing Nonces
    const auto& outRecords = pitEntry.getOutRecords();
    std::for_each(outRecords.begin(), outRecords.end(), [&] (const auto& outRecord) {
      m_deadNonceList.add(pitEntry.getName(), outRecord.getLastNonce());
    });
  }
  else {
    // insert outgoing Nonce of a specific face
    auto outRecord = pitEntry.getOutRecord(*upstream);
    if (outRecord != pitEntry.getOutRecords().end()) {
      m_deadNonceList.add(pitEntry.getName(), outRecord->getLastNonce());
    }
  }
}

void
Forwarder::setConfigFile(ConfigFile& configFile)
{
  configFile.addSectionHandler(CFG_FORWARDER, [this] (auto&&... args) {
    processConfig(std::forward<decltype(args)>(args)...);
  });
}

void
Forwarder::processConfig(const ConfigSection& configSection, bool isDryRun, const std::string&)
{
  Config config;

  for (const auto& pair : configSection) {
    const std::string& key = pair.first;
    if (key == "default_hop_limit") {
      config.defaultHopLimit = ConfigFile::parseNumber<uint8_t>(pair, CFG_FORWARDER);
    }
    else {
      NDN_THROW(ConfigFile::Error("Unrecognized option " + CFG_FORWARDER + "." + key));
    }
  }

  if (!isDryRun) {
    m_config = config;
  }
}

} // namespace nfd
