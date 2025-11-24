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
#include "ns3/node-list.h"
#include "ns3/node.h"

#include "ndn-cxx/mgmt/nfd/fib-entry.hpp"

namespace nfd {

NFD_LOG_INIT(Forwarder);

const std::string CFG_FORWARDER = "forwarder";

static Name
getDefaultStrategyName()
{
  return fw::BestRouteStrategy::getStrategyName();
}

Forwarder::Forwarder(FaceTable& faceTable)
//Forwarder::Forwarder(FaceTable& faceTable, ndn::KeyChain& keyChain)
  : m_faceTable(faceTable)
  //, m_keyChain(keyChain)
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



/*
This is the structure of the JSON object that we use to track interest and data packets for Service Discovery

SDservTracker = {
  "/service1/faceInIdString1&pDAG_param_hash)": {     // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
      "faceIN": {
          "inID": faceID1,                            // faceID where an interest for this service/pDAG has been received.
          "inName": "/service1/pDAG_param_hash",      // Notice we store the original name as received so we can respond with the same name when data arrives.
          "dag": { <pDag as received> },              // We add the pDAG here so that we can generate the name we use for the FIB entry that we create at the end. (serviceDiscovery interest application parameter has more info than regular workflow interest).
          "head": { <service head as received> },     // We add the service head so that we can generate the name we use for the FIB entry that we create at the end. (serviceDiscovery interest application parameter has more info than regular workflow interest).
          "serviceScheduling": {                      // If the service has been scheduled to run in this node, we will see this entry. Otherwise, it won't exist
            "start": <absolute start time>,
            "end": <absolute end time>
          }
      },
      "faceOUT": {
          "faceID3": {                // faceID where the interest has been forwarded to
              "intTx": 1,             // interest has already been generated on this upstream face
              "dataRx": 1,            // data packet has already been received from this upstream face
              "linkDelay": 2,         // 2ms link delay to the next node upstream
              "EFT": 3                // 3ms is the EFT upstream
          },
          "faceID4": {                // faceID where the interest has been forwarded to
              "intTx": 0,             // interest has not been generated
              "dataRx": 0,            // data packet has not been received from this upstream face
              "linkDelay": -1,        // default value is -1 (data packet not received yet)
              "EFT": -1               // default value is -1 (data packet not received yet)
          }
      }
  },
  "/service1/faceInIdString2&pDAG_param_hash)": {     // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
      "faceIN": {
          "inID": faceID1,                            // faceID where an interest for this service/pDAG has been received.
          "inName": "/service1/pDAG_param_hash",      // Notice we store the original name as received so we can respond with the same name when data arrives.
          "dag": { <pDag as received> },              // We add the pDAG here so that we can generate the name we use for the FIB entry that we create at the end. (serviceDiscovery interest application parameter has more info than regular workflow interest).
          "head": { <service head as received> },     // We add the service head so that we can generate the name we use for the FIB entry that we create at the end. (serviceDiscovery interest application parameter has more info than regular workflow interest).
          "serviceScheduling": {                      // If the service has been scheduled to run in this node, we will see this entry. Otherwise, it won't exist
            "start": <absolute start time>,
            "end": <absolute end time>
      },
      "faceOUT": {
          "faceID3": {                // faceID where the interest has been forwarded to
              "intTx": 1,             // interest has already been generated on this upstream face
              "dataRx": 1,            // data packet has already been received from this upstream face
              "linkDelay": 2,         // 2ms link delay to the next node upstream
              "EFT": 3                // 3ms is the EFT upstream
          },
          "faceID4": {                // faceID where the interest has been forwarded to
              "intTx": 0,             // interest has not been generated
              "dataRx": 0,            // data packet has not been received from this upstream face
              "linkDelay": -1,        // default value is -1 (data packet not received yet)
              "EFT": -1               // default value is -1 (data packet not received yet)
          }
      }
  },
  "service2/faceInIdString3&pDAG_param_hash": {
     etc
  }
}



*/



void
Forwarder::onIncomingInterest(const Interest& interest, const FaceEndpoint& ingress)
{
  ndn::Name simpleName;
  simpleName = (interest.getName()).getPrefix(1); // get just the first component of the name, and convert to Uri string
  std::string simpleStringName = simpleName.toUri();


  // get first part of the name, if it equals /nesco or /nescoscopt or /orchA or /orchB and it's coming from a local face (our application), then print INFO message
  // this effectively counts the number of interest packets that are generated at the consumer (including the custom forwarders)
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
      NFD_LOG_DEBUG("onIncomingInterest in=" << ingress << " interest=" << interest.getName() << " hop-limit=0");
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
    NFD_LOG_DEBUG("onIncomingInterest in=" << ingress << " interest=" << interest.getName() << " violates /localhost");
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
    NFD_LOG_DEBUG("onIncomingInterest in=" << ingress << " interest=" << interest.getName() << " reaching-producer-region");
    const_cast<Interest&>(interest).setForwardingHint({});
  }





  // if we are receiving a schedulerRelease message from a node downstream, then remove the scheduled service from this node if it exists. If it does not exist, forward the release request further upstream.
  if (interest.getName().getSubName(1,1).toUri() == "/schedulerRelease") // starting at component 1, get 1 component (where /schedulerRelease would be)
  {
    // name&hash to remove from CPU scheduling will come as an application parameter, not as part of the actual schedulerRelease name
    auto appParameterFromInterest = interest.getApplicationParameters();
    std::string nameAndHashToRemove = std::string(reinterpret_cast<const char*>(appParameterFromInterest.value()), appParameterFromInterest.value_size());
    NFD_LOG_DEBUG("NFDServiceDiscovery - received schedulerRelease message for " << nameAndHashToRemove << std::endl);
    for (auto& serviceIterator : m_SDservTracker.items())
    {
      //NFD_LOG_DEBUG("NFDServiceDiscovery - schedulerRelease evaluating service " << serviceIterator.key() << " with inName " << m_SDservTracker[serviceIterator.key()]["faceIN"]["inName"] << std::endl);
      if (m_SDservTracker[serviceIterator.key()]["faceIN"]["inName"] == nameAndHashToRemove)
      {
        if (m_SDservTracker[serviceIterator.key()]["faceIN"].contains("serviceScheduling"))
        {
          NFD_LOG_DEBUG("NFDServiceDiscovery - schedulerRelease removing scheduled service " << nameAndHashToRemove << std::endl);
          m_SDservTracker[serviceIterator.key()]["faceIN"].erase("serviceScheduling");
        }
        else
        {
          //NFD_LOG_DEBUG("NFDServiceDiscovery - schedulerRelease will send request further upststream using name " << serviceIterator.key() << std::endl);
          // send the request further upstream
          // TODO: package the following code into a function - sendInterestUpstreamToUnSchedule(uniqueHistoricalName&pDAG);
          shared_ptr<Interest> interestSchedulerRelease = make_shared<Interest>();
          interestSchedulerRelease->setName("/nesco/schedulerRelease");
          std::string appParamString = "/nesco/serviceDiscovery" + serviceIterator.key();
          //std::cout << "appParamString: " << appParamString << std::endl;
          // in order to convert from std::string to a char[] datatype we do the following (https://stackoverflow.com/questions/7352099/stdstring-to-char):
          char *newAppParamString = new char[appParamString.length() + 1];
          strcpy(newAppParamString, appParamString.c_str());
          size_t length = strlen(newAppParamString);
          interestSchedulerRelease->setApplicationParameters((const uint8_t *)newAppParamString, length);

          for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it)
          {
            Face* thisFace = &*it;
            for (auto& faceIterator : m_SDservTracker[serviceIterator.key()]["faceOUT"].items())
            {
              if (std::to_string(thisFace->getId()) == faceIterator.key())
              {
                NFD_LOG_DEBUG("NFDServiceDiscovery - sending schedulerRelease message for " << serviceIterator.key() << " upstream through face " << thisFace->getId() << std::endl);
                thisFace->sendInterest(*interestSchedulerRelease);
              }
            }
          }
        }
      }
    }
    return;
  }



  // service discovery interest processing (no PIT entry created)
  if (interest.getName().getPrefix(-1).getSubName(1,1).toUri() == "/serviceDiscovery") // remove last comopnent (app parameter), then starting at component 1, get 1 component (where /serviceDiscovery would be)
  {
    // Determine which interfaces can be used to reach the named service, and generate a new interest for the name service out of each of the faces (even local ones for teh APP).
    // keep track of which ones have left, so that when data packets arrive, I can evaluate their EFTs. Once all have returned, generate data packet with lowest EFT.
    ndn::Name simpleName;
    ndn::Name simpleNameAndHash;
    simpleName        = interest.getName().getPrefix(-1); // remove the last component of the name (the parameter digest) so we have just the raw name
    simpleNameAndHash = interest.getName();
    simpleName        = simpleName.getSubName(2,1); // remove the zeroeth component of the name (/nesco), and the first component of the name (/serviceDiscovery). starting at component 2, keep 1 component (just the service name)
    simpleNameAndHash = simpleNameAndHash.getSubName(2,simpleNameAndHash.size()); // remove the zeroeth component of the name (/nesco), and the first component of the name (/serviceDiscovery). starting at component 2, keep the rest of the components, including the application parameter hash
    std::string rxedInterestName        = simpleName.toUri();
    std::string rxedInterestNameAndHash = simpleNameAndHash.toUri();
    //NFD_LOG_DEBUG("NFDServiceDiscovery rxedInterestName -> simpleName: " << rxedInterestName << '\n');
    //NFD_LOG_DEBUG("NFDServiceDiscovery received an interest on face " << ingress.face.getId() << " that needs to be distributed to other faces.\n");
    NFD_LOG_DEBUG("NFDServiceDiscovery received interest has name: " << rxedInterestNameAndHash << " - faceID is: " << ingress.face.getId());

    auto faceInId = ingress.face.getId();
    std::string faceInIdString = std::to_string(faceInId);

    // Modify application parameters to add faceIN (downstream face), so we know which interest is being satisfied later when the data packet arrives (this changes the name hash! so it's unique)
    auto dagParameterFromInterest = interest.getApplicationParameters();
    std::string dagString = std::string(reinterpret_cast<const char*>(dagParameterFromInterest.value()), dagParameterFromInterest.value_size());
    json dagObject = json::parse(dagString);
    dagObject["faceIN"] = faceInIdString;
    dagObject["prevHash"] = rxedInterestNameAndHash; // adding the previous name&hash add the full "historical" path of where the interest has come from, trying to make it unique, although faces may have same ID on different nodes.
   
/* 
    ns3::Time timeNow;
    timeNow = ns3::Simulator::Now();
    // Convert to integer in milliseconds and then to string
    int64_t timeNowNS = timeNow.ToInteger(ns3::Time::NS); // extract the time in nano-seconds so that we have enough granularity to guarantee interest uniqueness.
    dagObject["rxTime"] = timeNowNS; // adding the time it was received, to make it truly unique.
*/

    auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
    //NFD_LOG_DEBUG("NodeID is " << (*node).GetId());
    dagObject["nodeID"] = (*node).GetId(); // adding the nodeID to the hash, making this interest unique based on nodeID and pDAG only

    std::string updatedDagString = dagObject.dump();
    // in order to convert from std::string to a char[] datatype we do the following (https://stackoverflow.com/questions/7352099/stdstring-to-char):
    char *dagStringParameter = new char[updatedDagString.length() + 1];
    strcpy(dagStringParameter, updatedDagString.c_str());
    size_t length = strlen(dagStringParameter);
    //add modified object as a parameter to the new interest
    shared_ptr<Interest> new_interest = make_shared<Interest>();
    new_interest->setName(interest.getName());
    new_interest->setApplicationParameters((const uint8_t *)dagStringParameter, length);


    //std::string justHash = interest.getName().getSubName(3,interest.getName().size()).toUri();
    //std::string jsonName = rxedInterestName + "/" + faceInIdString + justHash;
    std::string jsonName = new_interest->getName().getSubName(2,new_interest->getName().size()).toUri();
    //NFD_LOG_DEBUG("NFDServiceDiscovery created jsonName: " << jsonName);






    // if faceIN ID in this name entry doesn't exist, create it so that we know to send data packets out through it later
    //if (!m_SDservTracker[jsonName]["faceIN"].contains(faceInIdString))
    if (!m_SDservTracker.contains(jsonName))
    {
      //NFD_LOG_DEBUG("NFDServiceDiscovery adding this name to faceIN list: " << jsonName << " - faceID is: " << faceInIdString);
      m_SDservTracker[jsonName]["faceIN"]["inID"] = faceInId;                     // we record the faceID
      m_SDservTracker[jsonName]["faceIN"]["inName"] = interest.getName().toUri(); // we record the original name as it came in, so we know what name to use when we respond with data downstream.
      m_SDservTracker[jsonName]["faceIN"]["dag"] = dagObject["dag"];              // we capture the pDag here so that we can use it to generate the name we use for the FIB entry  we create later.
      m_SDservTracker[jsonName]["faceIN"]["head"] = dagObject["head"];            // we capture the "head" here so that we can use it to generate the name&hash we use for the FIB entry  we create later.
    }
    //if (!m_SDservTracker[jsonName].contains("faceOUT"))
    //{
    //}



    //look at FIB, and see if this service is reachable out of any other faces. If so, send interest out through each face.
    for (fib::Fib::const_iterator fib_iterator = m_fib.begin(); fib_iterator != m_fib.end(); ++fib_iterator)
    {

      ndn::Name name1;
      name1 = fib_iterator->getPrefix();
      name1 = name1.getSubName(1,1); // starting at component 1, get 1 component (/serviceDiscovery only)
      std::string name1String = name1.toUri();
      //NFD_LOG_DEBUG("NFDServiceDiscovery, fib name1String component 1 is " << name1String);

      // FIB entries do not have the hash. When creating the data structure, I need to use the full interest name. THEN the for loop iterator can use the simple name for matching with FIB entries
      ndn::Name name2;
      name2 = fib_iterator->getPrefix();
      name2 = name2.getSubName(2,1); // starting at component 2, get 1 component (name2 name only)
      std::string name2String = name2.toUri();
      //NFD_LOG_DEBUG("NFDServiceDiscovery, fib name2String component 2 is "<< name2String);


      //NFD_LOG_DEBUG("NFDServiceDiscovery, interest head is "<< dagObject["head"]);
      // only generate new serviceDiscovery interest if the incoming interest is for /serviceDiscovery, and this fib entry is for the service the interest is for
      if (name1String == "/serviceDiscovery" && name2String == dagObject["head"])
      {

        //NFD_LOG_DEBUG("NFDServiceDiscovery, fib entry has matching /serviceDiscovery/serviceX name\n");
        if (fib_iterator->hasNextHops())
        {

          //NFD_LOG_DEBUG("NFDServiceDiscovery, fib_iterator has nextHops, iterating to all faces...\n");
          const fib::NextHopList& hopList = fib_iterator->getNextHops();
          for (nfd::fib::NextHopList::const_iterator hop_iterator = hopList.begin(); hop_iterator != hopList.end(); ++hop_iterator)
          {
            //NFD_LOG_DEBUG("NFDServiceDiscovery, looking at all hops for this fib entry\n");
            if (hop_iterator->getFace().getId() != ingress.face.getId()) // do not send new interest out of the incoming face (avoid loops).
            {

              // if faceOUT ID in this name entry doesn't exist, create it (mark interest generated as False and data received as False, delay as -1, EFT as -1).
              if (!m_SDservTracker[jsonName]["faceOUT"].contains(std::to_string(hop_iterator->getFace().getId())))
              {
                m_SDservTracker[jsonName]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["intTx"] = 0;
                m_SDservTracker[jsonName]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["dataRx"] = 0;
                m_SDservTracker[jsonName]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["linkDelay"] = -1;
                m_SDservTracker[jsonName]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["EFT"] = -1;
              }
              // if we have not yet generated this interest out of this face, then generate it and mark it as generated
              if (m_SDservTracker[jsonName]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["intTx"] == 0)
              {
                // if interest is already marked as generated, skip sending a new one out. (we already added the faceIN id to the data structure above)
                // This happens for example when N1/S3 requests S1, and we already had received interests for S1 from N2/S3.
                // Just let it add the entry and drop the new interest.

                //NFD_LOG_DEBUG("NFDServiceDiscovery, generating interest " << interest.getName().toUri() << ", for face with faceID: " << hop_iterator->getFace().getId());
                //hop_iterator->getFace().sendInterest(interest);
                NFD_LOG_DEBUG("NFDServiceDiscovery, generating interest " << new_interest->getName().toUri() << ", for face with faceID: " << hop_iterator->getFace().getId());
                hop_iterator->getFace().sendInterest(*new_interest);

                // mark this interest as generated.
                m_SDservTracker[jsonName]["faceOUT"][std::to_string(hop_iterator->getFace().getId())]["intTx"] = 1;
              }
              else
              {
                NFD_LOG_DEBUG("NFDServiceDiscovery, We are trying to send out this interest through this face again (but won't): " << jsonName << ", for face with faceID: " << hop_iterator->getFace().getId());
              }
            }
          }
        }
        //else
          //NFD_LOG_DEBUG("NFDServiceDiscovery, fib_iterator does not have nextHops\n");
      }

    } // FIB iteration for loop

    NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Interest): " << std::setw(2) << m_SDservTracker << '\n');
    return;
  }

  else // regular interest processing
  {


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
}

void
Forwarder::onInterestLoop(const Interest& interest, const FaceEndpoint& ingress)
{
  // if multi-access or ad hoc face, drop
  if (ingress.face.getLinkType() != ndn::nfd::LINK_TYPE_POINT_TO_POINT) {
    NFD_LOG_DEBUG("onInterestLoop in=" << ingress << " interest=" << interest.getName() << " drop");
    return;
  }

  NFD_LOG_DEBUG("onInterestLoop in=" << ingress << " interest=" << interest.getName() << " send-Nack-duplicate");

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


  ndn::Name simpleName;
  simpleName = (interest.getName()).getPrefix(1); // get just the first component of the name, and convert to Uri string
  std::string simpleStringName = simpleName.toUri();
  


  // PRINT OUT THE FIB ENTRIES FOR THIS NAME - for debugging
  if (simpleStringName == "/nesco")
  {
    for (fib::Fib::const_iterator fib_iterator = m_fib.begin(); fib_iterator != m_fib.end(); ++fib_iterator)
    {
      //NFD_LOG_DEBUG("CABEEEshortcutOPT, looking at fib entry\n");
      ndn::Name entryName;
      entryName = fib_iterator->getPrefix();
      entryName = entryName.getSubName(0,1); // starting at component 0, get 1 component (/nescoSCOPT only)
      std::string entryString = entryName.toUri();

      auto dagParameterFromInterest = interest.getApplicationParameters();
      std::string dagString = std::string(reinterpret_cast<const char*>(dagParameterFromInterest.value()), dagParameterFromInterest.value_size());
      json dagObject = json::parse(dagString);
      ndn::Name serviceName;
      serviceName = fib_iterator->getPrefix();
      serviceName = serviceName.getSubName(1,1); // starting at component 1, get 1 component (service name only)
      std::string serviceString = serviceName.toUri();

      // only print FIB entries if this interest is for this fib iterator
      if (entryString == "/nesco" && serviceString == dagObject["head"])
      {
        if (fib_iterator->hasNextHops())
        {
          // figure out the faceID of all the nexthops in the list, and send interest to ones that are local
          const fib::NextHopList& hopList = fib_iterator->getNextHops();
          for (nfd::fib::NextHopList::const_iterator hop_iterator = hopList.begin(); hop_iterator != hopList.end(); ++hop_iterator)
          {
            NFD_LOG_DEBUG("CABEEEfibEntries: interest " << fib_iterator->getPrefix().toUri() << ", faceID: " << hop_iterator->getFace().getId() << ", cost: " << hop_iterator->getCost());
          }
        }
      }
    }
  }






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
  NFD_LOG_DEBUG("onIncomingData in=" << ingress << " data=" << data.getName());


  ndn::Name name1;
  name1 = (data.getName()).getPrefix(-1); // remove the last component of the name (the parameter digest) so we have just the raw name
  name1 = name1.getSubName(1,1); // remove the zeroeth component of the name (/nesco), starting at component 1, keep 1 component
  std::string name1String = name1.toUri();

  //ndn::Name name2;
  //name2 = (data.getName()).getPrefix(-1); // remove the last component of the name (the parameter digest) so we have just the raw name
  //name2 = name2.getSubName(2,1); // remove the zeroeth and first component of the name (/nesco/serviceDiscovery), starting at component 2, keep 1 component
  //name2 = name2.getSubName(2,name2.size()); // remove the zeroeth and first component of the name (/nesco/serviceDiscovery), starting at component 2, keep the rest of the components, including the application parameter hash
  //std::string name2String = name2.toUri();

  // this is where we keep track of service discovery data packets returning (onData).
  if (name1String == "/serviceDiscovery")
  {
    //auto dagParameterFromData = data.getApplicationParameters();
    //std::string dagString = std::string(reinterpret_cast<const char*>(dagParameterFromData.value()), dagParameterFromData.value_size());
    //json dagObject = json::parse(dagString);

    ndn::Name simpleName;
    ndn::Name simpleNameAndHash;
    simpleName        = (data.getName()).getPrefix(-1); // remove the last component of the name (the parameter digest) so we have just the raw name
    simpleNameAndHash = data.getName();
    simpleName        = simpleName.getSubName(2,1); // remove the zeroeth component of the name (/nesco), and the first component of the name (/serviceDiscovery). starting at component 2, keep 1 component
    simpleNameAndHash = simpleNameAndHash.getSubName(2,simpleNameAndHash.size()); // remove the zeroeth component of the name (/nesco), and the first component of the name (/serviceDiscovery). starting at component 2, keep the rest of the components, including the application parameter hash
    std::string rxedDataName        = simpleName.toUri();
    std::string rxedDataNameAndHash = simpleNameAndHash.toUri();


    NFD_LOG_DEBUG("NFDServiceDiscovery received data with name: " << rxedDataNameAndHash << " - faceID is: " << ingress.face.getId());
    // Upon receiving an SD data packet through a particular face, 
    // mark it as received through this face.
    if (m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["dataRx"] != 0)
      NFD_LOG_DEBUG("NFDServiceDiscovery, ERROR??? Should this happen? We received a data packet through this face again: " << rxedDataNameAndHash << ", for face with faceID: " << ingress.face.getId());
    m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["dataRx"] = 1;


    // Then the node NFD will need to calculate the new EFT for that face.
      //It will use the data RX time to calculate the link delay upstream, and add that to the EFT for that face.


    //NFD_LOG_DEBUG("Now reading it into string...");

    std::string dataPacketString;
    dataPacketString = (const char *)data.getContent().value();
    //NFD_LOG_DEBUG("Data string received: " << dataPacketString);


    //NFD_LOG_DEBUG("Now parsing it into JSON...");

    json dataPacketContents = json::parse(dataPacketString);
    NFD_LOG_DEBUG("Data received: " << dataPacketContents);

    int64_t serviceLatency = -1;
    if (ingress.face.getScope() == ndn::nfd::FACE_SCOPE_LOCAL) // if data is coming from local face, it will have the serviceLatency reported by the serviceDiscovery application.
    {
      NFD_LOG_DEBUG("Data received - EFT: " << dataPacketContents["EFT"] << ", txTime: " << dataPacketContents["txTime"] << ", serviceLatency: " << dataPacketContents["serviceLatency"]);
      serviceLatency = dataPacketContents["serviceLatency"];
      m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["serviceLatency"] = serviceLatency;
    }
    else
    {
      NFD_LOG_DEBUG("Data received - EFT: " << dataPacketContents["EFT"] << ", txTime: " << dataPacketContents["txTime"]);
    }

    int64_t dataTxTime = dataPacketContents["txTime"];

    ns3::Time timeNow;
    timeNow = ns3::Simulator::Now();
    ns3::Time timeTx;
    timeTx = ns3::Time::FromInteger(dataTxTime, ns3::Time::NS);
    ns3::Time linkDelay;
    linkDelay = timeNow - timeTx;
    NFD_LOG_DEBUG("Calculated link delay is: time.now " << timeNow.ToInteger(ns3::Time::NS) << " - timeTx " << timeTx.ToInteger(ns3::Time::NS) << " = " << linkDelay.ToInteger(ns3::Time::NS) << "ns");


    int64_t dataEFT = dataPacketContents["EFT"];

    ns3::Time eft;
    eft = ns3::Time::FromInteger(dataEFT, ns3::Time::NS);
    ns3::Time newEFT;
    newEFT = eft + linkDelay;
    NFD_LOG_DEBUG("Calculated EFT out of this face is: EFT " << eft.ToInteger(ns3::Time::NS) << " + upstreamLinkDelay " << linkDelay.ToInteger(ns3::Time::NS) << " = " << newEFT.ToInteger(ns3::Time::NS) << "ns");

    // Convert Time to integer in milliseconds and then to string
    int64_t linkDelayNS = linkDelay.ToInteger(ns3::Time::NS);
    std::string linkDelayStringNS = std::to_string(linkDelayNS);
    int64_t eftNS = newEFT.ToInteger(ns3::Time::NS);
    std::string eftStringNS = std::to_string(eftNS);

    m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["linkDelay"] = linkDelayNS;
    m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["EFT"] = eftNS;



    //NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Data): " << std::setw(2) << m_SDservTracker << '\n');

    // check if all data has been received now
    int allRxed = 1;
    for (auto& faceIterator : m_SDservTracker[rxedDataNameAndHash]["faceOUT"].items())
    {
      if (m_SDservTracker[rxedDataNameAndHash]["faceOUT"][faceIterator.key()]["dataRx"] != 1)
      {
        allRxed = 0;
      }
    }



    // Only when ALL interests have been satisfied (out of all the faces where we sent them out), will we generate the data packet(s) downstream with the overall lowest EFT.
    if (allRxed == 1)
    {
      NFD_LOG_DEBUG("NFDServiceDiscovery, all data packets for " << rxedDataNameAndHash << " have been received on all faces!!! Calculating best EFT and generating data packet downstream");
      // The new data packet going back downstream will contain: 
      // "Pruned DAG (pDAG) Service name" that it is being hosted and requested (serviceS/PWFH).
      // Calculate EFT (earliest finish time) and include it (lowest EFT of all the faces).
      // Timestamp of when data packet leaves (to measure delay to downstream nodes).

      NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Data after allRxed): " << std::setw(2) << m_SDservTracker << '\n');


      // this lowestEFT calculation determines the EFT for the path that can get results the quickest.
      int64_t lowestEFT = -1;  // initialize to invalid EFT
      int64_t lowestNonLocalEFT = -1;  // initialize to invalid EFT
      std::string lowestFace = "";
      std::string lowestNonLocalFace = "";
      for (auto& faceIterator : m_SDservTracker[rxedDataNameAndHash]["faceOUT"].items())
      {
        // figure out which is the lowest EFT of all the upstream faces that we've received packets for so far
        int64_t thisEFT = m_SDservTracker[rxedDataNameAndHash]["faceOUT"][faceIterator.key()]["EFT"];
        NFD_LOG_DEBUG("NFDServiceDiscovery, EFT for face " << faceIterator.key() << ": " << thisEFT);
        if (lowestEFT == -1)
        {
          lowestEFT = thisEFT; // initialize to the first one
          lowestFace = faceIterator.key(); // initialize to the first one
        }
        else if (thisEFT < lowestEFT)
        {
          lowestEFT = thisEFT; // this becomes the lowest EFT found so far
          lowestFace = faceIterator.key(); // initialize to the first one
        }

        // also keep track of lowest EFT for "non-local" faces
        Face* realFaceIterator;
        for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it)
        {
          realFaceIterator = &*it;
          if (std::to_string(realFaceIterator->getId()) == faceIterator.key())
          {
            if (realFaceIterator->getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL)
            {
              if (lowestNonLocalEFT == -1)
              {
                lowestNonLocalEFT = thisEFT; // initialize to the first one
                lowestNonLocalFace = faceIterator.key(); // initialize to the first one
              }
              else if (thisEFT < lowestNonLocalEFT)
              {
                lowestNonLocalEFT = thisEFT; // this becomes the lowest EFT found so far
                lowestNonLocalFace = faceIterator.key(); // initialize to the first one
              }
            }
          }
        }

      }

      NFD_LOG_DEBUG("NFDServiceDiscovery, lowestEFT is " << lowestEFT << " on face " << lowestFace << ", lowestNonLocalEFT is " << lowestNonLocalEFT << " on face " << lowestNonLocalFace << ". Now determining CPU scheduling...");




      // DETERMINE CPU SCHEDULING

      // if the lowestEFT calculated above is from a local face, then we must calculate what the new EFT would be after scheduling the service in this node.
      Face* lowestCostFace;
      for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it)
      {
        lowestCostFace = &*it;
        NFD_LOG_DEBUG("NFDServiceDiscovery, evaluating face " << std::to_string(lowestCostFace->getId()) );
        if (std::to_string(lowestCostFace->getId()) == lowestFace)
        {
          NFD_LOG_DEBUG("NFDServiceDiscovery, lowestCostFace found: " << lowestFace);
          break;
        }
      }

      NFD_LOG_DEBUG("NFDServiceDiscovery, lowestCostFace: " << lowestCostFace);

      if (lowestCostFace->getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
      {
        NFD_LOG_DEBUG("NFDServiceDiscovery, lowestCostFace is local.");
        // determine what the new EFT would be after scheduling (could be seriously delayed more if node is very busy)


/*        
        int64_t earliestStartPossible = -1;
        for (auto& serviceIterator : m_SDservTracker.items())
        {
          if (m_SDservTracker[serviceIterator.key()]["faceIN"].contains("serviceScheduling"))
          {
            if (m_SDservTracker[serviceIterator.key()rviceScheduling"]["end"] < lowestEFT) // if the service end time is before the inputs are ready, we don't even care about this service
              break;
            //else we do have to take it into account
            if (it fits before this service starts)
              schedule it right at the lowestEFT value
            else (it doesn't fit before this service starts)
              schedule it right after this service ends
          }
        }
*/

        int64_t earliestStartPossible = -1;
        int64_t earliestEndPossible = -1;


        NFD_LOG_DEBUG("NFDServiceDiscovery, parsing all scheduled items into vector for sorting...");
        // parse all scheduled items into vector (so we can later sort them)
        std::vector<Service> scheduledVector;
        for (auto& serviceIterator : m_SDservTracker.items())
        {
          if (m_SDservTracker[serviceIterator.key()]["faceIN"].contains("serviceScheduling"))
          {
            scheduledVector.push_back({
                serviceIterator.key(),
                m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["start"],
                m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["end"]
            });
          }
        }

        // Ensure sorted
        std::sort(scheduledVector.begin(), scheduledVector.end(),
                  [](const Service& a, const Service& b){
                      return a.start < b.start;
                  });

        NFD_LOG_DEBUG("NFDServiceDiscovery, sorted scheduled items: ");
        for (size_t i = 0; i < scheduledVector.size(); i++)
        {
          NFD_LOG_DEBUG("  item " << i << " name: " << scheduledVector[i].name << ", start: " << scheduledVector[i].start << ", end: " << scheduledVector[i].end);
        }

        // recall serviceLatency from datastructure (we saved it when the data packet from the local face came in - not necessarily the latest received data packet, which is why we need to grab the stored value from the data structure)
        serviceLatency = m_SDservTracker[rxedDataNameAndHash]["faceOUT"][lowestFace]["serviceLatency"];

        bool spotFound = false;
        // 0. check if no other service has been scheduled yet
        if (scheduledVector.empty())
        {
          earliestStartPossible = lowestEFT;
          earliestEndPossible = lowestEFT + serviceLatency;
          spotFound = true;
          NFD_LOG_DEBUG("NFDServiceDiscovery scheduling - vector was empty (no existing scheduled services). earliestStartPossible: " << earliestStartPossible << ", + serviceLatency: " << serviceLatency << " = earliestEndPossible: " << earliestEndPossible);
        }

        // 1. check before first existing service
        else if (lowestEFT + serviceLatency <= scheduledVector[0].start)
        {
          earliestStartPossible = lowestEFT;
          earliestEndPossible = lowestEFT + serviceLatency;
          spotFound = true;
          NFD_LOG_DEBUG("NFDServiceDiscovery scheduling - inserting before first existing service. earliestStartPossible: " << earliestStartPossible << ", earliestEndPossible: " << earliestEndPossible);
        }

        // 2. check between existing services
        else
        {
          for (size_t i = 0; i + 1 < scheduledVector.size(); i++)
          {
              int64_t earliest = std::max(lowestEFT, scheduledVector[i].end);
              int64_t gapEnd = scheduledVector[i+1].start;
              if (earliest + serviceLatency <= gapEnd)
              {
                earliestStartPossible = earliest;
                earliestEndPossible = earliest + serviceLatency;
                spotFound = true;
                NFD_LOG_DEBUG("NFDServiceDiscovery scheduling - inserting between existing services. earliestStartPossible: " << earliestStartPossible << ", earliestEndPossible: " << earliestEndPossible);
              }
          }
        }

        if (spotFound == false)
        {
          // 3. no gap big enough for this service -> schedule after the last existing service
          int64_t start = std::max(lowestEFT, scheduledVector.back().end);
          earliestStartPossible = start;
          earliestEndPossible = start + serviceLatency;
          spotFound = true;
          NFD_LOG_DEBUG("NFDServiceDiscovery scheduling - inserting after last existing service. earliestStartPossible: " << earliestStartPossible << ", earliestEndPossible: " << earliestEndPossible);
        }


        NFD_LOG_DEBUG("NFDServiceDiscovery re-evaluating lowest EFT after scheduling - earliestEndPossible: " << earliestEndPossible << ", lowestNonLocalEFT: " << lowestNonLocalEFT);





        // Then re-evaluate if running locally is still the lowest EFT (or if it's our only choice - in which case lowestNonLocalEFT would still be zero).
        if ((earliestEndPossible < lowestNonLocalEFT) || (lowestNonLocalEFT == -1)) // if yes, then schedule it locally
        {
          if (m_SDservTracker[rxedDataNameAndHash]["faceIN"].contains("serviceScheduling"))
          {
            NFD_LOG_DEBUG("NFD SD Forwarding ERROR!! This service has already been scheduled!!!!");
          }
          NFD_LOG_DEBUG("NFDServiceDiscovery - SCHEDULING TO RUN LOCALLY!!!");
          m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["start"] = earliestStartPossible;
          m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["end"] = earliestEndPossible;
          lowestEFT = earliestEndPossible;
          //lowestFace = lowestFace; // if we are here, lowestFace will be the local face already.

        }
        else // Running locally is no longer the lowest EFT, so don't schedule the task and use the other face (lowestNonLocalFace)
        {
          // update lowestEFT and lowestFace variables to be the non-local one (with lowestNonLocalEFT)
          NFD_LOG_DEBUG("NFDServiceDiscovery - NOT SCHEDULING, RUNNING ELSEWHERE UPSTREAM!!!");
          lowestEFT = lowestNonLocalEFT;
          lowestFace = lowestNonLocalFace;
        }

      }


      NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Data after scheduled): " << std::setw(2) << m_SDservTracker << '\n');


      // Loop through all faceOUTs (local and non-local), and send schedulerRelease message to each face only if it is a non-local faces AND is not the lowestCostFace

      // TODO: package the following code into a function - sendInterestUpstreamToUnSchedule(uniqueHistoricalName&pDAG);
      shared_ptr<Interest> interestSchedulerRelease = make_shared<Interest>();
      interestSchedulerRelease->setName("/nesco/schedulerRelease");
      std::string appParamString = "/nesco/serviceDiscovery" + rxedDataNameAndHash;
      //std::cout << "appParamString: " << appParamString << std::endl;
      // in order to convert from std::string to a char[] datatype we do the following (https://stackoverflow.com/questions/7352099/stdstring-to-char):
      char *newAppParamString = new char[appParamString.length() + 1];
      strcpy(newAppParamString, appParamString.c_str());
      size_t length = strlen(newAppParamString);
      interestSchedulerRelease->setApplicationParameters((const uint8_t *)newAppParamString, length);


      for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it)
      {
        Face* thisFace = &*it;
        for (auto& faceIterator : m_SDservTracker[rxedDataNameAndHash]["faceOUT"].items())
        {
          if (std::to_string(thisFace->getId()) == faceIterator.key())
          {
            if ((faceIterator.key() != lowestFace) && (thisFace->getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL))
            {
              NFD_LOG_DEBUG("NFDServiceDiscovery - sending schedulerRelease message for " << rxedDataNameAndHash << " upstream through face " << thisFace->getId() << std::endl);
              thisFace->sendInterest(*interestSchedulerRelease);
            }
          }
        }
      }


      NFD_LOG_DEBUG("NFDServiceDiscovery, scheduling done, generating FIB entry now...");


      // GENERATE NEW FIB ENTRY

      // create the FIB entry, so that when the workflow runs, we route through the face that has the lowest EFT.
      // The node will record the lowest EFT cost in the FIB by creating a new table entry using the workflow pDAG name (not the serviceDiscovery pDAG name).
      // The cost will be EFT in nano-seconds. This EFT is units of time after the initial interest is generated.
      //auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
      //Face* lowestCostFace;
      for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it)
      {
        lowestCostFace = &*it;
        if (std::to_string(lowestCostFace->getId()) == lowestFace)
        {
          break;
        }
      }

      // create name&pDAG just like it will be created by the regular consumer.
      ndn::Name futureName;
      futureName = (data.getName()).getPrefix(-1); // remove the last component of the name (the parameter digest) so we have just the raw name
      futureName = futureName.getSubName(2,1); // remove the zeroeth component of the name (/nesco), and the first component of the name (/serviceDiscovery). starting at component 2, keep 1 component
      std::string futureNameString = "/nesco" + futureName.toUri();

      json dagObject;
      dagObject["dag"]  = m_SDservTracker[rxedDataNameAndHash]["faceIN"]["dag"];
      dagObject["head"] = m_SDservTracker[rxedDataNameAndHash]["faceIN"]["head"];
      std::string updatedDagString = dagObject.dump();
      // in order to convert from std::string to a char[] datatype we do the following (https://stackoverflow.com/questions/7352099/stdstring-to-char):
      char *dagStringParameter = new char[updatedDagString.length() + 1];
      strcpy(dagStringParameter, updatedDagString.c_str());
      size_t lengthParam = strlen(dagStringParameter);

      shared_ptr<Interest> dummyInterest = make_shared<Interest>();
      dummyInterest->setName(futureNameString);
      dummyInterest->setApplicationParameters((const uint8_t *)dagStringParameter, lengthParam);
      futureName = dummyInterest->getName();

      // if it is a local face (to an application - to a locally hosted service), we don't create the FIB entry, and instead rely on the 0 cost regular FIB entry from the service itself.
        // this is because the recorded face with lowest EFT is for the serviceDiscovery service' face, not the actual workflow service's face. Each application gets its own local face.
      if (lowestCostFace->getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
      {
        // if there is an existing FIB entry for this name&pDAG, remove it. We need to forward to this local face using regular FIB entry with just service name and cost 0.
        fib::Entry* exact = m_fib.findExactMatch(futureName);
        if (exact != nullptr) {
          m_fib.erase(futureName);
          NFD_LOG_DEBUG("NFDServiceDiscovery, removed FIB entry for " << futureName.toUri());
        }
      }
      // otherwise, if it is a non-local face, we would be going out to another NFD node, and thus we create a new FIB entry with that non-local face.
      if (lowestCostFace->getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL)
      {
        fib::Entry* entry = m_fib.insert(futureName).first;
        m_fib.addOrUpdateNextHop(*entry, *lowestCostFace, lowestEFT);
        NFD_LOG_DEBUG("NFDServiceDiscovery, addNextHopRecord for " << futureName.toUri() << " added, with face " << lowestCostFace->getId() << ", and cost " << lowestEFT);
      }





      // create data packet, but use stored name/hash!
      std::string storedName = m_SDservTracker[rxedDataNameAndHash]["faceIN"]["inName"];
      auto new_data = std::make_shared<ndn::Data>(storedName);
      new_data->setFreshnessPeriod(data.getFreshnessPeriod());

      unsigned char myBuffer[1024];
      json dataPacketContents;
      ns3::Time timeNow;
      timeNow = ns3::Simulator::Now();
      // Convert to integer in milliseconds and then to string
      int64_t timeNowNS = timeNow.ToInteger(ns3::Time::NS);
      std::string timeStringNS = std::to_string(timeNowNS);
      dataPacketContents["txTime"] = timeNowNS;
      dataPacketContents["EFT"] = lowestEFT;

      std::string dataPacketString = dataPacketContents.dump();
      
      //NFD_LOG_DEBUG("The data packet EFT (lowest) is " << lowestEFT);
      //NFD_LOG_DEBUG("The data packet string is " << dataPacketString);

      // instead of just writing a single value to the buffer, now we write the JSON data structure containing EFT and tx timestamp
      // write to the buffer, after making sure it's big enough
      if (strlen(dataPacketString.c_str())+1 > 1024) // string length plus NULL terminating character
      {
        NFD_LOG_DEBUG("NFD SD Forwarding ERROR!! The data packet size is larger than 1024!!!");
      }
      //else
      //{
        //NFD_LOG_DEBUG("The data packet size using strlen+1 is " << strlen(dataPacketString.c_str())+1);
        //NFD_LOG_DEBUG("The data packet size using length+1 operator is " << dataPacketString.length()+1);
      //}
      memcpy(myBuffer, dataPacketString.c_str(), strlen(dataPacketString.c_str())+1);
      //new_data->setContent(myBuffer, 1024); // make the data always 1024 bytes long
      new_data->setContent(myBuffer, strlen(dataPacketString.c_str())+1); // make the data just big enough to fit the json object

      new_data->setSignatureInfo(ndn::SignatureInfo(tlv::NullSignature));
      new_data->setSignatureValue(std::make_shared<ndn::Buffer>());
      new_data->wireEncode();
      //NFD_LOG_DEBUG("Sending Data packet for " << new_data->getName());


      // we now only have one IN face
      Face* downFace = m_faceTable.get(m_SDservTracker[rxedDataNameAndHash]["faceIN"]["inID"]);
      NFD_LOG_DEBUG("NFDServiceDiscovery, data packet for " << rxedDataNameAndHash << " is being sent downstream through face " << m_SDservTracker[rxedDataNameAndHash]["faceIN"]["inID"]);

      this->onOutgoingData(*new_data, *downFace);


      // clear m_SDservTracker faceOUT entry FOR THIS SERVICE ONLY, so that if a new interest is received, we go looking for inputs again
      //NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - removing m_SDservTracker entry for " << rxedDataNameAndHash << '\n');
      //m_SDservTracker.erase(rxedDataNameAndHash); // erase the entire entry
      //m_SDservTracker[rxedDataNameAndHash].erase("faceOUT"); // only erase the "faceOUT" portion, otherwise we would be removing the CPU scheduling information!
      // we can't just erase the faceOUT portion, because I'm trying to use it for dissiminating schedulerRelease messages too! So instead we just reset all the values.
      for (auto& faceIterator : m_SDservTracker[rxedDataNameAndHash]["faceOUT"].items())
      {
        m_SDservTracker[rxedDataNameAndHash]["faceOUT"][faceIterator.key()]["intTx"] = 0;
        m_SDservTracker[rxedDataNameAndHash]["faceOUT"][faceIterator.key()]["dataRx"] = 0;
        m_SDservTracker[rxedDataNameAndHash]["faceOUT"][faceIterator.key()]["linkDelay"] = -1;
        m_SDservTracker[rxedDataNameAndHash]["faceOUT"][faceIterator.key()]["EFT"] = -1;
      }
      NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Data after sending downstream): " << std::setw(2) << m_SDservTracker << '\n');

    } // end if (allRxed)

  } // end if (name1String == "/serviceDiscovery")


  else // regular data packet processing
  {

    // receive Data
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
        NFD_LOG_DEBUG("Node " << (*node)->GetId() << " does not export GlobalRouter interface");
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

      // NFD_LOG_DEBUG (predecessors.size () << ", " << distances.size ());

      Ptr<L3Protocol> L3protocol = (*node)->GetObject<L3Protocol>();
      shared_ptr<nfd::Forwarder> forwarder = L3protocol->getForwarder();

      NFD_LOG_DEBUG("Reachability from Node: " << source->GetObject<Node>()->GetId());
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
              NFD_LOG_DEBUG(" prefix " << prefix << " reachable via face " << *std::get<0>(dist.second)
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
