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
        //this->onIncomingInterest(interest, FaceEndpoint(const_cast<Face&>(face), endpointId));
        this->processIncomingInterest(interest, FaceEndpoint(const_cast<Face&>(face), endpointId));
        //this->processIncomingInterestMutex(interest, FaceEndpoint(const_cast<Face&>(face), endpointId));
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
  m_FibOwnerTracker.clear();
  //m_interestMutex.unlock();
  //m_resourceMutex.unlock();
  m_interestBusy = false;
  m_resourceBusy = false;
  auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
  NFD_LOG_INFO("NFD is running on node " << (*node).GetId());
}

Forwarder::~Forwarder() = default;



/*
This is the structure of the JSON object that we use to track interest and data packets for Service Discovery

m_SDservTracker = {
  "/service1/faceInIdString1&pDAG_param_hash": {      // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
      "faceIN": {
          "inID": faceID1,                            // faceID where an interest for this service/pDAG has been received.
          "inName": "/service1/SDpDAG_param_hash",    // Notice we store the original name as received so we can respond with the same name when data arrives.
          "serviceDiscovery": <0 or 1>,               // We add the setting for service discovery: 0 - disabled, 1 - enabled
          "resourceAllocation": <0 or 1>,             // We add the setting for resource allocation: 0 - disabled, 1 - enabled
          "allocationReuse": <0 or 1>,                // We add the setting for allocation reuse of previous slots: 0 - disabled, 1 - enabled
          "scheduleCompaction": <0 or 1>,             // We add the setting for schedule compaction: 0 - disabled, 1 - enabled
          "dag": { <pDag as received> },              // We add the pDAG here so that we can generate the name we use for the FIB entry that we create at the end. (serviceDiscovery interest application parameter has more info than regular workflow interest).
          "head": <service head as received>,         // We add the service head so that we can generate the name we use for the FIB entry that we create at the end. (serviceDiscovery interest application parameter has more info than regular workflow interest).
          "serviceDiscoveryStartTimeNS": <absolute SD start time>,      // set and initially sent by the consumer
          "workflowStartTimeNS": <absolute estimated WF start time>,    // set and initially sent by the consumer
          "WFinterestRxedTime": <absolute time when WF interest is estimated to be received>,   // Used for calculating EFT in caching nodes>
          "serviceScheduling": {                      // If the service has been scheduled to run in this node, we will see this entry. Otherwise, it won't exist
            "WFnameAndHash": "/service1/WFpDAG_param_hash",             // WFnameAndHash name could match with other ones below
            "inputsReadyTime": <absolute time when all inputs have been received>,
            "start": <absolute start time>,
            "end": <absolute end time>,
            "face": <faceID for service>              // this should always be the local face
          }
      },
      "faceOUT": {
          "faceID3": {                // faceID where the interest has been forwarded to
              "intTx": 1,             // interest has already been generated on this upstream face
              "dataRx": 1,            // data packet has already been received from this upstream face
              "linkDelay": 2,         // 2ms link delay to the next node upstream
              "EFT": 3,               // 3ms is the EFT upstream
              "serviceLatency": <in nanoseconds>   // this will only be present for local faces!
          },
          "faceID4": {                // faceID where the interest has been forwarded to
              "intTx": 0,             // interest has not been generated
              "dataRx": 0,            // data packet has not been received from this upstream face
              "linkDelay": -1,        // default value is -1 (data packet not received yet)
              "EFT": -1               // default value is -1 (data packet not received yet)
          }
      }
  },
  "/service1/faceInIdString2&pDAG_param_hash": {      // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
      "faceIN": {
          "inID": faceID1,                            // faceID where an interest for this service/pDAG has been received.
          "inName": "/service1/SDpDAG_param_hash",    // Notice we store the original name as received so we can respond with the same name when data arrives.
          "serviceDiscovery": <0 or 1>,               // We add the setting for service discovery: 0 - disabled, 1 - enabled
          "resourceAllocation": <0 or 1>,             // We add the setting for resource allocation: 0 - disabled, 1 - enabled
          "allocationReuse": <0 or 1>,                // We add the setting for allocation reuse of previous slots: 0 - disabled, 1 - enabled
          "scheduleCompaction": <0 or 1>,             // We add the setting for schedule compaction: 0 - disabled, 1 - enabled
          "dag": { <pDag as received> },              // We add the pDAG here so that we can generate the name we use for the FIB entry that we create at the end. (serviceDiscovery interest application parameter has more info than regular workflow interest).
          "head": <service head as received>,         // We add the service head so that we can generate the name we use for the FIB entry that we create at the end. (serviceDiscovery interest application parameter has more info than regular workflow interest).
          "serviceDiscoveryStartTimeNS": <absolute SD start time>,      // set and initially sent by the consumer
          "workflowStartTimeNS": <absolute estimated WF start time>,    // set and initially sent by the consumer
          "WFinterestRxedTime": <absolute time when WF interest is estimated to be received>,   // Used for calculating EFT in caching nodes>
          "serviceScheduling": {                      // If the service has been scheduled to run in this node, we will see this entry. Otherwise, it won't exist
            "WFnameAndHash": "/service1/WFpDAG_param_hash",             // WFnameAndHash name could match with other ones below
            "inputsReadyTime": <absolute time when all inputs have been received>,
            "start": <absolute start time>,
            "end": <absolute end time>,
            "face": <faceID for service>              // this should always be the local face
      },
      "faceOUT": {
          "faceID3": {                // faceID where the interest has been forwarded to
              "intTx": 1,             // interest has already been generated on this upstream face
              "dataRx": 1,            // data packet has already been received from this upstream face
              "linkDelay": 2,         // 2ms link delay to the next node upstream
              "EFT": 3,               // 3ms is the EFT upstream
              "serviceLatency": <in nanoseconds>   // this will only be present for local faces!
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
     etc...
  }
}


// We are keeping a local custom fib datastructure where we can store the full SD name - /serviceX/faceInIdStringx&pDAG_param_hash (rxedDataNameAndHash). If we no longer need the fib entry (because it's not optimal), we can then check and see if there
//   are still other fib entries for this futureWFnameAndHash on that same face, and if there are, we don't remove the actual FIB entry. We only remove it once there are no more entries in the custom FIB for that particular face.
// We can't just delete the FIB entry if it exists because we may have several "active" requests with unique paths. Only the current path that a received data packet was for has finished being analyzed. Other paths may still be optimal,
//   but since they all share the same full WF name - /serviceX/WFpDAG_param_hash (futureWFnameAndHash) for each face, removing this one would remove the other one(s) for that face too.

m_FibOwnerTracker = {
    "/service1/WFpDAG_param_hash": {                        // key has full WF name service/pDAG
        "fibEntryExists": 0/1,                              // if any of the faceIDs below claim a fibOwner, this value will be 1, otherwise 0. Represents the actual FIB entry existing or not.
        "/service1/faceInIdString1&pDAG_param_hash": {      // key has full SD name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
          "faceOUT": {
            "faceID3": {                                    // faceID where the interest has been forwarded to
                "EFT": 3,                                   // 3ms is the EFT upstream
                "fibOwner": 0                               // tells us if this is the entry that currently defined the FIB entry. Only one per /service1/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
            },
            "faceID4": {                                    // faceID where the interest has been forwarded to
                "EFT": 2,                                   // 2ms is the EFT upstream
                "fibOwner": 1                               // tells us if this is the entry that currently defined the FIB entry. Only one per /service1/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
            },
            "faceID5": {                                    // faceID where the interest has been forwarded to
                "EFT": -1                                   // default value is -1 (data packet not received yet)
                "fibOwner": 0                               // tells us if this is the entry that currently defined the FIB entry. Only one per /service1/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
            }
          }
        }
        "/service1/faceInIdString2&pDAG_param_hash": {      // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
          "faceOUT": {
            "faceID3": {                                    // faceID where the interest has been forwarded to
                "EFT": 6,                                   // this is the EFT upstream
                "fibOwner": 0                               // tells us if this is the entry that currently defined the FIB entry. Only one per /service1/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
            },
            "faceID5": {                                    // faceID where the interest has been forwarded to
                "EFT": -1                                   // default value is -1 (data packet not received yet)
                "fibOwner": 0                               // tells us if this is the entry that currently defined the FIB entry. Only one per /service1/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
            }
          }
        }
    },
    "/service2/WFpDAG_param_hash": {                        // key has full name service/pDAG
        etc...
    }
}

m_FibOwnerTracker = {
    "/service1/WFpDAG_param_hash": {                        // key has full WF name service/pDAG
        "/service1/faceInIdString1&pDAG_param_hash": 0,     // key has full SD name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
                                                                // value tells us if this is the entry that currently defined the FIB entry. Only one per /serviceX/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
        "/service1/faceInIdString2&pDAG_param_hash": 1,     // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
                                                                // value tells us if this is the entry that currently defined the FIB entry. Only one per /serviceX/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
        "/service1/faceInIdString3&pDAG_param_hash": 0      // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
                                                                // value tells us if this is the entry that currently defined the FIB entry. Only one per /serviceX/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
    },
    "/service2/WFpDAG_param_hash": {                        // key has full name service/pDAG
        "/service2/faceInIdString1&pDAG_param_hash": 1,     // key has full SD name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
                                                                // value tells us if this is the entry that currently defined the FIB entry. Only one per /serviceX/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
        "/service2/faceInIdString2&pDAG_param_hash": 0      // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
                                                                // value tells us if this is the entry that currently defined the FIB entry. Only one per /serviceX/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
    },
    "/service3/WFpDAG_param_hash": {                        // key has full name service/pDAG
        etc...
    }
}





*/


/*
void
Forwarder::processIncomingInterestMutex(const Interest& interest, const FaceEndpoint& ingress)
{
  auto interestPtr = std::make_shared<Interest>(interest);
  auto ingressPtr = std::make_shared<FaceEndpoint>(ingress);

  std::unique_lock<std::mutex> lock(m_interestMutex); // blocks here if another interest is currently processing
  // mutex successfully locked here
  // Schedule the release (after interest processing)
  ns3::Simulator::Schedule(ns3::MilliSeconds(1), &Forwarder::doneProcessingIncomingInterestMutex, this, *interestPtr, *ingressPtr, m_interestMutex);

  // Resource is busy processing another interest, retry later
  //ns3::Simulator::Schedule(ns3::NanoSeconds(100), &Forwarder::processIncomingInterestMutex, this, *interestPtr, *ingressPtr);
  //return;

}
void
Forwarder::doneProcessingIncomingInterestMutex(const Interest& interest, const FaceEndpoint& ingress)
{
  auto interestPtr = std::make_shared<Interest>(interest);
  auto ingressPtr = std::make_shared<FaceEndpoint>(ingress);

  m_interestMutex.unlock();
  Forwarder::onIncomingInterest(*interestPtr, *ingressPtr); // finish processing the incoming interest packet.
}
*/


void
Forwarder::processIncomingInterest(const Interest& interest, const FaceEndpoint& ingress)
{
  auto interestPtr = std::make_shared<Interest>(interest);
  auto ingressPtr = std::make_shared<FaceEndpoint>(ingress);

  if (m_interestBusy) {
    // Resource is busy processing another interest, retry later
    ns3::Simulator::Schedule(ns3::MicroSeconds(1), &Forwarder::processIncomingInterest, this, *interestPtr, *ingressPtr);
    return;
  }

  // Acquire resource - if we comment out the following line, we will be adding in a delay for interest processing, but we would allow multiple interests to be processed at the same time.
  m_interestBusy = true;

  // Schedule the release (after interest processing)
  ns3::Simulator::Schedule(ns3::MicroSeconds(200), &Forwarder::doneProcessingIncomingInterest, this, *interestPtr, *ingressPtr);
}
void
Forwarder::doneProcessingIncomingInterest(const Interest& interest, const FaceEndpoint& ingress)
{
  auto interestPtr = std::make_shared<Interest>(interest);
  auto ingressPtr = std::make_shared<FaceEndpoint>(ingress);

  m_interestBusy = false;
  Forwarder::onIncomingInterest(*interestPtr, *ingressPtr); // finish processing the incoming interest packet.
}




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
    if (interest.getName().getPrefix(-1).getSubName(1,1).toUri() == "/serviceDiscovery" ||
        interest.getName().getPrefix(-1).getSubName(1,1).toUri() == "/schedulerRelease")
    {
      if (ingress.face.getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
      {
        NFD_LOG_INFO("     CABEEE: onIncomingSDInterestFromApp (from consuming application only) =" << " name=" << interest.getName());
      }
      else
      {
        NFD_LOG_INFO("     CABEEE: onIncomingSDInterestFromFace (from another NFD node on a physical face) =" << " name=" << interest.getName());
      }
    }
    else
    {

/*
auto dagParameterFromInterest = interest.getApplicationParameters();
std::string dagString = std::string(reinterpret_cast<const char*>(dagParameterFromInterest.value()), dagParameterFromInterest.value_size());
//NFD_LOG_INFO("NFDServiceDiscovery, FIB entry future WF name&hash is " << interest.getName());
//NFD_LOG_INFO("NFDServiceDiscovery, FIB entry future WF name&hash contains application parameters: " << dagString);

ns3::Time timeNow;
timeNow = ns3::Simulator::Now();
// Convert to integer in milliseconds and then to string
int64_t timeNowNS = timeNow.ToInteger(ns3::Time::NS); // extract the time in nano-seconds so that we have enough granularity to guarantee interest uniqueness.

if (timeNowNS > 2000000000) { // make sure we are only looking at WF interests (after the workflow has started)
//NFD_LOG_INFO("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Interest): " << std::setw(2) << m_SDservTracker << '\n');

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
      //if (entryString == "/nesco" && serviceString == dagObject["head"])
      if (entryString == "/nesco")
      {
        if (fib_iterator->hasNextHops())
        {
          // figure out the faceID of all the nexthops in the list, and print them
          const fib::NextHopList& hopList = fib_iterator->getNextHops();
          for (nfd::fib::NextHopList::const_iterator hop_iterator = hopList.begin(); hop_iterator != hopList.end(); ++hop_iterator)
          {
            NFD_LOG_INFO("CABEEEfibEntries: interest " << fib_iterator->getPrefix().toUri() << ", faceID: " << hop_iterator->getFace().getId() << ", cost: " << hop_iterator->getCost());
          }
        }
      }
    }
  }

}
*/


      if (ingress.face.getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
      {
        NFD_LOG_INFO("     CABEEE: onIncomingWFInterestFromApp (from consuming application only) =" << " name=" << interest.getName());
      }
      else
      {
        NFD_LOG_INFO("     CABEEE: onIncomingWFInterestFromFace (from another NFD node on a physical face) =" << " name=" << interest.getName());
      }
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
//NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on schedulerReleaseFromApp Interest): " << std::setw(2) << m_SDservTracker << '\n');
    for (auto& serviceIterator : m_SDservTracker.items())
    {
      //NFD_LOG_DEBUG("NFDServiceDiscovery - schedulerRelease evaluating service " << serviceIterator.key() << " with inName " << m_SDservTracker[serviceIterator.key()]["faceIN"]["inName"] << std::endl);
      if (m_SDservTracker[serviceIterator.key()]["faceIN"]["inName"] == nameAndHashToRemove)
      {
//NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure for just the service that will be deallocated (on interest for schedulerRelease): " << '\n' << serviceIterator.key() << '\n' << std::setw(2) << m_SDservTracker[serviceIterator.key()] << '\n');
        if (m_SDservTracker[serviceIterator.key()]["faceIN"].contains("serviceScheduling"))
        {
          NFD_LOG_DEBUG("NFDServiceDiscovery - schedulerRelease removing scheduled service " << nameAndHashToRemove << std::endl);

          // insert info level message with node id, start and stop time, so process script can see it
          auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
          NFD_LOG_INFO("NFDServiceDiscovery - SDresourceAllocation: Service " << serviceIterator.key() << " no longer scheduled on node " << (*node).GetId() << " starting at " << m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["start"] << " and ending at " << m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["end"] << " nanoseconds).");

          m_SDservTracker[serviceIterator.key()]["faceIN"].erase("serviceScheduling");


          // SCHEDULE COMPACTION - LEFT-PACKING
          if (m_SDservTracker[serviceIterator.key()]["faceIN"]["scheduleCompaction"] == 1)  // if we have it set up for doing schedule compaction
          {
            this->scheduleCompaction();
          }


          // Once a service APP receives this interest, it will look at this service's inputs, and generate schedulerRelease messages for those.
          NFD_LOG_DEBUG("NFDServiceDiscovery - schedulerRelease will now send request further upstream to the service APP using name " << serviceIterator.key() << std::endl);
          // send the request further upstream
          sendSchedulerReleaseInterestUpstream(serviceIterator.key(), "");
        }
        else
        {
          NFD_LOG_DEBUG("NFDServiceDiscovery - schedulerRelease will now send request further upstream using name " << serviceIterator.key() << std::endl);
          // send the request further upstream
          sendSchedulerReleaseInterestUpstream(serviceIterator.key(), "");
        }
      }
    }
    return;
  } // else



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

    NFD_LOG_DEBUG("NFDServiceDiscovery received interest with name " << rxedInterestNameAndHash << " where prev name&hash was " << dagObject["prevHash"]);

    dagObject["faceIN"] = faceInIdString;
    dagObject["prevHash"] = rxedInterestNameAndHash; // adding the previous name&hash add the full "historical" path of where the interest has come from, trying to make it unique, although faces may have same ID on different nodes.
   
    ns3::Time timeNow;
    timeNow = ns3::Simulator::Now();
    // Convert to integer in milliseconds and then to string
    int64_t timeNowNS = timeNow.ToInteger(ns3::Time::NS); // extract the time in nano-seconds so that we have enough granularity to guarantee interest uniqueness.
    //dagObject["rxTime"] = timeNowNS; // adding the time it was received, to make it truly unique.

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



    int64_t WFstartTimeNS = dagObject["workflowStartTimeNS"];
    int64_t SDstartTimeNS = dagObject["serviceDiscoveryStartTimeNS"];


    // if faceIN ID in this name entry doesn't exist, create it so that we know to send data packets out through it later
    //if (!m_SDservTracker[jsonName]["faceIN"].contains(faceInIdString))
    if (!m_SDservTracker.contains(jsonName))
    {
      //NFD_LOG_DEBUG("NFDServiceDiscovery adding this name to faceIN list: " << jsonName << " - faceID is: " << faceInIdString);
      m_SDservTracker[jsonName]["faceIN"]["inID"] = faceInId;                     // we record the faceID
      m_SDservTracker[jsonName]["faceIN"]["inName"] = interest.getName().toUri(); // we record the original name as it came in, so we know what name to use when we respond with data downstream.
      m_SDservTracker[jsonName]["faceIN"]["dag"] = dagObject["dag"];              // we capture the pDag here so that we can use it to generate the name we use for the FIB entry  we create later.
      m_SDservTracker[jsonName]["faceIN"]["head"] = dagObject["head"];            // we capture the "head" here so that we can use it to generate the name&hash we use for the FIB entry  we create later.
      m_SDservTracker[jsonName]["faceIN"]["serviceDiscovery"] = dagObject["serviceDiscovery"];  // we capture the setting here so that we know if we'll need to perform this later
      m_SDservTracker[jsonName]["faceIN"]["resourceAllocation"] = dagObject["resourceAllocation"];  // we capture the setting here so that we know if we'll need to perform this later
      m_SDservTracker[jsonName]["faceIN"]["allocationReuse"] = dagObject["allocationReuse"];  // we capture the setting here so that we know if we'll need to perform this later
      m_SDservTracker[jsonName]["faceIN"]["scheduleCompaction"] = dagObject["scheduleCompaction"];  // we capture the setting here so that we know if we'll need to perform this later
      m_SDservTracker[jsonName]["faceIN"]["WFinterestRxedTime"] = timeNowNS + WFstartTimeNS - SDstartTimeNS;  // we capture the "WFinterestRxedTime" here so that we can use it for allocation slot reuse calculations later.
    }
    //if (!m_SDservTracker[jsonName].contains("faceOUT"))
    //{
    //}



    //look at FIB, and see if this service is reachable out of any other faces. If so, send interest out through each face.
    unsigned char interestBudget = 2; // don't send out too many interests to avoid overwhelming the network. Pick the lowest cost (next hop) faces.
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

              if (interestBudget > 0)
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
                  //interestBudget--;

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
        }
        //else
          //NFD_LOG_DEBUG("NFDServiceDiscovery, fib_iterator does not have nextHops\n");
      }

    } // FIB iteration for loop

//NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Interest): " << std::setw(2) << m_SDservTracker << '\n');
    //std::cout << "\nnode " << (*node).GetId() << " NFDServiceDiscovery - m_SDservTracker data structure (on Interest) has " << m_SDservTracker.size() << " entries.\n";
    //NFD_LOG_INFO("\n\nnode " << (*node).GetId() << " NFDServiceDiscovery - m_SDservTracker data structure (on Interest) has " << m_SDservTracker.size() << " entries.\n");
   


    //this->countFaceOutsAndSatisfied(); // for debugging. Will count the number of faceOUT entries in this node, along with how many of those are satisfied

    
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
Forwarder::countFaceOutsAndSatisfied(void)
{
  // For debugging
  // Will count the number of faceOUT entries in the m_SDservTracker data structure for this node, and
  // see how many of those have already been satisfied.

  int totalFaceOuts = 0;
  int satisfiedFaceOuts = 0;
  for (auto& el : m_SDservTracker.items()) {
      const std::string& serviceKey = el.key();
      const json& entryValue = el.value();

      int faceOutCount = 0;

      // Check if faceOUT exists and is an object before counting
      if (entryValue.contains("faceOUT") && entryValue["faceOUT"].is_object()) {
          //faceOutCount = entryValue["faceOUT"].size();
          const json& faceOutMap = entryValue["faceOUT"];

          for (auto& face : faceOutMap.items()) {
              totalFaceOuts++;

              // Access the specific face object (e.g., faceID3)
              const json& faceStats = face.value();
              
              // Check if dataRx exists and equals 1
              if (faceStats.contains("dataRx") && faceStats["dataRx"] == 1) {
                  satisfiedFaceOuts++;
              }
          }
      }

      //totalFaceOuts += faceOutCount;

      //std::cout << "Entry: " << serviceKey << "  -> faceOUT entries: " << faceOutCount << "\n\n";
      //NFD_LOG_INFO("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Interest) entry has " << faceOutCount << " faceOUT entries\n");
  }
  auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
  std::cout << "node " << (*node).GetId() << " FaceOUTs (interests) " << totalFaceOuts << ", satisfied: " << satisfiedFaceOuts << "\n\n";

}



void
Forwarder::scheduleCompaction(void)
{
  // SCHEDULE COMPACTION - LEFT-PACKING --------------------------------------------------------------------------------------------------------------------------------

  // check if we can move other allocated services to run sooner. They will be limited by when they receive all their inputs.
  // Initially I can just make local adjustments. Eventually we can also communicate the earlier EFTs to services downstream and perhaps adjust their allocations too.

  // 1. Parse into vector (Storage)
  std::vector<Service> scheduledVector;
  // Use reserve if possible to prevent pointer invalidation, though not strictly required if we don't add elements later
  scheduledVector.reserve(m_SDservTracker.size()); 

  for (auto& serviceIterator : m_SDservTracker.items())
  {
    if (m_SDservTracker[serviceIterator.key()]["faceIN"].contains("serviceScheduling"))
    {
      scheduledVector.push_back({
          serviceIterator.key(),
          m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["inputsReadyTime"],
          m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["start"],
          m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["end"]
          //m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["face"] // no need, this field shouldn't be changing
      });
    }
  }

  // 2. Group by Time Slot
  // Map Key: {start, end} pair. 
  // Map Value: Vector of pointers to the Service objects in 'scheduledVector'
  // usage: std::map sorts by the Key automatically (Start time, then End time)
  std::map<std::pair<int64_t, int64_t>, std::vector<Service*>> timeSlotGroups;

  for (auto& s : scheduledVector)
  {
      timeSlotGroups[{s.start, s.end}].push_back(&s);
  }

  // 3. Compaction Loop (Iterate over GROUPS, not individual items)
  int64_t resourceFreeTime = 0; 
  std::vector<Service> newEFTsToReport;

  for (auto& group : timeSlotGroups)
  {
      // 'group.first' is the Pair {oldStart, oldEnd}
      // 'group.second' is the vector of Service pointers
      
      int64_t oldStart = group.first.first;
      int64_t oldEnd   = group.first.second;
      int64_t duration = oldEnd - oldStart;

      // A. Find the limiting 'inputsReadyTime' for this entire group
      //    The group cannot start until the SLOWEST member is ready.
      int64_t groupMaxInputsReady = 0;
      for (const auto* s : group.second)
      {
          groupMaxInputsReady = std::max(groupMaxInputsReady, s->inputsReadyTime);
      }

      // B. Calculate the new start time for the GROUP
      int64_t newStart = std::max(resourceFreeTime, groupMaxInputsReady);
      int64_t newEnd   = newStart + duration;

      // C. Apply changes to all members of the group
      for (auto* s : group.second) 
      {
          //TODO: if the difference is lower than a threshold, then don't bother sending an updated EFT message downstream!!
//NFD_LOG_INFO("NFDServiceDiscovery - SDresourceAllocation: Service " << s->name << " can gain " << (s->start - newStart) << " nanoseconds .");
          //if (newStart != s->start)
          if (newStart < s->start)
          //if (s->start - newStart > 1000000) // only report new EFT if the EFT gain is more than 1ms
          {

              // Logging & Updates
              NFD_LOG_DEBUG("NFDServiceDiscovery - schedulerRelease SCHEDULE COMPACTION - LEFT PACKING: sliding service " << s->name << " previously starting at " << s->start << " and now starting at " << newStart);
//NFD_LOG_INFO ("NFDServiceDiscovery - schedulerRelease SCHEDULE COMPACTION - LEFT PACKING: sliding service " << s->name << " previously starting at " << s->start << " and now starting at " << newStart);
              
              auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
              NFD_LOG_INFO("NFDServiceDiscovery - SDresourceAllocation: Service " << s->name << " no longer scheduled on node " << (*node).GetId() << " starting at " << s->start << " and ending at " << s->end << " nanoseconds).");
              NFD_LOG_INFO("NFDServiceDiscovery - SDresourceAllocation: Service " << s->name << " scheduled on node " << (*node).GetId() << " starting at " << newStart << " and ending at " << newEnd << " nanoseconds).");

              // To adjust EFTs, we keep track of which ones are moving. Once done, we will send messages downstream with updated EFTs after schedule compaction. This guarantees to send out in workflow order.
                newEFTsToReport.push_back({
                    s->name,
                    s->inputsReadyTime,
                    newStart,
                    newEnd
                });
          }

          // Update the Service object in the vector
          s->start = newStart;
          s->end   = newEnd;
      }

      // D. Advance resource cursor
      // We advance it by the duration of the group (since they run together)
      resourceFreeTime = newEnd;
  }

  // 4. Sync back to JSON
  std::unordered_map<std::string, const Service*> serviceMap;
  for (const auto& s : scheduledVector)
  {
      serviceMap[s.name] = &s;
  }

  for (auto& serviceIterator : m_SDservTracker.items())
  {
    if (m_SDservTracker[serviceIterator.key()]["faceIN"].contains("serviceScheduling"))
    {
      if (serviceMap.find(serviceIterator.key()) != serviceMap.end())
      {
        const Service* modified = serviceMap[serviceIterator.key()];

        //m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["inputsReadyTime"] = modified->inputsReadyTime; // no need, this field shouldn't be changing
        m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["start"] = modified->start;
        m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["end"]   = modified->end;
        //m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["face"]   = modified->face; // no need, this field shouldn't be changing
      }
    }
  }


  // now we can report the new EFTs for all the shifted tasks
  for (auto& s : newEFTsToReport)
  {
//NFD_LOG_INFO("NFDServiceDiscovery - schedule compaction sendEFTdataUpdate for " << s.name << ", new EFT: " << s.end << std::endl);
    this->sendEFTdataUpdate(s.name, s.end);
  }

  // END OF SCHEDULE COMPACTION - LEFT-PACKING ------------------------------------------------------------------------------------------------------------------------------
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
  

/*
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
          // figure out the faceID of all the nexthops in the list, and print them
          const fib::NextHopList& hopList = fib_iterator->getNextHops();
          for (nfd::fib::NextHopList::const_iterator hop_iterator = hopList.begin(); hop_iterator != hopList.end(); ++hop_iterator)
          {
            NFD_LOG_DEBUG("CABEEEfibEntries: interest " << fib_iterator->getPrefix().toUri() << ", faceID: " << hop_iterator->getFace().getId() << ", cost: " << hop_iterator->getCost());
          }
        }
      }
    }
  }
*/





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

/*
std::string
Forwarder::PruneDagWorkflow(const std::string& interestName, std::string dagString)
{

  auto dagObject = json::parse(dagString);

  // we PRUNE the DAG workflow to not include anything further downstream than this service

  // start by removing the head service received from downstream
  //std::cout << "DAG before erasing head: " << std::setw(2) << dagObject << '\n';
  dagObject["dag"].erase((std::string)dagObject["head"]);
  //std::cout << "DAG after erasing head: " << std::setw(2) << dagObject << '\n';

  char prunedLastIteration = 1;
  while (prunedLastIteration > 0)
  {
    prunedLastIteration = 0;

    //std::cout << "\n\n\n\nNew main iteration, looking for sink nodes in the following dag: " << std::setw(2) << dagObject << '\n';


    //find Sink Nodes
    std::list <std::string> listOfServicesWithInputs;   // keeps track of which services have inputs
    std::list <std::string> listOfRootServices;         // keeps track of which services don't have any inputs
    std::list <std::string> listOfSinkNodes;            // keeps track of which node doesn't have an output (usually this is just the consumer)
    for (auto& x : dagObject["dag"].items())
    {
      listOfRootServices.push_back(x.key()); // for now, add ALL keys to the list, we'll remove non-root ones later
      for (auto& y : dagObject["dag"][x.key()].items())
      {
        listOfServicesWithInputs.push_back(y.key()); // add all values to the list
        if ((std::find(listOfSinkNodes.begin(), listOfSinkNodes.end(), y.key()) == listOfSinkNodes.end())) // if y.key() does not exist in listOfSinkNodes
        {
          listOfSinkNodes.push_back(y.key()); // for now, add ALL values to the list, we'll remove non-sinks later
        }
      }
    }
    //std::cout << "removing services that feed into other services" << '\n';
    // now remove services that feed into other services from the list of sink nodes
    for (auto& x : dagObject["dag"].items())
    {
      if (!(std::find(listOfSinkNodes.begin(), listOfSinkNodes.end(), x.key()) == listOfSinkNodes.end())) // if x.key() exists in listOfSinkNodes
      {
        listOfSinkNodes.remove(x.key());
      }
    }
    //std::cout << "done finding sink nodes. Num found: " << std::to_string(listOfSinkNodes.size()) << '\n';


    // for each sink node found
    for (auto sinkNode : listOfSinkNodes) // for (each sink node)
    {
      //std::cout << "  Comparing sinkNode: " << sinkNode << " with interestName: " << interestName << '\n';
      //std::cout << "  prunedLastIteration = " << std::to_string(prunedLastIteration) << '\n';
      if (sinkNode != interestName) //this service name must include the "version", ex: "/service2/B"
      {
        // prune sink Node
        //std::cout << "ServiceDiscovery prunning current sink node: " << sinkNode << '\n';
        dagObject["dag"].erase(sinkNode);

        // now that the sink node has been pruned, remove it from all feeds from key services. Key services that end up as new sinks will be removed in next iteration.
        for (auto& x : dagObject["dag"].items())
        {
          char prunedLastIterationY = 1;
          while (prunedLastIterationY > 0) // since we have iteration loops that deal with key/value pairs, we can only prune one at a time. If more than one prunning is necessary, we need to re-iterate
          {
            for (auto& y : dagObject["dag"][x.key()].items())
            {
              prunedLastIterationY = 0;
              if (y.key() == sinkNode)
              {
                //std::cout << "   prunning sink node feed, key: " << x.key() << ", feed: " << y.key() << '\n';
                dagObject["dag"][x.key()].erase(y.key());
                //std::cout << "   after prunning feed: " << std::setw(2) << dagObject << '\n';
                prunedLastIterationY++;
                break;
              }
            }
            if (dagObject["dag"][x.key()].size() == 0)
              break;
          }
        }

        prunedLastIteration++;
        break;
      }
    }

    // now prune any keys that are left with no values
    char prunedLastIterationX = 1;
    while (prunedLastIterationX > 0) // since we have iteration loops that deal with key/value pairs, we can only prune one at a time. If more than one prunning is necessary, we need to re-iterate
    {
      prunedLastIterationX = 0;
      for (auto& x : dagObject["dag"].items())
      {
        if (dagObject["dag"][x.key()].size() == 0)
        {
          // x doesn't have any more feeds, we can prune it.
          //std::cout << "   no feeds left, prunning key: " << x.key() << '\n';
          dagObject["dag"].erase(x.key());
          //std::cout << "   after prunning key: " << std::setw(2) << dagObject << '\n';
          prunedLastIterationX++;
          break;
        }
      }
    }

  }

  // DAG now contains current interest service as the only sink

  // Now prune this service (upstream DAG should not contain this service as a source - but leave where it appears as a feed!)
  dagObject["dag"].erase(interestName);
  //std::cout << "All keys pruned: " << std::setw(2) << dagObject << '\n';


  dagObject["head"] = interestName;

  std::string updatedDagString = dagObject.dump();

  return updatedDagString;
}
*/


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
      if (localFace->getScope() == ndn::nfd::FACE_SCOPE_LOCAL) {
        NFD_LOG_DEBUG("cabeee CABEEEshortcutOPT, generating interest " << interestOPT << ", for local face " << localFace);
        // TODO: must iterate through all hosted services
        // TODO: must check if incoming interest is nescoSCOPT, and that hosted service name is not the same as incoming interest head
        // i.e., only generate shorcutOPT interest if the incoming interest is for /nescoSCOPT, and this fib entry is not for the service the interest is for (in which case the interest is forwarded to the service normally later on) 
        localFace->sendInterest(*interestOPT);
      }
    }

  }
  if (method==2) // iterate through all fib entries, then through all faces(hops) for each entry, and if entry is for /nescoSCOPT AND it is a local face, then send interest.
  {
    //NFD_LOG_DEBUG("CABEEEshortcutOPT, sending /shortcutOPT interest to apps on local faces to generate new interests for inputs into locally hosted services.");



/*
    // PRINT FIB ENTRIES
    for (fib::Fib::const_iterator fib_iterator = m_fib.begin(); fib_iterator != m_fib.end(); ++fib_iterator)
    {
      auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
      //NFD_LOG_DEBUG("CABEEEshortcutOPT, looking at fib entry\n");
      ndn::Name entryName;
      entryName = fib_iterator->getPrefix();
      entryName = entryName.getSubName(0,1); // starting at component 0, get 1 component (/nescoSCOPT only)
      std::string entryString = entryName.toUri();

      ndn::Name serviceName;
      serviceName = fib_iterator->getPrefix();
      serviceName = serviceName.getSubName(1,1); // starting at component 1, get 1 component (service name only)
      std::string serviceString = serviceName.toUri();
      NFD_LOG_DEBUG((*node).GetId() << " <--nodeID. CABEEEshortcutOPT, fib entry name is "<< entryString << serviceString);

        if (fib_iterator->hasNextHops())
        {
          // figure out the faceID of all the nexthops in the list, and send interest to ones that are local
          const fib::NextHopList& hopList = fib_iterator->getNextHops();
          for (nfd::fib::NextHopList::const_iterator hop_iterator = hopList.begin(); hop_iterator != hopList.end(); ++hop_iterator)
          {
            NFD_LOG_DEBUG("     CABEEEshortcutOPT, looking at all hops for this fib entry, hop_iterator: " << hop_iterator->getFace().getId());
          }
        }
    }
*/


    // look at FIB, and see if any UPSTREAM services are hosted on a local face (upstream only, since we received a pruned dag, so upstream is all we know about).
    // If so, send interestOPT out through that face.
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



//std::string prunedDagString;
//prunedDagString = this->PruneDagWorkflow(serviceString, dagString);
//json prunedDagObject = json::parse(prunedDagString);
//std::cout << "CABEEEshortcutOPT, prunedDagString is: " << prunedDagString << std::endl;


      // only generate shorcutOPT interest if the incoming interest is for /nescoSCOPT, and this fib entry is not for the service the interest is for (in which case the interest is forwarded to the service normally later on), and the service we'd be generating an interest for is upstream in the pruned DAG we received. Hosted services from other branches are not dealt with in shortcutOPT.
      //if (entryString == "/nescoSCOPT" && serviceString != dagObject["head"])
      //if (entryString == "/nescoSCOPT" && serviceString != dagObject["head"] && prunedDagObject["dag"].contains(serviceString))
      if (entryString == "/nescoSCOPT" && serviceString != dagObject["head"] && dagObject["dag"].contains(serviceString))
      {
//auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
//NFD_LOG_DEBUG("NodeID is " << (*node).GetId());
//std::cout << (*node).GetId() << " <--nodeiD : CABEEEshortcutOPT, we have /nescoSCOPT and serviceString " << serviceString << " is not the same as dag head " << dagObject["head"] << std::endl;
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
            //NFD_LOG_DEBUG("CABEEEshortcutOPT, looking at all hops for this fib entry, hop_iterator: " << hop_iterator->getFace().getId());
            //Face thisFace = hop_iterator->getFace();
            //if (thisFace.getScope() != ndn::nfd::FACE_SCOPE_NON_LOCAL)
            //{
              //thisFace.sendInterest(interestOPT);
            //}
            if (hop_iterator->getFace().getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
            {
              //interestOPT->setName(fib_iterator->getPrefix()); // give it the hosted service name, instead of /nescoSCOPT/shortcutOPT
              ndn::Name scoptFullName;
              scoptFullName = "/nescoSCOPT/shortcutOPT" + fib_iterator->getPrefix().getSubName(1,1).toUri();
              interestOPT->setName(scoptFullName); // add the hosted service name to the full name: /nescoSCOPT/shortcutOPT/<serviceName>
              NFD_LOG_DEBUG("CABEEEshortcutOPT, generating interest " << interestOPT->getName().toUri() << ", for local face with faceID: " << hop_iterator->getFace().getId());
//std::cout << "CABEEEshortcutOPT, generating interest " << interestOPT->getName().toUri() << ", for local face with faceID: " << hop_iterator->getFace().getId() << std::endl;


              //TODO: rather than just sending the interest out, rank it and add it to a queue.
              hop_iterator->getFace().sendInterest(*interestOPT);
            }
          }
        }
      }
    }

    // TODO: pick a threshold for how many shortcutOPT interests we are willing to send out
    // TODO: iterate through queue, and send out the best ranked ones
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
//NFD_LOG_INFO("onIncomingData in=" << ingress << " data=" << data.getName());


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



    bool updatedEFTmessage = false;
    NFD_LOG_DEBUG("NFDServiceDiscovery received data with name: " << rxedDataNameAndHash << " - faceID is: " << ingress.face.getId());
//NFD_LOG_INFO("NFDServiceDiscovery received data with name: " << rxedDataNameAndHash << " - faceID is: " << ingress.face.getId());
    // Upon receiving an SD data packet through a particular face, 
    // mark it as received through this face.
    if (m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["dataRx"] != 0)
    {
      NFD_LOG_WARN("NFDServiceDiscovery, ERROR??? Should this happen? We received a data packet through this face again: " << rxedDataNameAndHash << ", for face with faceID: " << ingress.face.getId());
      // since this is an updated EFT message, record it and check later to update the data and perform schedule compaction, etc
      // TODO: only update the data structure and run schedule compaction IF the new EFT is lower than the currently stored one. Note that this update coming in may occur before all inputs are ready.
      // TODO: in order to do this, we need to first calculate the new EFT (which is done below)!
      updatedEFTmessage = true;
    }






    m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["dataRx"] = 1;


    // Then the node NFD will need to calculate the new EFT for that face.
      //It will use the data RX time to calculate the link delay upstream, and add that to the EFT for that face.


    //NFD_LOG_DEBUG("Now reading it into string...");

    std::string dataPacketString;
    dataPacketString = (const char *)data.getContent().value();
    //NFD_LOG_DEBUG("Data string received: " << dataPacketString);


    //NFD_LOG_DEBUG("Now parsing it into JSON...");

    json dataPacketContents = json::parse(dataPacketString);
    //NFD_LOG_DEBUG("Data received: " << dataPacketContents);

    int64_t serviceLatency = -1;
    if (ingress.face.getScope() == ndn::nfd::FACE_SCOPE_LOCAL) // if data is coming from local face, it will have the serviceLatency reported by the serviceDiscovery application.
    {
      NFD_LOG_DEBUG("Data received - EFT: " << dataPacketContents["EFT"] << ", txTime: " << dataPacketContents["txTime"] << ", serviceLatency: " << dataPacketContents["serviceLatency"]);
//NFD_LOG_INFO("Data received - EFT: " << dataPacketContents["EFT"] << ", txTime: " << dataPacketContents["txTime"] << ", serviceLatency: " << dataPacketContents["serviceLatency"]);
      serviceLatency = dataPacketContents["serviceLatency"];
      m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["serviceLatency"] = serviceLatency;
    }
    else
    {
      NFD_LOG_DEBUG("Data received - EFT: " << dataPacketContents["EFT"] << ", txTime: " << dataPacketContents["txTime"]);
//NFD_LOG_INFO("Data received - EFT: " << dataPacketContents["EFT"] << ", txTime: " << dataPacketContents["txTime"]);
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

    if (updatedEFTmessage == false)
    {
      m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["linkDelay"] = linkDelayNS;
      m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["EFT"] = eftNS;
    }
    // TODO: only update the data structure and run schedule compaction IF the new EFT is lower than the currently stored one. Note that this update coming in may occur before all inputs are ready.
    if (updatedEFTmessage == true)
    {
      if (eftNS < m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["EFT"])
      {
//NFD_LOG_INFO("This is an updated EFT message with an EFT that is lower than the currently stored EFT! OldEFT = " << m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["EFT"] << ", newEFT = " << newEFT.ToInteger(ns3::Time::NS) << "ns");
        m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["linkDelay"] = linkDelayNS;
        m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["EFT"] = eftNS;
      }
      else
      {
//NFD_LOG_INFO("This is an updated EFT message with an EFT that is NOT lower than the currently stored EFT! OldEFT = " << m_SDservTracker[rxedDataNameAndHash]["faceOUT"][std::to_string(ingress.face.getId())]["EFT"] << ", newEFT = " << newEFT.ToInteger(ns3::Time::NS) << "ns");
      }
    }



/*
    //NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Data): " << std::setw(2) << m_SDservTracker << '\n');
auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
if ( (*node).GetId() == 1 && rxedDataNameAndHash == "/service2/params-sha256=d2fda8c1d3419f687ac35b918e6a98f3d48afdb723b75bb491193a3d1c9571ef")
{
NFD_LOG_INFO("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Data): " << std::setw(2) << m_SDservTracker << '\n');
}
*/

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
//NFD_LOG_INFO("NFDServiceDiscovery, all data packets for " << rxedDataNameAndHash << " have been received on all faces!!! Calculating best EFT and generating data packet downstream");
      // The new data packet going back downstream will contain: 
      // "Pruned DAG (pDAG) Service name" that it is being hosted and requested (serviceS/PWFH).
      // Calculate EFT (earliest finish time) and include it (lowest EFT of all the faces).
      // Timestamp of when data packet leaves (to measure delay to downstream nodes).

      //NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Data after allRxed): " << std::setw(2) << m_SDservTracker << '\n');

      // We first check if an allocation for this service has already been made. This would happen if after schedule compaction, an updated EFT is reported.
      if (m_SDservTracker[rxedDataNameAndHash]["faceIN"].contains("serviceScheduling"))
      {
        NFD_LOG_DEBUG("NFDServiceDiscovery, all data packets for " << rxedDataNameAndHash << " had already been received, but we received a new one, so it must be an EFT recalculation after schedule compaction.");
//NFD_LOG_INFO("NFDServiceDiscovery, all data packets for " << rxedDataNameAndHash << " had already been received, but we received a new one, so it must be an EFT recalculation after schedule compaction.");
        // can we simply delete the allocation, and let the code below look for a new (and potentially better) spot? - No, handle it separately.
        //m_SDservTracker[rxedDataNameAndHash]["faceIN"].erase("serviceScheduling");
      }


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
//NFD_LOG_INFO("NFDServiceDiscovery, EFT for face " << faceIterator.key() << ": " << thisEFT);
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

      int64_t allInputsReceivedEFT = lowestEFT;
      int64_t WFinterestRxedTime = m_SDservTracker[rxedDataNameAndHash]["faceIN"]["WFinterestRxedTime"];
      NFD_LOG_DEBUG("NFDServiceDiscovery, WF interest for " << rxedDataNameAndHash << " will be received at time " << WFinterestRxedTime);
      NFD_LOG_DEBUG("NFDServiceDiscovery, lowestEFT of all inputs is " << allInputsReceivedEFT << " on face " << lowestFace << ", lowestNonLocalEFT is " << lowestNonLocalEFT << " on face " << lowestNonLocalFace << ". Now determining CPU scheduling...");
//NFD_LOG_INFO("NFDServiceDiscovery, lowestEFT of all inputs is " << allInputsReceivedEFT << " on face " << lowestFace << ", lowestNonLocalEFT is " << lowestNonLocalEFT << " on face " << lowestNonLocalFace << ". Now determining CPU scheduling...");

      

      if (updatedEFTmessage == true)
      {
        // if serviceScheduling exists in this entry then this is an updateEFT message.
        // dont' bother looking for a new slot or anything like that, simply update the "inputsReadyTime" value in the data structure and then run the schedule compaction code
        // TODO: only update the data structure and run schedule compaction IF the new EFT is lower than the currently stored one.
        NFD_LOG_DEBUG("NFDServiceDiscovery, checking if serviceScheduling allocation exists to then call schedule compaction since this was an EFT update message for " << rxedDataNameAndHash);
//NFD_LOG_INFO("NFDServiceDiscovery, checking if serviceScheduling allocation exists to then call schedule compaction since this was an EFT update message for " << rxedDataNameAndHash);
        //NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure for just the service that has been updated: " << '\n' << rxedDataNameAndHash << '\n' << std::setw(2) << m_SDservTracker[rxedDataNameAndHash] << '\n');
        if (m_SDservTracker[rxedDataNameAndHash]["faceIN"].contains("serviceScheduling"))
        {
          m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["inputsReadyTime"] = allInputsReceivedEFT;
          //m_SDservTracker[rxedDataNameAndHash]["faceIN"].erase("serviceScheduling");
          if (m_SDservTracker[rxedDataNameAndHash]["faceIN"]["scheduleCompaction"] == 1)  // if we have it set up for doing schedule compaction
          {
            NFD_LOG_DEBUG("NFDServiceDiscovery, calling schedule compaction since this was an EFT update message for " << rxedDataNameAndHash);
//NFD_LOG_INFO("NFDServiceDiscovery, calling schedule compaction since this was an EFT update message for " << rxedDataNameAndHash);
            this->scheduleCompaction();
          }
        }
        else // if serviceScheduling entry doesn't exist, then it is not locally hosted, and we must forward the request further downstream - no need for scheduleCompaction here.
        {
          this->sendEFTdataUpdate(rxedDataNameAndHash, allInputsReceivedEFT);
        }
        return;
      }


      // create name&pDAG just like it will be created by the regular consumer when the real workflow runs. The application parameters are different (less of them), so the hash will be different.
      ndn::Name futureWFnameAndHash;
      futureWFnameAndHash = (data.getName()).getPrefix(-1); // remove the last component of the name (the parameter digest) so we have just the raw name
      futureWFnameAndHash = futureWFnameAndHash.getSubName(2,1); // remove the zeroeth component of the name (/nesco), and the first component of the name (/serviceDiscovery). starting at component 2, keep 1 component
      std::string futureWFnameAndHashString = "/nesco" + futureWFnameAndHash.toUri();

      json dagObject;
      dagObject["dag"]  = m_SDservTracker[rxedDataNameAndHash]["faceIN"]["dag"];
      dagObject["head"] = m_SDservTracker[rxedDataNameAndHash]["faceIN"]["head"];
      std::string updatedDagString = dagObject.dump();
      // in order to convert from std::string to a char[] datatype we do the following (https://stackoverflow.com/questions/7352099/stdstring-to-char):
      char *dagStringParameter = new char[updatedDagString.length() + 1];
      strcpy(dagStringParameter, updatedDagString.c_str());
      size_t lengthParam = strlen(dagStringParameter);

      shared_ptr<Interest> dummyInterest = make_shared<Interest>();
      dummyInterest->setName(futureWFnameAndHashString);
      dummyInterest->setApplicationParameters((const uint8_t *)dagStringParameter, lengthParam);
      futureWFnameAndHash = dummyInterest->getName();
      futureWFnameAndHashString = futureWFnameAndHash.toUri();





      if (m_SDservTracker[rxedDataNameAndHash]["faceIN"]["resourceAllocation"] == 0)
      {
        // Here we take into account the time taken to run the service, and add it to the EFT.
        // We just want to add the service latency to the EFT, assuming the service can start running right away.
        // This is simpler than below, where CPU allocation takes place (finding a gap to run the service). In that case, only 1 service can run at a time.

        // if the lowestEFT calculated above is from a local face, then we must calculate what the new EFT would be after scheduling the service in this node.
        Face* lowestCostFace;
        for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it)
        {
          lowestCostFace = &*it;
          //NFD_LOG_DEBUG("NFDServiceDiscovery, evaluating face " << std::to_string(lowestCostFace->getId()) );
          if (std::to_string(lowestCostFace->getId()) == lowestFace)
          {
            //NFD_LOG_DEBUG("NFDServiceDiscovery, lowestCostFace found: " << lowestFace);
            break;
          }
        }
        if (lowestCostFace->getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
        {
          // recall serviceLatency from datastructure (we saved it when the data packet from the local face came in - not necessarily the latest received data packet, which is why we need to grab the stored value from the data structure)
          serviceLatency = m_SDservTracker[rxedDataNameAndHash]["faceOUT"][lowestFace]["serviceLatency"];
          lowestEFT += serviceLatency;
        }
      }



      if (m_SDservTracker[rxedDataNameAndHash]["faceIN"]["resourceAllocation"] == 1)
      {

        // DETERMINE CPU SCHEDULING - ALLOCATION

        // if the lowestEFT calculated above is from a local face, then we must calculate what the new EFT would be after scheduling the service in this node.
        Face* lowestCostFace;
        for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it)
        {
          lowestCostFace = &*it;
          //NFD_LOG_DEBUG("NFDServiceDiscovery, evaluating face " << std::to_string(lowestCostFace->getId()) );
          if (std::to_string(lowestCostFace->getId()) == lowestFace)
          {
            //NFD_LOG_DEBUG("NFDServiceDiscovery, lowestCostFace found: " << lowestFace);
            break;
          }
        }

        NFD_LOG_DEBUG("NFDServiceDiscovery, lowestCostFace: " << lowestCostFace->getId());
//NFD_LOG_INFO("NFDServiceDiscovery, lowestCostFace: " << lowestCostFace->getId());

        if (lowestCostFace->getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
        {
          NFD_LOG_DEBUG("NFDServiceDiscovery, lowestCostFace is local.");
//NFD_LOG_INFO("NFDServiceDiscovery, lowestCostFace is local.");



          // recall serviceLatency from datastructure (we saved it when the data packet from the local face came in - not necessarily the latest received data packet, which is why we need to grab the stored value from the data structure)
          serviceLatency = m_SDservTracker[rxedDataNameAndHash]["faceOUT"][lowestFace]["serviceLatency"];



//NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (before looking at allocation reuse): " << std::setw(2) << m_SDservTracker << '\n');


          // Check if the WFname&hash exists in the currently allocated services and that it can be reused.
          int64_t previousAllocationEFT = -1; // default value before we start analyzing EFTs.
          int64_t previousAllocationStart = -1; // default value before we start analyzing EFTs.
          std::string previousAllocationFace;
          if (m_SDservTracker[rxedDataNameAndHash]["faceIN"]["allocationReuse"] == 1)  // if we have it set up for doing allocation reuse of previously allocated slots
          {
            for (auto& serviceIterator : m_SDservTracker.items())
            {
              if (m_SDservTracker[serviceIterator.key()]["faceIN"].contains("serviceScheduling"))
              {
                if (m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["WFnameAndHash"] == futureWFnameAndHashString)
                {
                  // If this node doesn't use caching and the previously allocated service finishes later than when the WF interest for this latest iteration of the service would arrive, then we reuse the already allocated version.
                  // An extra PIT entry will be added and will be able to be satisfied once the already allocated service runs.
                  // If this node uses caching (content store), then we just look for the best previously allocated slot (any end time is OK to reuse)
                  // m_cs.size() gives us the number of currently cached data packets. m_cs.getLimit() give us the cache size.
                  //if (m_cs.getLimit() > 0 || m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["end"] > WFinterestRxedTime)
                  if (m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["start"] > allInputsReceivedEFT) // only reuse a slot if it starts after all inputs for this service have been received. This way, if the previous allocation is removed, we can truly still use this one.
                  {
                    NFD_LOG_DEBUG("NFDServiceDiscovery scheduling with no caching - Found a way to potentially reuse allocation for " << futureWFnameAndHashString << ", which was scheduled to finish at: " << m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["end"] << ", with serviceLatency = " << serviceLatency);
//NFD_LOG_INFO("NFDServiceDiscovery scheduling with no caching - Found a way to potentially reuse allocation for " << futureWFnameAndHashString << ", which was scheduled to finish at: " << m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["end"] << ", with serviceLatency = " << serviceLatency);
                    if (previousAllocationEFT == -1 || previousAllocationEFT > m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["end"]) // now we look for the lowest EFT of the previously allocated slots (look for the best one).
                    {
                      previousAllocationEFT = m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["end"];
                      previousAllocationStart = m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["start"];
                      previousAllocationFace = m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["face"];
                    }
                  }
                }
              }
            }
          }


          // Next, we will compare using the already allocated slot that matched above vs scheduling from scratch
          // so we determine what the new EFT would be after scheduling (could be seriously delayed more if node is very busy)
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
                  m_SDservTracker[serviceIterator.key()]["faceIN"]["serviceScheduling"]["inputsReadyTime"],
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
  /*
          NFD_LOG_DEBUG("NFDServiceDiscovery, sorted scheduled items: ");
          for (size_t i = 0; i < scheduledVector.size(); i++)
          {
            NFD_LOG_DEBUG("  item " << i << " name: " << scheduledVector[i].name << ", start: " << scheduledVector[i].start << ", end: " << scheduledVector[i].end);
          }
  */


          bool spotFound = false;
          // 0. check if no other service has been scheduled yet
          if (scheduledVector.empty())
          {
            earliestStartPossible = lowestEFT;
            earliestEndPossible = lowestEFT + serviceLatency;
            spotFound = true;
            NFD_LOG_DEBUG("NFDServiceDiscovery scheduling - vector was empty (no existing scheduled services). Can insert at beginning. earliestStartPossible: " << earliestStartPossible << ", + serviceLatency: " << serviceLatency << " = earliestEndPossible: " << earliestEndPossible);
          }

          // 1. check before first existing service
          else if (lowestEFT + serviceLatency <= scheduledVector[0].start)
          {
            earliestStartPossible = lowestEFT;
            earliestEndPossible = lowestEFT + serviceLatency;
            spotFound = true;
            NFD_LOG_DEBUG("NFDServiceDiscovery scheduling - Can insert before first existing service. earliestStartPossible: " << earliestStartPossible << ", earliestEndPossible: " << earliestEndPossible);
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
                  NFD_LOG_DEBUG("NFDServiceDiscovery scheduling - Can insert between existing services. earliestStartPossible: " << earliestStartPossible << ", earliestEndPossible: " << earliestEndPossible);
                  break;
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
            NFD_LOG_DEBUG("NFDServiceDiscovery scheduling - Can insert after last existing service. earliestStartPossible: " << earliestStartPossible << ", earliestEndPossible: " << earliestEndPossible);
          }


          NFD_LOG_DEBUG("NFDServiceDiscovery re-evaluating lowest EFT after potential scheduling - earliestEndPossible: " << earliestEndPossible << ", lowestNonLocalEFT: " << lowestNonLocalEFT);
//NFD_LOG_INFO("NFDServiceDiscovery re-evaluating lowest EFT after potential scheduling - earliestEndPossible: " << earliestEndPossible << ", lowestNonLocalEFT: " << lowestNonLocalEFT);

          NFD_LOG_DEBUG("NFDServiceDiscovery - previousAllocationEFT: " << previousAllocationEFT);
//NFD_LOG_INFO("NFDServiceDiscovery - previousAllocationEFT: " << previousAllocationEFT);




          // There are THREE options. 1) Use previous allocation. 2) Use new allocation locally. 3) Use non-local Face.
          // Pick the earliest EFT of the three.
          // If using a previous allocation, we create a replicated allocation slot again so that any schedulerRelease messages will only release a single slot without breaking dependencies for the services that were reusing that slot.
          // If using a previous allocation, there is no need to add a new FIB entry.
          if (previousAllocationEFT != -1 && previousAllocationEFT <= earliestEndPossible)
          {
            NFD_LOG_DEBUG("NFDServiceDiscovery - reusing an allocation spot from a previous scheduled service instance.");
            lowestEFT = previousAllocationEFT;

            // creating a copy of the allocated slot (as done below) prevents reused allocations from disappearing when a schedulerRelease message is received for a different version of the allocated slot. A slot is truly only release if no other versions of a service require it.
            if (m_SDservTracker[rxedDataNameAndHash]["faceIN"].contains("serviceScheduling"))
            {
              NFD_LOG_ERROR("NFD SD Forwarding ERROR!! This service has already been scheduled!!!!");
            }
            m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["WFnameAndHash"] = futureWFnameAndHashString;
            m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["inputsReadyTime"] = allInputsReceivedEFT;
            m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["start"] = previousAllocationStart;
            m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["end"] = previousAllocationEFT;
            m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["face"] = previousAllocationFace;
            // print info level message with node id, start and stop time, so process script can see it
            auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
            NFD_LOG_INFO("NFDServiceDiscovery - SDresourceAllocation: Service " << rxedDataNameAndHash << " scheduled on node " << (*node).GetId() << " starting at " << m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["start"] << " and ending at " << m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["end"] << " nanoseconds). This is an allocation reuse.");
            //lowestFace = previousAllocationLowestFace; // if we are reusing a previous allocation, its lowestFace will be the local face, which won't require a FIB entry to be created.
//NFD_LOG_INFO("\n\nNFDServiceDiscovery - m_SDservTracker data structure for just this service (on Data after scheduled): " << '\n' << rxedDataNameAndHash << '\n' << std::setw(2) << m_SDservTracker[rxedDataNameAndHash] << '\n');
          }

          // Then re-evaluate if running locally is still the lowest EFT (or if it's our only choice - in which case lowestNonLocalEFT would still be zero).
          else if ((earliestEndPossible < lowestNonLocalEFT) || (lowestNonLocalEFT == -1)) // if yes, then schedule it locally
          {
            if (m_SDservTracker[rxedDataNameAndHash]["faceIN"].contains("serviceScheduling"))
            {
              NFD_LOG_ERROR("NFD SD Forwarding ERROR!! This service has already been scheduled!!!!");
            }
            NFD_LOG_DEBUG("NFDServiceDiscovery - SCHEDULING TO RUN LOCALLY!!!");
            m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["WFnameAndHash"] = futureWFnameAndHashString;
            m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["inputsReadyTime"] = allInputsReceivedEFT;
            m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["start"] = earliestStartPossible;
            m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["end"] = earliestEndPossible;
            m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["face"] = lowestFace;
            lowestEFT = earliestEndPossible;
            //lowestFace = lowestFace; // if we are here, lowestFace will be the local face already.

            // print info level message with node id, start and stop time, so process script can see it
            auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
            NFD_LOG_INFO("NFDServiceDiscovery - SDresourceAllocation: Service " << rxedDataNameAndHash << " scheduled on node " << (*node).GetId() << " starting at " << m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["start"] << " and ending at " << m_SDservTracker[rxedDataNameAndHash]["faceIN"]["serviceScheduling"]["end"] << " nanoseconds).");

//NFD_LOG_INFO("\n\nNFDServiceDiscovery - m_SDservTracker data structure for just this service (on Data after scheduled): " << '\n' << rxedDataNameAndHash << '\n' << std::setw(2) << m_SDservTracker[rxedDataNameAndHash] << '\n');
            if (earliestStartPossible > allInputsReceivedEFT)
            {
              NFD_LOG_DEBUG("NFDServiceDiscovery - FYI: inputs are arriving before allocation slot!!");
            }

            // send schedulerRelease message to each face where the same service results may have come from - they are no longer needed to run elsewhere since the local EFT is lower.
            //sendSchedulerReleaseInterestUpstream(rxedDataNameAndHash, lowestFace);

          }
          else // Running locally is no longer the lowest EFT, so don't schedule the task locally and instead use the other face (lowestNonLocalFace)
          {
            // update lowestEFT and lowestFace variables to be the non-local one (with lowestNonLocalEFT)
            NFD_LOG_DEBUG("NFDServiceDiscovery - NOT SCHEDULING, RUNNING ELSEWHERE UPSTREAM!!!");
            lowestEFT = lowestNonLocalEFT;
            lowestFace = lowestNonLocalFace;
          }


//NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Data after scheduled): " << std::setw(2) << m_SDservTracker << '\n');




        }
        else
        {
          NFD_LOG_DEBUG("NFDServiceDiscovery - lowest cost face is not local. No need to schedule anything here.");
//NFD_LOG_INFO("NFDServiceDiscovery - lowest cost face is not local. No need to schedule anything here.");
        }


        // send schedulerRelease messages to each face (except the lowest cost face) where the same service results may have come from - they are no longer needed to run elsewhere other than the lowest cost face.
        sendSchedulerReleaseInterestUpstream(rxedDataNameAndHash, lowestFace);

        NFD_LOG_DEBUG("NFDServiceDiscovery - scheduling done");

      }



      NFD_LOG_DEBUG("NFDServiceDiscovery - generating FIB entry now...");

      // GENERATE NEW FIB ENTRY

      // create the FIB entry, so that when the workflow runs, we route through the face that has the lowest EFT.
      // The node will record the lowest EFT cost in the FIB by creating a new table entry using the workflow pDAG name (not the serviceDiscovery pDAG name).
      // The cost will be EFT in nano-seconds. This EFT is units of time after the initial interest is generated.
      //auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
      Face* lowestCostFace;
      for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it)
      {
        lowestCostFace = &*it;
        if (std::to_string(lowestCostFace->getId()) == lowestFace)
        {
          break;
        }
      }



/*
m_FibOwnerTracker = {
    "/service1/WFpDAG_param_hash": {                        // key has full WF name service/pDAG
        "fibEntryExists": 0/1,                              // if any of the faceIDs below claim a fibOwner, this value will be 1, otherwise 0. Represents the actual FIB entry existing or not.
        "/service1/faceInIdString1&pDAG_param_hash": {      // key has full SD name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
          "faceOUT": {
            "faceID3": {                                    // faceID where the interest has been forwarded to
                "EFT": 3,                                   // 3ms is the EFT upstream
                "fibOwner": 0                               // tells us if this is the entry that currently defined the FIB entry. Only one per /service1/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
            },
            "faceID4": {                                    // faceID where the interest has been forwarded to
                "EFT": 2,                                   // 2ms is the EFT upstream
                "fibOwner": 1                               // tells us if this is the entry that currently defined the FIB entry. Only one per /service1/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
            },
            "faceID5": {                                    // faceID where the interest has been forwarded to
                "EFT": -1                                   // default value is -1 (data packet not received yet)
                "fibOwner": 0                               // tells us if this is the entry that currently defined the FIB entry. Only one per /service1/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
            }
          }
        }
        "/service1/faceInIdString2&pDAG_param_hash": {      // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
          "faceOUT": {
            "faceID3": {                                    // faceID where the interest has been forwarded to
                "EFT": 6,                                   // this is the EFT upstream
                "fibOwner": 0                               // tells us if this is the entry that currently defined the FIB entry. Only one per /service1/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
            },
            "faceID5": {                                    // faceID where the interest has been forwarded to
                "EFT": -1                                   // default value is -1 (data packet not received yet)
                "fibOwner": 0                               // tells us if this is the entry that currently defined the FIB entry. Only one per /service1/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
            }
          }
        }
    }

m_FibOwnerTracker = {
    "/service1/WFpDAG_param_hash": {                        // key has full WF name service/pDAG
        "/service1/faceInIdString1&pDAG_param_hash": 0,     // key has full SD name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
                                                                // value tells us if this is the entry that currently defined the FIB entry. Only one per /serviceX/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
        "/service1/faceInIdString2&pDAG_param_hash": 1,     // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
                                                                // value tells us if this is the entry that currently defined the FIB entry. Only one per /serviceX/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
        "/service1/faceInIdString3&pDAG_param_hash": 0      // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
                                                                // value tells us if this is the entry that currently defined the FIB entry. Only one per /serviceX/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
    },
    "/service2/WFpDAG_param_hash": {                        // key has full name service/pDAG
        "/service2/faceInIdString1&pDAG_param_hash": 1,     // key has full SD name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
                                                                // value tells us if this is the entry that currently defined the FIB entry. Only one per /serviceX/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
        "/service2/faceInIdString2&pDAG_param_hash": 0      // key has full name service/pDAG with locally modified param hash that includes input faceID (added right when interest is received)
                                                                // value tells us if this is the entry that currently defined the FIB entry. Only one per /serviceX/WFpDAG_param_hash can be true at a time, and it will be the one with the lowest EFT.
    },
    "/service3/WFpDAG_param_hash": {                        // key has full name service/pDAG
        etc...
    }
}
*/

      // if it is a local face (to an application - to a locally hosted service), we don't create the FIB entry, and instead rely on the 0 cost regular FIB entry from the service itself.
        // this is because the recorded face with lowest EFT is for the serviceDiscovery service's face, not the actual workflow service's face. Each application gets its own local face.
      if (lowestCostFace->getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
      {
        NFD_LOG_DEBUG("NFDServiceDiscovery, lowestCostFace is local. Going to try to delete FIB entry if it has no owner. This is for WF name " << futureWFnameAndHashString);
        // if there is an existing FIB entry for this name&pDAG, remove it. We need to forward to this local face using regular FIB entry with just service name and cost 0.

        //TODO: we can't just delete the entry if it exists. We may have several "active" requests with unique paths. Only the current path that this data packet was for has finished being analyzed.
        //      Other paths may still be optimal, but since they all share the same futureWFnameAndHash for each face, removing this one would remove the other one(s) for that face too.
        //      Look into perhaps keeping a local custom fib where we can store the rxedDataNameAndHash too. If we no longer need the fib entry (cuz it's not optimal), we can then check and see if there
        //      are still other fib entries for this futureWFnameAndHash on that same face, and if there are, we don't remove the actual FIB entry. We only remove it once there are no more entries in the custom FIB for that particular face.

/*
 // PRINT OUT THE FIB ENTRIES FOR THIS NAME - for debugging
if (futureWFnameAndHashString == "/nesco/service1/params-sha256=b11a48b8384e652ea726efb193902553c97041a52670bb25b5f2c19bb15a8af3")
{
  for (fib::Fib::const_iterator fib_iterator = m_fib.begin(); fib_iterator != m_fib.end(); ++fib_iterator)
  {
    //NFD_LOG_DEBUG("CABEEEshortcutOPT, looking at fib entry\n");
    ndn::Name entryName;
    entryName = fib_iterator->getPrefix();
    entryName = entryName.getSubName(0,1); // starting at component 0, get 1 component (/nescoSCOPT only)
    std::string entryString = entryName.toUri();

    ndn::Name serviceName;
    serviceName = fib_iterator->getPrefix();
    serviceName = serviceName.getSubName(1,1); // starting at component 1, get 1 component (service name only)
    std::string serviceString = serviceName.toUri();

    if (entryString == "/nesco")
    {
      if (fib_iterator->hasNextHops())
      {
        // figure out the faceID of all the nexthops in the list, and print them
        const fib::NextHopList& hopList = fib_iterator->getNextHops();
        for (nfd::fib::NextHopList::const_iterator hop_iterator = hopList.begin(); hop_iterator != hopList.end(); ++hop_iterator)
        {
          NFD_LOG_INFO("CABEEEfibEntries: name " << fib_iterator->getPrefix().toUri() << ", faceID: " << hop_iterator->getFace().getId() << ", cost: " << hop_iterator->getCost());
        }
      }
    }
  }
}
*/

        // make value of this specific rxedDataNameAndHash = 0
        m_FibOwnerTracker[futureWFnameAndHashString][rxedDataNameAndHash] = 0;

        NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_FibOwnerTracker data structure: " << std::setw(2) << m_FibOwnerTracker << '\n');
        // check if any other values for this futureWFnameAndHash is still a 1. If none, then remove real FIB entry
        bool has_active_owner = false;
        if (m_FibOwnerTracker.contains(futureWFnameAndHashString))
        {
          for (auto& [service, value] : m_FibOwnerTracker[futureWFnameAndHashString].items())
          {
            if (value == 1) {
              has_active_owner = true;
              NFD_LOG_DEBUG("NFDServiceDiscovery, FIB entry has active owner (not deleting entry)");
              break; 
            }
          }
        }

        if (has_active_owner == false)
        {
          NFD_LOG_DEBUG("NFDServiceDiscovery, skipping creating FIB entry for " << futureWFnameAndHashString << " since it is on a local face (instead rely on the 0 cost regular FIB entry from the service itself).");
//NFD_LOG_INFO("NFDServiceDiscovery, skipping creating FIB entry for " << futureWFnameAndHashString << " since it is on a local face (instead rely on the 0 cost regular FIB entry from the service itself).");
          fib::Entry* exact = m_fib.findExactMatch(futureWFnameAndHash);
          if (exact != nullptr) {
            m_fib.erase(futureWFnameAndHash);
            NFD_LOG_DEBUG("NFDServiceDiscovery, removed FIB entry for " << futureWFnameAndHashString);
//NFD_LOG_INFO("NFDServiceDiscovery, entry for " << futureWFnameAndHashString << " removed from FIB. This is for rxedDataNameAndHash: " << rxedDataNameAndHash << "\n");
          }
        }
      }

      // otherwise, if it is a non-local face, we would be going out to another NFD node, and thus we create a new FIB entry with that non-local face.
      if (lowestCostFace->getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL)
      {
        NFD_LOG_DEBUG("NFDServiceDiscovery, lowestCostFace is NOT local. Going to try to add FIB entry.");
        fib::Entry* entry = m_fib.insert(futureWFnameAndHash).first;
        m_fib.addOrUpdateNextHop(*entry, *lowestCostFace, lowestEFT);
        // make value of this specific rxedDataNameAndHash = 1. Create if doesn't exist? Make all other entries = 0?
        m_FibOwnerTracker[futureWFnameAndHashString][rxedDataNameAndHash] = 1;
        NFD_LOG_DEBUG("NFDServiceDiscovery, addNextHopRecord entry for " << futureWFnameAndHashString << " added to FIB, with face " << lowestCostFace->getId() << ", and cost " << lowestEFT << ". This is for rxedDataNameAndHash: " << rxedDataNameAndHash << "\n");
//NFD_LOG_INFO("NFDServiceDiscovery, addNextHopRecord entry for " << futureWFnameAndHashString << " added to FIB, with face " << lowestCostFace->getId() << ", and cost " << lowestEFT << ". This is for rxedDataNameAndHash: " << rxedDataNameAndHash << "\n");
      }




      this->sendEFTdataUpdate(rxedDataNameAndHash, lowestEFT);




      // clear m_SDservTracker faceOUT entry FOR THIS SERVICE ONLY, so that if a new interest is received, we go looking for inputs again
      //NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - removing m_SDservTracker entry for " << rxedDataNameAndHash << '\n');
      //m_SDservTracker.erase(rxedDataNameAndHash); // erase the entire entry
      //m_SDservTracker[rxedDataNameAndHash].erase("faceOUT"); // only erase the "faceOUT" portion, otherwise we would be removing the CPU scheduling information!
      // we can't just erase the faceOUT portion, because I'm trying to use it for disseminating schedulerRelease messages too! So instead we just reset all the values.
      // Actually, we can just leave the values as they were. This way, after schedule compaction, an update message will trigger EFT adjustments even if only one input is updated.
/*
      for (auto& faceIterator : m_SDservTracker[rxedDataNameAndHash]["faceOUT"].items())
      {
        m_SDservTracker[rxedDataNameAndHash]["faceOUT"][faceIterator.key()]["intTx"] = 0;
        m_SDservTracker[rxedDataNameAndHash]["faceOUT"][faceIterator.key()]["dataRx"] = 0;
        m_SDservTracker[rxedDataNameAndHash]["faceOUT"][faceIterator.key()]["linkDelay"] = -1;
        m_SDservTracker[rxedDataNameAndHash]["faceOUT"][faceIterator.key()]["EFT"] = -1;
      }
*/

//NFD_LOG_DEBUG("\n\nNFDServiceDiscovery - m_SDservTracker data structure (on Data after sending downstream): " << std::setw(2) << m_SDservTracker << '\n');

    } // end if (allRxed)

  } // end if (name1String == "/serviceDiscovery")


  else // regular data packet processing
  {


    // CPU ALLOCATION CHECK
    if (data.getName().getPrefix(1).toUri() == "/nesco" ||
        data.getName().getPrefix(1).toUri() == "/nescoSCOPT" ||
        data.getName().getPrefix(1).toUri() == "/orchA" ||
        data.getName().getPrefix(1).toUri() == "/orchB")
    {
      if (ingress.face.getScope() == ndn::nfd::FACE_SCOPE_LOCAL) // only if data is coming from local face (if coming from local, it's from a service, and thus we need to report resource usage).
      {
        //NFD_LOG_DEBUG("Now reading it into string...");
        std::string dataPacketString;
        dataPacketString = (const char *)data.getContent().value();
        //NFD_LOG_DEBUG("Data string received: " << dataPacketString);

        //NFD_LOG_DEBUG("Now parsing it into JSON...");
        json dataPacketContents = json::parse(dataPacketString);
        //NFD_LOG_DEBUG("Data received: " << dataPacketContents);

        uint64_t makespanNS = 1000000;
        makespanNS = dataPacketContents["makespanNS"]; // we don't really do anything with the previous service's makespan here.
        Forwarder::allocateResource(name1String, data, ingress, makespanNS);

        return;

      }
    }





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
Forwarder::sendEFTdataUpdate(std::string nameAndHash, int64_t lowestEFT)
{
  // create data packet, but use stored name/hash!
  std::string storedName = m_SDservTracker[nameAndHash]["faceIN"]["inName"];
  auto new_data = std::make_shared<ndn::Data>(storedName);
  //new_data->setFreshnessPeriod(data.getFreshnessPeriod());
  new_data->setFreshnessPeriod(ndn::time::milliseconds(3000));

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
    NFD_LOG_ERROR("NFD SD Forwarding ERROR!! The data packet size is larger than 1024!!!");
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
  Face* downFace = m_faceTable.get(m_SDservTracker[nameAndHash]["faceIN"]["inID"]);
  NFD_LOG_DEBUG("NFDServiceDiscovery, EFTdataUpdate packet for " << nameAndHash << " is being sent downstream as " << storedName << " through face " << m_SDservTracker[nameAndHash]["faceIN"]["inID"]);
//NFD_LOG_INFO("NFDServiceDiscovery, EFTdataUpdate packet for " << nameAndHash << " is being sent downstream as " << storedName << " through face " << m_SDservTracker[nameAndHash]["faceIN"]["inID"]);

  this->onOutgoingData(*new_data, *downFace);
}




void
Forwarder::onIncomingDataAfterServiceRuns(const Data& data, const FaceEndpoint& ingress)
{
  NFD_LOG_DEBUG("onIncomingDataAfterServiceRuns in=" << ingress << " data=" << data.getName());

  // receive Data
  data.setTag(make_shared<lp::IncomingFaceIdTag>(ingress.face.getId()));
  ++m_counters.nInData;

  // /localhost scope control
  bool isViolatingLocalhost = ingress.face.getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL &&
                              scope_prefix::LOCALHOST.isPrefixOf(data.getName());
  if (isViolatingLocalhost) {
    NFD_LOG_DEBUG("onIncomingDataAfterServiceRuns in=" << ingress << " data=" << data.getName() << " violates /localhost");
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
    NFD_LOG_DEBUG("onIncomingDataAfterServiceRuns matching=" << pitEntry->getName());

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





void
Forwarder::allocateResource(const std::string& serviceName, const Data& data, const FaceEndpoint& ingress, uint64_t makespanNS)
{
  auto dataPtr = std::make_shared<Data>(data);
  auto ingressPtr = std::make_shared<FaceEndpoint>(ingress);

  ns3::Time timeNow = ns3::Simulator::Now();
  int64_t timeNowNS = timeNow.ToInteger(ns3::Time::NS); // Convert to integer
  auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());

  if (!m_resourceBusy)
  //if (m_resourceMutex.try_lock())
  {
    // mutex successfully locked here
    m_resourceBusy = true; // Acquire resource
    NFD_LOG_INFO("NFDServiceDiscovery - WFresourceAllocation: Service " << serviceName << " started running on node " << (*node).GetId() << ". Setting resourceBusy = true (resource locked at " << timeNowNS << " nanoseconds).");
    // Schedule release
    //ns3::Simulator::Schedule(ns3::MilliSeconds(1), &Forwarder::freeResource, this, serviceName, data, ingress);
    ns3::Simulator::Schedule(ns3::NanoSeconds(makespanNS), &Forwarder::freeResource, this, serviceName, *dataPtr, *ingressPtr);
  }
  else
  {
    // Resource is busy, retry later
    NFD_LOG_DEBUG("NFDServiceDiscovery - WFresourceAllocation: Service " << serviceName << " needs to start running on node " << (*node).GetId() << " but the node is busy running another service. Waiting 0.1ms and trying again. Current time: " << timeNowNS << " nanoseconds).");
    //ns3::Simulator::Schedule(ns3::MilliSeconds(0.1), &Forwarder::allocateResource, this, serviceName, data, ingress);
    ns3::Simulator::Schedule(ns3::MicroSeconds(100), &Forwarder::allocateResource, this, serviceName, *dataPtr, *ingressPtr, makespanNS);
    return;
  }
}

void
Forwarder::freeResource(const std::string& serviceName, const Data& data, const FaceEndpoint& ingress)
{
  auto dataPtr = std::make_shared<Data>(data);
  auto ingressPtr = std::make_shared<FaceEndpoint>(ingress);

  m_resourceBusy = false;
  //m_resourceMutex.unlock();

  ns3::Time timeNow = ns3::Simulator::Now();
  int64_t timeNowNS = timeNow.ToInteger(ns3::Time::NS); // Convert to integer
  auto node = ::ns3::NodeList::GetNode(::ns3::Simulator::GetContext());
  NFD_LOG_INFO("NFDServiceDiscovery - WFresourceAllocation: Service " << serviceName << " finished running on node " << (*node).GetId() << ". Setting resourceBusy = false (resource unlocked at " << timeNowNS << " nanoseconds).");
  //NFD_LOG_DEBUG("NFDServiceDiscovery - WFresourceAllocation: Service finished running. Setting resourceBusy = false.");
  //Forwarder::onIncomingDataAfterServiceRuns(data, ingress); // finish processing the incoming data packet.
  Forwarder::onIncomingDataAfterServiceRuns(*dataPtr, *ingressPtr); // finish processing the incoming data packet.
}



void
Forwarder::sendSchedulerReleaseInterestUpstream(const std::string nameAndHash, const std::string lowestFace)
{
  // Loop through all faceOUTs (local and non-local), and send schedulerRelease message
  // to each face only if it is not the lowestCostFace

  shared_ptr<Interest> interestSchedulerRelease = make_shared<Interest>();
  interestSchedulerRelease->setName("/nesco/schedulerRelease");
  std::string appParamString = "/nesco/serviceDiscovery" + nameAndHash;
  //std::cout << "appParamString: " << appParamString << std::endl;
  // in order to convert from std::string to a char[] datatype we do the following (https://stackoverflow.com/questions/7352099/stdstring-to-char):
  char *newAppParamString = new char[appParamString.length() + 1];
  strcpy(newAppParamString, appParamString.c_str());
  size_t length = strlen(newAppParamString);
  interestSchedulerRelease->setApplicationParameters((const uint8_t *)newAppParamString, length);

  bool done = false;
  for (FaceTable::const_iterator it = m_faceTable.begin(); it != m_faceTable.end(); ++it)
  {
    Face* thisFace = &*it;
    for (auto& faceIterator : m_SDservTracker[nameAndHash]["faceOUT"].items())
    {
      if (std::to_string(thisFace->getId()) == faceIterator.key())
      {
        //if (thisFace->getScope() == ndn::nfd::FACE_SCOPE_NON_LOCAL && faceIterator.key() != lowestFace)
        if (faceIterator.key() != lowestFace) // send out to all faces EXCEPT to the lowest cost face, which is where the service is meant to run.
        {
          NFD_LOG_DEBUG("NFDServiceDiscovery - sending schedulerRelease message for " << nameAndHash << " upstream through face " << thisFace->getId() << std::endl);
          thisFace->sendInterest(*interestSchedulerRelease);
          //m_SDservTracker[nameAndHash]["faceOUT"].erase(faceIterator.key()); // only erase the entry for the "faceOUT" we are sending the message to.
          //done = true;
          //break;
        }
      }
    }
    //if (done == true)
    //{
      //break;
    //}
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
    if (data.getName().getPrefix(-1).getSubName(1,1).toUri() == "/serviceDiscovery" ||
        data.getName().getPrefix(-1).getSubName(1,1).toUri() == "/schedulerRelease")
    {
      if (egress.getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
      {
        NFD_LOG_INFO("     CABEEE: onOutgoingSDDataToApp (to the consuming application only) =" << " name=" << data.getName());
      }
      else
      {
        NFD_LOG_INFO("     CABEEE: onOutgoingSDDataToFace (to another NFD node on a physical face) =" << " name=" << data.getName());
      }
    }
    else
    {
      if (egress.getScope() == ndn::nfd::FACE_SCOPE_LOCAL)
      {
        NFD_LOG_INFO("     CABEEE: onOutgoingWFDataToApp (to the consuming application only) =" << " name=" << data.getName());
      }
      else
      {
        NFD_LOG_INFO("     CABEEE: onOutgoingWFDataToFace (to another NFD node on a physical face) =" << " name=" << data.getName());
      }
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
