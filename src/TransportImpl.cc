/* Copyright (c) 2018-2020, Stanford University
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TransportImpl.h"
#include <algorithm>
#include "Cycles.h"
#include "Perf.h"
#include "Protocol.h"

namespace Homa {
namespace Core {

// Basic timeout unit.
const uint64_t BASE_TIMEOUT_US = 2000;
/// Microseconds to wait before timeout out and failing a message.
const uint64_t MESSAGE_TIMEOUT_US = 40 * BASE_TIMEOUT_US;
/// Microseconds to wait before pinging to check on outbound messages.
const uint64_t PING_INTERVAL_US = 3 * BASE_TIMEOUT_US;
/// Microseconds to wait before performing retires on inbound messages.
const uint64_t RESEND_INTERVAL_US = BASE_TIMEOUT_US;

/**
 * Construct an instances of a Homa-based transport.
 *
 * @param driver
 *      Driver with which this transport should send and receive packets.
 * @param transportId
 *      This transport's unique identifier in the group of transports among
 *      which this transport will communicate.
 */
TransportImpl::TransportImpl(Driver* driver, uint64_t transportId)
    : transportId(transportId)
    , driver(driver)
    , policyManager(new Policy::Manager(driver))
    , sender(new Sender(transportId, driver, policyManager.get(),
                        PerfUtils::Cycles::fromMicroseconds(MESSAGE_TIMEOUT_US),
                        PerfUtils::Cycles::fromMicroseconds(PING_INTERVAL_US)))
    , receiver(
          new Receiver(driver, policyManager.get(),
                       PerfUtils::Cycles::fromMicroseconds(MESSAGE_TIMEOUT_US),
                       PerfUtils::Cycles::fromMicroseconds(RESEND_INTERVAL_US)))
    , nextTimeoutCycles(0)
{}

/**
 * TransportImpl Destructor.
 */
TransportImpl::~TransportImpl() = default;

/// See Homa::Transport::poll()
void
TransportImpl::poll()
{
    Perf::Timer timer;

    // Receive and dispatch incoming packets.
    processPackets();

    // Allow sender and receiver to make incremental progress.
    sender->poll();
    receiver->poll();

    Perf::counters.total_cycles.add(timer.split());
}

/**
 * Helper method which receives a burst of incoming packets and process them
 * through the transport protocol.  Pulled out of TransportImpl::poll() to
 * simplify unit testing.
 */
void
TransportImpl::processPackets()
{
    // Keep track of time spent doing active processing versus idle.
    Perf::Timer timer;

    const int MAX_BURST = 32;
    Driver::Packet* packets[MAX_BURST];
    IpAddress srcAddrs[MAX_BURST];
    int numPackets = driver->receivePackets(MAX_BURST, packets, srcAddrs);
    int releaseCount = 0;

    uint64_t now = PerfUtils::Cycles::rdtsc();
    for (int i = 0; i < numPackets; ++i) {
        bool retainPacket = processPacket(packets[i], srcAddrs[i], now);
        if (!retainPacket) {
            packets[releaseCount++] = packets[i];
        }
    }
    driver->releasePackets(packets, releaseCount);

    if (numPackets > 0) {
        Perf::counters.active_cycles.add(timer.split());
    }
}

/**
 * Process an incoming packet.
 *
 * @param packet
 *      Incoming packet to be processed.
 * @param sourceIp
 *      Source IP address.
 * @param now
 *      The rdtsc cycle that should be considered the "current" time.
 * @return
 *      True if the transport decides to take ownership of the packet. False
 *      if the transport has no more use of this packet and it can be released
 *      to the driver.
 */
bool
TransportImpl::processPacket(Driver::Packet* packet, IpAddress sourceIp,
                             uint64_t now)
{
    assert(packet->length >=
           Util::downCast<int>(sizeof(Protocol::Packet::CommonHeader)));
    Perf::counters.rx_bytes.add(packet->length);
    Protocol::Packet::CommonHeader* header =
        static_cast<Protocol::Packet::CommonHeader*>(packet->payload);
    bool retainPacket = false;
    switch (header->opcode) {
        case Protocol::Packet::DATA:
            Perf::counters.rx_data_pkts.add(1);
            retainPacket = receiver->handleDataPacket(packet, now, sourceIp);
            break;
        case Protocol::Packet::GRANT:
            Perf::counters.rx_grant_pkts.add(1);
            sender->handleGrantPacket(packet, now);
            break;
        case Protocol::Packet::ACK:
            Perf::counters.rx_ack_pkts.add(1);
            sender->handleAckPacket(packet);
            break;
        case Protocol::Packet::RESEND:
            Perf::counters.rx_resend_pkts.add(1);
            sender->handleResendPacket(packet, now);
            break;
        case Protocol::Packet::BUSY:
            Perf::counters.rx_busy_pkts.add(1);
            receiver->handleBusyPacket(packet, now);
            break;
        case Protocol::Packet::PING:
            Perf::counters.rx_ping_pkts.add(1);
            receiver->handlePingPacket(packet, now, sourceIp);
            break;
        case Protocol::Packet::UNKNOWN:
            Perf::counters.rx_unknown_pkts.add(1);
            sender->handleUnknownPacket(packet);
            break;
    }
    return retainPacket;
}

}  // namespace Core
}  // namespace Homa
