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

#include "Receiver.h"

#include <Cycles.h>

#include "Perf.h"
#include "Tub.h"

namespace Homa {
namespace Core {

/**
 * Receiver constructor.
 *
 * @param driver
 *      The driver used to send and receive packets.
 * @param policyManager
 *      Provides information about the grant and network priority policies.
 * @param messageTimeoutCycles
 *      Number of cycles of inactivity to wait before this Receiver declares an
 *      Receiver::Message receive failure.
 * @param resendIntervalCycles
 *      Number of cycles of inactivity to wait between requesting retransmission
 *      of un-received parts of a message.
 */
Receiver::Receiver(Driver* driver, Policy::Manager* policyManager,
                   uint64_t messageTimeoutCycles, uint64_t resendIntervalCycles)
    : driver(driver)
    , policyManager(policyManager)
    , MESSAGE_TIMEOUT_INTERVALS(
          Util::roundUpIntDiv(messageTimeoutCycles, resendIntervalCycles))
    , messageBuckets()
    , schedulerMutex()
    , scheduledPeers()
    , receivedMessages()
    , granting()
    , messageAllocator()
    , externalBuffers()
    , timeoutMutex()
    , resendTimeouts(resendIntervalCycles)
{}

/**
 * Receiver destructor.
 */
Receiver::~Receiver()
{
    // To ensure that all resources of a Receiver can be freed correctly, it's
    // the user's responsibility to ensure the following before destructing the
    // Receiver:
    //  - The transport must have been taken "offline" so that no more incoming
    //    packets will arrive.
    //  - All completed incoming messages that are delivered to the application
    //    must have been returned back to the Receiver (so that they don't hold
    //    dangling pointers to the destructed Receiver afterwards).
    //  - There must be only one thread left that can hold a reference to the
    //    transport (the destructor is designed to run exactly once).

    // Technically speaking, the Receiver is designed in a way that a default
    // destructor should be sufficient. However, for clarity and debugging
    // purpose, we decided to write the cleanup procedure explicitly anyway.

    // Destruct all MessageBucket's and the Messages within.
    messageBuckets.buckets.clear();

    // Destruct all Peer's. Peer's must be removed from scheduledPeers first.
    scheduledPeers.clear();
    peerTable.clear();

    // Destruct all completed Messages that are not yet delivered.
    for (auto& message : receivedMessages.queue) {
        messageAllocator.destroy(&message);
    }
}

/**
 * Execute the common processing logic that is shared among all incoming
 * packets.
 *
 * @param packet
 *      Incoming packet to be processed.
 * @param now
 *      The rdtsc cycle that should be considered the "current" time.
 * @param createIfAbsent
 *      True if a new Message should be constructed when no matching message
 *      can be found.
 * @param sourceIp
 *      Source IP address of the packet. Only valid when createIfAbsent is true.
 * @return
 *      Pointer to the message targeted by the incoming packet, or nullptr if no
 *      matching message can be found.
 */
Receiver::Message*
Receiver::handleIncomingPacket(Driver::Packet* packet, uint64_t now,
                               bool createIfAbsent, IpAddress* sourceIp)
{
    // Find the message bucket.
    Protocol::Packet::CommonHeader* commonHeader =
        static_cast<Protocol::Packet::CommonHeader*>(packet->payload);
    Protocol::MessageId msgId = commonHeader->messageId;
    MessageBucket* bucket = messageBuckets.getBucket(msgId);

    bool needSchedule = false;
    Message* message;
    {
        // Acquire the bucket mutex to ensure that a new message can be
        // constructed and inserted to the bucket atomically.
        SpinLock::Lock lock_bucket(bucket->mutex);

        // Find the target message, or construct a new message if necessary.
        message = bucket->findMessage(msgId, lock_bucket);
        if (message == nullptr && createIfAbsent) {
            // Construct a new message
            Protocol::Packet::DataHeader* header =
                static_cast<Protocol::Packet::DataHeader*>(packet->payload);
            int messageLength = header->totalLength;
            int numUnscheduledPackets = header->unscheduledIndexLimit;
            SocketAddress srcAddress = {
                .ip = *sourceIp, .port = be16toh(header->common.prefix.sport)};
            message = messageAllocator.construct(
                this, driver, sizeof(Protocol::Packet::DataHeader),
                messageLength, header->common.messageId, srcAddress,
                numUnscheduledPackets, (bool)header->needAck);
            Perf::counters.allocated_rx_messages.add(1);

            // Start tracking the message.
            bucket->messages.push_back(&message->bucketNode);

            policyManager->signalNewMessage(
                message->source.ip, header->policyVersion, header->totalLength);
            if (message->scheduled) {
                // Don't schedule the message while holding the bucket mutex.
                needSchedule = true;
            }
        }
    }

    // Message needs to be entered into the scheduler.
    if (needSchedule) {
        SpinLock::Lock lock_scheduler(schedulerMutex);
        schedule(message, lock_scheduler);
    }

    // The sender is still alive.
    if (message != nullptr && message->needTimeouts) {
        message->lastAliveTime.store(now, std::memory_order_release);
    }
    return message;
}

/**
 * Process an incoming DATA packet.
 *
 * @param packet
 *      The incoming packet to be processed.
 * @param now
 *      The rdtsc cycle that should be considered the "current" time.
 * @param sourceIp
 *      Source IP address of the packet.
 */
void
Receiver::handleDataPacket(Driver::Packet* packet, uint64_t now,
                           IpAddress sourceIp)
{
    // Find the message
    Message* message = handleIncomingPacket(packet, now, true, &sourceIp);
    Protocol::Packet::DataHeader* header =
        static_cast<Protocol::Packet::DataHeader*>(packet->payload);

    // Sanity checks
    assert(message->source.ip == sourceIp);
    assert(message->source.port == be16toh(header->common.prefix.sport));
    assert(message->messageLength == Util::downCast<int>(header->totalLength));

    // Copy the packet's payload into the message buffer if this packet hasn't
    // been received before.
    SpinLock::UniqueLock msg_lock(message->mutex);
    uint16_t index = header->index;
    if (message->occupied.test(index)) {
        // Must be a duplicate packet; drop it.
        return;
    }
    message->occupied.set(index);
    message->numPackets++;
    bool messageComplete = (message->numPackets == message->numExpectedPackets);
    std::memcpy(
        message->buffer + index * message->PACKET_DATA_LENGTH,
        static_cast<char*>(packet->payload) + message->TRANSPORT_HEADER_LENGTH,
        packet->length - message->TRANSPORT_HEADER_LENGTH);
    msg_lock.unlock();

    // Complete the message if all packets have been received.
    if (messageComplete) {
        message->state.store(Message::State::COMPLETED,
                             std::memory_order_release);
        if (message->needTimeouts) {
            SpinLock::Lock lock(timeoutMutex);
            resendTimeouts.cancelTimeout(&message->resendTimeout);
        }

        // Deliver the message to the user of the transport.
        SpinLock::Lock lock_received_messages(receivedMessages.mutex);
        receivedMessages.queue.push_back(&message->receivedMessageNode);
        Perf::counters.received_rx_messages.add(1);

        // Send back an ACK if it's requested by the sender.
        if (message->needAck) {
            Perf::counters.tx_ack_pkts.add(1);
            ControlPacket::send<Protocol::Packet::AckHeader>(
                driver, message->source.ip, message->id);
        }
        return;
    }

    // Update schedule for scheduled messages.
    if (message->scheduled) {
        SpinLock::Lock lock_scheduler(schedulerMutex);
        ScheduledMessageInfo* info = &message->scheduledMessageInfo;
        // Update the schedule if the message is still being scheduled
        // (i.e. still linked to a scheduled peer).
        if (info->peer != nullptr) {
            int packetDataBytes =
                packet->length - message->TRANSPORT_HEADER_LENGTH;
            assert(info->bytesRemaining >= packetDataBytes);
            info->bytesRemaining -= packetDataBytes;
            updateSchedule(message, lock_scheduler);
        }
    }
}

/**
 * Process an incoming BUSY packet.
 *
 * @param packet
 *      The incoming BUSY packet to be processed.
 * @param now
 *      The rdtsc cycle that should be considered the "current" time.
 */
void
Receiver::handleBusyPacket(Driver::Packet* packet, uint64_t now)
{
    handleIncomingPacket(packet, now, false, nullptr);
}

/**
 * Process an incoming PING packet.
 *
 * @param packet
 *      The incoming PING packet to be processed.
 * @param now
 *      The rdtsc cycle that should be considered the "current" time.
 * @param sourceIp
 *      Source IP address of the packet.
 */
void
Receiver::handlePingPacket(Driver::Packet* packet, uint64_t now,
                           IpAddress sourceIp)
{
    Message* message = handleIncomingPacket(packet, now, false, nullptr);
    if (message != nullptr) {
        // We are here either because a GRANT  got lost, or we haven't issued a
        // GRANT in along time.  Send out the latest GRANT if one exists or just
        // an "empty" GRANT to let the Sender know we are aware of the message.

        // Default to an empty GRANT.  Sending an empty GRANT will not reset the
        // priority of a Message that has not yet received a GRANT.
        int bytesGranted = 0;
        int priority = 0;

        if (message->scheduled) {
            // Use the scheduled GRANT information for scheduled messages.
            // This may still contain the default values (i.e. an empty GRANT)
            // if no GRANTs have been issued yet.
            SpinLock::Lock lock_scheduler(schedulerMutex);
            ScheduledMessageInfo* info = &message->scheduledMessageInfo;
            bytesGranted = info->bytesGranted;
            priority = info->priority;
        }

        Perf::counters.tx_grant_pkts.add(1);
        ControlPacket::send<Protocol::Packet::GrantHeader>(
            driver, message->source.ip, message->id, bytesGranted, priority);
    } else {
        // We are here because we have no knowledge of the message the Sender is
        // asking about.  Reply UNKNOWN so the Sender can react accordingly.
        Perf::counters.tx_unknown_pkts.add(1);
        Protocol::Packet::CommonHeader* header =
            static_cast<Protocol::Packet::CommonHeader*>(packet->payload);
        ControlPacket::send<Protocol::Packet::UnknownHeader>(driver, sourceIp,
                                                             header->messageId);
    }
}

/**
 * Return a handle to a new received Message.
 *
 * The Transport should regularly call this method to insure incoming messages
 * are processed.
 *
 * @return
 *      A new Message which has been received, if available; otherwise, nullptr.
 */
Homa::InMessage*
Receiver::receiveMessage()
{
    SpinLock::Lock lock_received_messages(receivedMessages.mutex);
    Message* message = nullptr;
    if (!receivedMessages.queue.empty()) {
        message = &receivedMessages.queue.front();
        receivedMessages.queue.pop_front();
        Perf::counters.delivered_rx_messages.add(1);
    }
    return message;
}

/**
 * Allow the Receiver to make progress toward receiving incoming messages.
 *
 * This method must be called eagerly to ensure messages are received.
 */
void
Receiver::poll()
{
    trySendGrants();
    checkTimeouts();
}

/**
 * Process any inbound messages that may need to issue resends.
 *
 * Pulled out of poll() for ease of testing.
 *
 * @return
 *      The rdtsc cycle time when this method should be called again.
 */
uint64_t
Receiver::checkTimeouts()
{
    // Used to hold RESEND headers temporarily so we don't have to hold the
    // timeout mutex while sending out RESEND packets.
    std::vector<std::pair<IpAddress, Tub<Protocol::Packet::ResendHeader>>>
        resendPackets;
    resendPackets.reserve(32);

    uint64_t now = PerfUtils::Cycles::rdtsc();
    uint64_t nextCheckTime;
    const uint64_t resendIntervalCycles = resendTimeouts.getTimeoutInterval();

    SpinLock::UniqueLock timeout_lock(timeoutMutex);
    resendPackets.clear();
    while (true) {
        // No remaining timeouts.
        if (resendTimeouts.empty()) {
            nextCheckTime = now + resendIntervalCycles;
            break;
        }

        // No remaining expired timeouts.
        Message* message = &resendTimeouts.front();
        if (now < message->resendTimeout.getExpirationTime()) {
            // In a pathological case, the timeouts may be lined up closely;
            // in order to avoid checking timeouts too frequently, enforce a
            // minimum interval between consecutive calls of this method
            // (currently set to 5% of the resend interval).
            uint64_t minStepCycles = resendIntervalCycles / 20;
            nextCheckTime = std::max(message->resendTimeout.getExpirationTime(),
                                     now + minStepCycles);
            break;
        }

        // Found an expired timeout, but it could be stale.
        uint64_t numResendTimeouts =
            (now - message->lastAliveTime) / resendIntervalCycles;
        if (numResendTimeouts == 0) {
            // We have heard from the Sender in the last timeout period, but
            // we decided not to reset the timeout because almost all messages
            // will finish before the timeout expires.
            resendTimeouts.setTimeout(&message->resendTimeout);
            continue;
        }

        // Found expired timeout.
        assert(message->getState() == Message::State::IN_PROGRESS);
        if (numResendTimeouts >= (uint64_t)MESSAGE_TIMEOUT_INTERVALS) {
            // Message timed out before being fully received; drop the message.
            messageAllocator.destroy(message);
            continue;
        } else {
            resendTimeouts.setTimeout(&message->resendTimeout);
        }

        // This Receiver expected to have heard from the Sender within the
        // last timeout period but it didn't.  Request a resend of granted
        // packets in case DATA packets got lost.
        uint16_t index = 0;
        uint16_t num = 0;
        int grantIndexLimit = message->numUnscheduledPackets;

        int resendPriority;
        if (message->scheduled) {
            SpinLock::Lock lock_scheduler(schedulerMutex);
            ScheduledMessageInfo* info = &message->scheduledMessageInfo;
            int receivedBytes = info->messageLength - info->bytesRemaining;
            if (receivedBytes >= info->bytesGranted) {
                // Sender is blocked on this Receiver; all granted packets
                // have already been received.  No need to check for resend.
                continue;
            } else if (grantIndexLimit * message->PACKET_DATA_LENGTH <
                       info->bytesGranted) {
                grantIndexLimit = Util::roundUpIntDiv(
                    info->bytesGranted, message->PACKET_DATA_LENGTH);
            }

            // The RESEND also includes the current granted priority so that it
            // can act as a GRANT in case a GRANT was lost.  If this message
            // hasn't been scheduled (i.e. no grants have been sent) then the
            // priority will hold the default value; this is ok since the Sender
            // will ignore the priority field for resends of purely unscheduled
            // packets (see Sender::handleResendPacket()).
            resendPriority = info->priority;
        } else {
            resendPriority = 0;
        }

        SpinLock::Lock msg_lock(message->mutex);
        for (int i = 0; i < grantIndexLimit; ++i) {
            if (!message->occupied.test(i)) {
                // Unreceived packet
                if (num == 0) {
                    // First unreceived packet
                    index = Util::downCast<uint16_t>(i);
                }
                ++num;
            } else {
                // Received packet
                if (num != 0) {
                    // Send out the range of packets found so far.
                    resendPackets.emplace_back(message->source.ip);
                    auto& resend = resendPackets.back().second;
                    resend.construct(message->id, index, num, resendPriority);
                    num = 0;
                }
            }
        }
        if (num != 0) {
            // Send out the last range of packets found.
            resendPackets.emplace_back(message->source.ip);
            auto& resend = resendPackets.back().second;
            resend.construct(message->id, index, num, resendPriority);
        }
    }

    // Release the timeout mutex before actually sending out the RESEND packets.
    timeout_lock.unlock();
    Perf::counters.tx_resend_pkts.add(resendPackets.size());
    for (auto& pair : resendPackets) {
        IpAddress address = pair.first;
        Protocol::Packet::ResendHeader& header = *pair.second;
        ControlPacket::send<Protocol::Packet::ResendHeader>(
            driver, address, header.common.messageId, header.index, header.num,
            header.priority);
    }
    return nextCheckTime;
}

/**
 * Destruct a Message.
 *
 * This method will detach the message from the transport and release all
 * contained Packet objects.
 */
Receiver::Message::~Message()
{
    Perf::counters.destroyed_rx_messages.add(1);
    Receiver* receiver = bucket->receiver;

    // Unschedule the message if it is still scheduled (i.e. still linked to a
    // scheduled peer).
    if (scheduled) {
        SpinLock::Lock lock_scheduler(receiver->schedulerMutex);
        ScheduledMessageInfo* info = &scheduledMessageInfo;
        if (info->peer != nullptr) {
            receiver->unschedule(this, lock_scheduler);
        }
    }

    // Remove this message from the other data structures of the Receiver.
    if (needTimeouts) {
        SpinLock::Lock lock(receiver->timeoutMutex);
        receiver->resendTimeouts.cancelTimeout(&resendTimeout);
    }
    {
        SpinLock::Lock bucket_lock(bucket->mutex);
        bucket->messages.remove(&bucketNode);

        SpinLock::Lock receive_lock(receiver->receivedMessages.mutex);
        receiver->receivedMessages.queue.remove(&receivedMessageNode);
    }

    // Release the external buffer, if any.
    if (buffer != internalBuffer) {
        MessageBuffer<MAX_MESSAGE_LENGTH>* externalBuf =
            (MessageBuffer<MAX_MESSAGE_LENGTH>*)buffer;
        receiver->externalBuffers.destroy(externalBuf);
    }
}

/**
 * @copydoc Homa::InMessage::data()
 */
void*
Receiver::Message::data() const
{
    return buffer;
}

/**
 * @copydoc Homa::InMessage::length()
 */
size_t
Receiver::Message::length() const
{
    return Util::downCast<size_t>(messageLength);
}

/**
 * @copydoc Homa::InMessage::release()
 */
void
Receiver::Message::release()
{
    bucket->receiver->messageAllocator.destroy(this);
}

/**
 * Send GRANTs to incoming Message according to the Receiver's policy.
 */
void
Receiver::trySendGrants()
{
    Perf::Timer timer;

    // Skip scheduling if another poller is already working on it.
    if (granting.test_and_set()) {
        return;
    }

    Tub<SpinLock::Lock> sched_lock;
    sched_lock.construct(schedulerMutex);
    if (scheduledPeers.empty()) {
        granting.clear();
        return;
    }

    // Used to hold GRANT packets that will be sent out in this method.
    using Grant = std::tuple<IpAddress, Protocol::MessageId, uint32_t, int>;
    std::array<Grant, 16> grantPackets;
    int numGrants = 0;

    /* The overall goal is to grant up to policy.degreeOvercommitment number of
     * scheduled messages simultaneously.  Each of these messages should always
     * have at least policy.minScheduledBytes number of bytes granted.  Ideally,
     * each message will be assigned a different network priority based on a
     * message's number of bytesRemaining.  The message with the fewest
     * bytesRemaining (SRPT) will be assigned the highest priority.  If the
     * number of messages to grant exceeds the number of available priorities,
     * the lowest priority is shared by multiple messages.  If the number of
     * messages to grant is fewer than the available priorities, than the
     * messages are assigned to the lowest available priority.
     */
    Policy::Scheduled policy = policyManager->getScheduledPolicy();
    assert(policy.degreeOvercommitment > policy.maxScheduledPriority);
    assert(policy.minScheduledBytes <= policy.minScheduledBytes);
    int unusedPriorities =
        std::max(0, (policy.maxScheduledPriority + 1) -
                        Util::downCast<int>(scheduledPeers.size()));

    auto it = scheduledPeers.begin();
    int slot = 0;
    while (it != scheduledPeers.end() && slot < policy.degreeOvercommitment) {
        assert(!it->scheduledMessages.empty());
        Message* message = &it->scheduledMessages.front();
        ScheduledMessageInfo* info = &message->scheduledMessageInfo;
        // Access message const variables without message mutex.
        const Protocol::MessageId id = message->id;
        const IpAddress sourceIp = message->source.ip;

        // Recalculate message priority
        info->priority =
            std::max(0, policy.maxScheduledPriority - slot - unusedPriorities);

        // Send a GRANT if there are too few bytes granted and unreceived.
        int receivedBytes = info->messageLength - info->bytesRemaining;
        if (info->bytesGranted - receivedBytes < policy.minScheduledBytes) {
            // Calculate new grant limit
            int newGrantLimit = std::min(
                receivedBytes + policy.maxScheduledBytes, info->messageLength);
            assert(newGrantLimit >= info->bytesGranted);
            info->bytesGranted = newGrantLimit;
            // Defer sending the grant packets.
            grantPackets.at(numGrants) = {
                sourceIp, id, Util::downCast<uint32_t>(newGrantLimit),
                info->priority};
            ++numGrants;
        }

        // Update the iterator first since calling unschedule() may cause the
        // iterator to become invalid.
        ++it;

        if (info->messageLength <= info->bytesGranted) {
            // All packets granted, unschedule the message.
            unschedule(message, *sched_lock);
            Perf::counters.active_cycles.add(timer.split());
        }

        ++slot;
    }

    // Release the scheduler mutex before sending out the grant packets.
    if (numGrants > 0) {
        sched_lock.destroy();
        Perf::counters.tx_grant_pkts.add(numGrants);
        for (int i = 0; i < numGrants; ++i) {
            auto& [sourceIp, msgId, bytesGranted, priority] = grantPackets[i];
            ControlPacket::send<Protocol::Packet::GrantHeader>(
                driver, sourceIp, msgId, bytesGranted, priority);
        }
        Perf::counters.active_cycles.add(timer.split());
    }

    granting.clear();
}

/**
 * Add a Message to the schedule.
 *
 * Helper function separated mostly for ease of testing.
 *
 * @param message
 *      Message to be added.
 * @param lock
 *      Reminder to hold the Receiver::schedulerMutex during this call.
 */
void
Receiver::schedule(Receiver::Message* message, const SpinLock::Lock& lock)
{
    (void)lock;
    ScheduledMessageInfo* info = &message->scheduledMessageInfo;
    Peer* peer = &peerTable[message->source.ip];
    // Insert the Message
    peer->scheduledMessages.push_front(&info->scheduledMessageNode);
    Intrusive::deprioritize<Message>(&peer->scheduledMessages,
                                     &info->scheduledMessageNode);
    info->peer = peer;
    if (!scheduledPeers.contains(&peer->scheduledPeerNode)) {
        // Must be the only message of this peer; push the peer to the
        // end of list to be moved later.
        assert(peer->scheduledMessages.size() == 1);
        scheduledPeers.push_front(&peer->scheduledPeerNode);
        Intrusive::deprioritize<Peer>(&scheduledPeers,
                                      &peer->scheduledPeerNode);
    } else if (&info->peer->scheduledMessages.front() == message) {
        // Update the Peer's position in the queue since the new message is the
        // peer's first scheduled message.
        Intrusive::prioritize<Peer>(&scheduledPeers, &peer->scheduledPeerNode);
    } else {
        // The peer's first scheduled message did not change.  Nothing to do.
    }
}

/**
 * Remove a Message from the schedule.
 *
 * Helper function separated mostly for ease of testing.
 *
 * @param message
 *      Message to be removed.
 * @param lock
 *      Reminder to hold the Receiver::schedulerMutex during this call.
 */
void
Receiver::unschedule(Receiver::Message* message, const SpinLock::Lock& lock)
{
    (void)lock;
    ScheduledMessageInfo* info = &message->scheduledMessageInfo;
    assert(info->peer != nullptr);
    Peer* peer = info->peer;
    Intrusive::List<Peer>::Iterator it =
        scheduledPeers.get(&peer->scheduledPeerNode);
    Peer::ComparePriority comp;

    // Remove message.
    assert(peer->scheduledMessages.contains(&info->scheduledMessageNode));
    peer->scheduledMessages.remove(&info->scheduledMessageNode);
    info->peer = nullptr;

    // Cleanup the schedule
    if (peer->scheduledMessages.empty()) {
        // Remove the empty peer from the schedule (the peer object is still
        // alive).
        scheduledPeers.remove(it);
    } else if (std::next(it) == scheduledPeers.end() ||
               !comp(*std::next(it), *it)) {
        // Peer already in the right place (peer incremented as part of the
        // check).  Note that only "next" needs be checked (and not "prev")
        // since removing a message cannot increase the peer's priority.
    } else {
        // Peer needs to be moved.
        Intrusive::deprioritize<Peer>(&scheduledPeers,
                                      &peer->scheduledPeerNode);
    }
}

/**
 * Update Message's position in the schedule.
 *
 * Called when new data has arrived for the Message.
 *
 * Helper function separated mostly for ease of testing.
 *
 * @param message
 *      Message whose position should be updated.
 * @param lock
 *      Reminder to hold the Receiver::schedulerMutex during this call.
 */
void
Receiver::updateSchedule(Receiver::Message* message, const SpinLock::Lock& lock)
{
    (void)lock;
    ScheduledMessageInfo* info = &message->scheduledMessageInfo;
    assert(info->peer != nullptr);
    assert(info->peer->scheduledMessages.contains(&info->scheduledMessageNode));

    // Update the message's position within its Peer scheduled message queue.
    Intrusive::prioritize<Message>(&info->peer->scheduledMessages,
                                   &info->scheduledMessageNode);

    // Update the Peer's position in the queue if this message is now the first
    // scheduled message.
    if (&info->peer->scheduledMessages.front() == message) {
        Intrusive::prioritize<Peer>(&scheduledPeers,
                                    &info->peer->scheduledPeerNode);
    }
}

}  // namespace Core
}  // namespace Homa
