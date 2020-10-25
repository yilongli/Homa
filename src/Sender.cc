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

#include "Sender.h"

#include <Cycles.h>

#include <algorithm>

#include "ControlPacket.h"
#include "Debug.h"
#include "Perf.h"

namespace Homa {
namespace Core {

/**
 * Sender Constructor.
 *
 * @param transportId
 *      Unique identifier for the Transport that owns this Sender.
 * @param driver
 *      The driver used to send and receive packets.
 * @param policyManager
 *      Provides information about the network packet priority policies.
 * @param messageTimeoutCycles
 *      Number of cycles of inactivity to wait before this Sender declares an
 *      Sender::Message send failure.
 * @param pingIntervalCycles
 *      Number of cycles of inactivity to wait between checking on the liveness
 *      of an Sender::Message.
 */
Sender::Sender(uint64_t transportId, Driver* driver,
               Policy::Manager* policyManager, uint64_t messageTimeoutCycles,
               uint64_t pingIntervalCycles)
    : transportId(transportId)
    , driver(driver)
    , policyManager(policyManager)
    , nextMessageSequenceNumber(1)
    , DRIVER_QUEUED_BYTE_LIMIT(2 * driver->getMaxPayloadSize())
    , MESSAGE_TIMEOUT_INTERVALS(
          Util::roundUpIntDiv(messageTimeoutCycles, pingIntervalCycles))
    , messageBuckets(this)
    , queueMutex()
    , sendQueue()
    , sending()
    , sendReady(false)
    , messageAllocator()
    , timeoutMutex()
    , pingTimeouts(pingIntervalCycles)
{}

/**
 * Allocate an OutMessage that can be sent with this Sender.
 */
Homa::OutMessage*
Sender::allocMessage(uint16_t sourcePort)
{
    Perf::counters.allocated_tx_messages.add(1);
    return messageAllocator.construct(this, sourcePort);
}

/**
 * Execute the common processing logic that is shared among all incoming control
 * packets.
 *
 * @param packet
 *      Incoming control packet to be processed.
 * @return
 *      Pointer to the message targeted by the incoming packet, or nullptr if no
 *      matching message can be found.
 */
Sender::Message*
Sender::handleIncomingPacket(Driver::Packet* packet)
{
    // Find the message bucket
    Protocol::Packet::CommonHeader* commonHeader =
        static_cast<Protocol::Packet::CommonHeader*>(packet->payload);
    Protocol::MessageId msgId = commonHeader->messageId;
    MessageBucket* bucket = messageBuckets.getBucket(msgId);

    // Find the target message and update its expiration time
    SpinLock::Lock lock(bucket->mutex);
    Message* message = bucket->findMessage(msgId, lock);
    return message;
}

/**
 * Process an incoming ACK packet.
 *
 * @param packet
 *      Incoming ACK packet to be processed.
 */
void
Sender::handleAckPacket(Driver::Packet* packet)
{
    Message* message = handleIncomingPacket(packet);
    if (message == nullptr) {
        // No message for this ACK packet; must be old.
        return;
    }

    // Process ACK packet
    Protocol::MessageId msgId = message->id;
    OutMessage::Status status = message->getStatus();
    switch (status) {
        case OutMessage::Status::SENT:
            // Expected behavior
            message->setStatus(OutMessage::Status::DONE, false);
            break;
        case OutMessage::Status::CANCELED:
            // Canceled by the the application; just ignore the ACK.
            break;
        case OutMessage::Status::DONE:
            // Message already DONE
            NOTICE("Message (%lu, %lu) received duplicate ACK confirmation",
                   msgId.transportId, msgId.sequence);
            break;
        case OutMessage::Status::FAILED:
            WARNING(
                "Message (%lu, %lu) received ACK confirmation after the "
                "message was already declared FAILED",
                msgId.transportId, msgId.sequence);
            break;
        case OutMessage::Status::NOT_STARTED:
            WARNING(
                "Message (%lu, %lu) received ACK confirmation but sending has "
                "NOT_STARTED (message not yet sent); ACK is ignored.",
                msgId.transportId, msgId.sequence);
            break;
        case OutMessage::Status::IN_PROGRESS:
            WARNING(
                "Message (%lu, %lu) received ACK confirmation while sending "
                "is still IN_PROGRESS (message not completely sent); ACK is "
                "ignored.",
                msgId.transportId, msgId.sequence);
            break;
        default:
            // Unexpected status
            ERROR(
                "Message (%lu, %lu) received ACK confirmation while in an "
                "unexpected state; ACK is ignored.",
                msgId.transportId, msgId.sequence);
            break;
    }
}

/**
 * Process an incoming RESEND packet.
 *
 * @param packet
 *      Incoming RESEND packet to be processed.
 * @param now
 *      The rdtsc cycle that should be considered the "current" time.
 */
void
Sender::handleResendPacket(Driver::Packet* packet, uint64_t now)
{
    Message* message = handleIncomingPacket(packet);

    // Check for unexpected conditions
    if (message == nullptr) {
        // No message for this RESEND; RESEND must be old. Just ignore it; this
        // case should be pretty rare and the Receiver will timeout eventually.
        return;
    } else if (message->numPackets < 2) {
        // We should never get a RESEND for a single packet message.  Just
        // ignore this RESEND from a buggy Receiver.
        WARNING(
            "Message (%lu, %lu) with only 1 packet received unexpected RESEND "
            "request; peer Transport may be confused.",
            message->id.transportId, message->id.sequence);
        return;
    }

    message->lastAliveTime.store(now, std::memory_order_release);
    Protocol::Packet::ResendHeader* resendHeader =
        static_cast<Protocol::Packet::ResendHeader*>(packet->payload);
    int index = resendHeader->index;
    int resendEnd = index + resendHeader->num;

    // Check if RESEND request is out of range.
    if (index >= message->numPackets || resendEnd > message->numPackets) {
        WARNING(
            "Message (%lu, %lu) RESEND request range out of bounds: requested "
            "range [%d, %d); message only contains %d packets; peer Transport "
            "may be confused.",
            message->id.transportId, message->id.sequence, index, resendEnd,
            message->numPackets);
        return;
    }

    // In case a GRANT may have been lost, consider the RESEND a GRANT.
    QueuedMessageInfo* info = &message->queuedMessageInfo;
    SpinLock::Lock msg_lock(info->mutex);
    if (info->packetsGranted < resendEnd) {
        info->packetsGranted = resendEnd;
        // Note that the priority of messages under the unscheduled byte
        // limit will never be overridden since the resend index will not
        // exceed the preset packetsGranted.
        info->priority = resendHeader->priority;
        sendReady.store(true);
    }

    if (index >= info->packetsSent) {
        // If this RESEND is only requesting unsent packets, it must be that
        // this Sender has been busy and the Receiver is trying to ensure there
        // are no lost packets.  Reply BUSY and allow this Sender to send DATA
        // when it's ready.
        Perf::counters.tx_busy_pkts.add(1);
        ControlPacket::send<Protocol::Packet::BusyHeader>(
            driver, message->destination.ip, message->id);
    } else {
        // There are some packets to resend but only resend packets that have
        // already been sent.
        resendEnd = std::min(resendEnd, info->packetsSent);
        int resendPriority = policyManager->getResendPriority();
        for (int i = index; i < resendEnd; ++i) {
            Driver::Packet* resendPacket = message->getPacket(i);
            Perf::counters.tx_data_pkts.add(1);
            Perf::counters.tx_bytes.add(resendPacket->length);
            driver->sendPacket(resendPacket, message->destination.ip,
                               resendPriority);
        }
    }
}

/**
 * Process an incoming GRANT packet.
 *
 * @param packet
 *      Incoming GRANT packet to be processed.
 * @param now
 *      The rdtsc cycle that should be considered the "current" time.
 */
void
Sender::handleGrantPacket(Driver::Packet* packet, uint64_t now)
{
    Message* message = handleIncomingPacket(packet);
    if (message == nullptr) {
        // No message for this grant; grant must be old.
        return;
    }
    message->lastAliveTime.store(now, std::memory_order_release);

    Protocol::Packet::GrantHeader* grantHeader =
        static_cast<Protocol::Packet::GrantHeader*>(packet->payload);
    if (message->getStatus() == OutMessage::Status::IN_PROGRESS) {
        // Convert the byteLimit to a packet index limit such that the packet
        // that holds the last granted byte is also considered granted.  This
        // can cause at most 1 packet worth of data to be sent without a grant
        // but allows the sender to always send full packets.
        int incomingGrantIndex = Util::roundUpIntDiv(
            grantHeader->byteLimit, message->PACKET_DATA_LENGTH);

        // Make that grants don't exceed the number of packets.  Internally,
        // the sender always assumes that packetsGranted <= numPackets.
        if (incomingGrantIndex > message->numPackets) {
            WARNING(
                "Message (%lu, %lu) GRANT exceeds message length; granted "
                "packets: %d, message packets %d; extra grants are ignored.",
                message->id.transportId, message->id.sequence,
                incomingGrantIndex, message->numPackets);
            incomingGrantIndex = message->numPackets;
        }

        QueuedMessageInfo* info = &message->queuedMessageInfo;
        SpinLock::Lock msg_lock(info->mutex);
        if (info->packetsGranted < incomingGrantIndex) {
            info->packetsGranted = incomingGrantIndex;
            // Note that the priority of messages under the unscheduled byte
            // limit will never be overridden since the incomingGrantIndex will
            // not exceed the preset packetsGranted.
            info->priority = grantHeader->priority;
            sendReady.store(true);
        }
    }
}

/**
 * Process an incoming UNKNOWN packet.
 *
 * @param packet
 *      Incoming UNKNOWN packet to be processed.
 */
void
Sender::handleUnknownPacket(Driver::Packet* packet)
{
    Message* message = handleIncomingPacket(packet);
    if (message == nullptr) {
        // No message was found.
        return;
    }

    OutMessage::Status status = message->getStatus();
    assert(status != OutMessage::Status::NOT_STARTED);
    if (status != OutMessage::Status::IN_PROGRESS &&
        status != OutMessage::Status::SENT) {
        // The message is already considered "done" so the UNKNOWN packet
        // must be a stale response to a ping.
    } else if (message->needRetry) {
        // Either the Message or the ACK packet was lost; consider the message
        // failed since the application asked for the message not to be retried.
        message->setStatus(OutMessage::Status::FAILED, true);
    } else {
        // Message isn't done yet so we will restart sending the message.
        startMessage(message, true);
    }
}

/**
 * Allow the Sender to make progress toward sending outgoing messages.
 *
 * This method must be called eagerly to ensure messages are sent.
 */
void
Sender::poll()
{
    trySend();
    checkTimeouts();
}

/**
 * Process any outbound messages in a given bucket that need to be pinged to
 * ensure the message is kept alive by the receiver.
 *
 * @return
 *      The rdtsc cycle time when this method should be called again.
 */
uint64_t
Sender::checkTimeouts()
{
    // Used to hold PING headers temporarily so we don't have to hold the
    // timeout mutex while sending out PING packets.
    std::vector<std::pair<IpAddress, Protocol::MessageId>> pingPackets;
    pingPackets.reserve(32);

    uint64_t now = PerfUtils::Cycles::rdtsc();
    uint64_t nextCheckTime;
    const uint64_t pingIntervalCycles = pingTimeouts.getTimeoutInterval();

    SpinLock::UniqueLock timeout_lock(timeoutMutex);
    while (true) {
        // No remaining timeouts.
        if (pingTimeouts.empty()) {
            nextCheckTime = now + pingIntervalCycles;
            break;
        }

        // No remaining expired timeouts.
        Message* message = &pingTimeouts.front();
        if (now < message->pingTimeout.getExpirationTime()) {
            // In a pathological case, the timeouts may be lined up closely;
            // in order to avoid checking timeouts too frequently, enforce a
            // minimum interval between consecutive calls of this method
            // (currently set to 5% of the ping interval).
            uint64_t minStepCycles = pingIntervalCycles / 20;
            nextCheckTime = std::max(message->pingTimeout.getExpirationTime(),
                                     now + minStepCycles);
            break;
        }

        // Found an expired timeout, but it could be stale.
        uint64_t numPingTimeouts =
            (now - message->lastAliveTime) / pingIntervalCycles;
        if (numPingTimeouts == 0) {
            // We have heard from the Receiver in the last timeout period, but
            // we decided not to reset the timeout because almost all messages
            // will finish before the timeout expires.
            pingTimeouts.setTimeout(&message->pingTimeout);
            continue;
        }

        // Found expired timeout.
        OutMessage::Status status = message->getStatus();
        assert(status == OutMessage::Status::IN_PROGRESS ||
               status == OutMessage::Status::SENT);
        if (numPingTimeouts >= (uint64_t)MESSAGE_TIMEOUT_INTERVALS) {
            // Found expired message.

            // Unfortunately, we have to release the timeout mutex first to
            // avoid deadlock in setStatus(), which is ugly and inefficient;
            // fortunately, expired message should be pretty rare.
            timeout_lock.unlock();
            message->setStatus(OutMessage::Status::FAILED, true);
            timeout_lock.lock();
            continue;
        } else {
            pingTimeouts.setTimeout(&message->pingTimeout);
        }

        // Check if sender still has packets to send
        if (status == OutMessage::Status::IN_PROGRESS) {
            QueuedMessageInfo* info = &message->queuedMessageInfo;
            SpinLock::Lock msg_lock(info->mutex);
            if (info->packetsSent < info->packetsGranted) {
                // Sender is blocked on itself, no need to send ping
                continue;
            }
        }

        // Have not heard from the Receiver in the last timeout period. Ping
        // the receiver to ensure it still knows about this Message.
        pingPackets.emplace_back(message->destination.ip, message->id);
    }

    // Release the timeout mutex before actually sending out the PING packets.
    timeout_lock.unlock();
    Perf::counters.tx_ping_pkts.add(pingPackets.size());
    for (auto& [ip, msgId] : pingPackets) {
        ControlPacket::send<Protocol::Packet::PingHeader>(driver, ip, msgId);
    }
    return nextCheckTime;
}

/**
 * Destruct a Message.
 *
 * This method will detach the message from the Sender and release all contained
 * Packet objects.
 */
Sender::Message::~Message()
{
    Perf::counters.destroyed_tx_messages.add(1);

    // We assume that this message has been unlinked from the sendQueue before
    // this method is invoked.
    assert(getStatus() != OutMessage::Status::IN_PROGRESS);

    // Remove this message from the other data structures of the Sender.
    if (needTimeouts) {
        SpinLock::Lock timeout_lock(sender->timeoutMutex);
        sender->pingTimeouts.cancelTimeout(&pingTimeout);
    }
    {
        SpinLock::Lock bucket_lock(bucket->mutex);
        bucket->messages.remove(&bucketNode);
    }

    // Sender message must be contiguous
    driver->releasePackets(packets, numPackets);
}

/**
 * @copydoc Homa::OutMessage::append()
 */
void
Sender::Message::append(const void* source, size_t count)
{
    int _count = Util::downCast<int>(count);
    int packetIndex = messageLength / PACKET_DATA_LENGTH;
    int packetOffset = messageLength % PACKET_DATA_LENGTH;
    int bytesCopied = 0;
    int maxMessageLength = PACKET_DATA_LENGTH * MAX_MESSAGE_PACKETS;

    if (messageLength + _count > maxMessageLength) {
        WARNING("Max message size limit (%dB) reached; %d of %d bytes appended",
                maxMessageLength, maxMessageLength - messageLength, _count);
        _count = maxMessageLength - messageLength;
    }

    while (bytesCopied < _count) {
        int bytesToCopy =
            std::min(_count - bytesCopied, PACKET_DATA_LENGTH - packetOffset);
        Driver::Packet* packet = getOrAllocPacket(packetIndex);
        char* copyDst = static_cast<char*>(packet->payload);
        copyDst += packetOffset + TRANSPORT_HEADER_LENGTH;
        std::memcpy(copyDst, static_cast<const char*>(source) + bytesCopied,
                    bytesToCopy);
        // TODO(cstlee): A Message probably shouldn't be in charge of setting
        //               the packet length.
        packet->length += bytesToCopy;
        assert(packet->length <= TRANSPORT_HEADER_LENGTH + PACKET_DATA_LENGTH);
        bytesCopied += bytesToCopy;
        packetIndex++;
        packetOffset = 0;
    }

    messageLength += _count;
}

/**
 * @copydoc Homa::OutMessage::cancel()
 */
void
Sender::Message::cancel()
{
    setStatus(OutMessage::Status::CANCELED, true);
}

/**
 * @copydoc Homa::OutMessage::getStatus()
 */
OutMessage::Status
Sender::Message::getStatus() const
{
    return state.load(std::memory_order_acquire);
}

/**
 * Change the status of this message.
 *
 * All status change must be done by this method.
 *
 * @param newStatus
 *     The new status.
 * @param deschedule
 *     True if we should remove this message from the send queue.
 *
 * Note: special care must be taken when calling this method to avoid deadlocks
 * because our spinlock is not reentrant.
 */
void
Sender::Message::setStatus(Status newStatus, bool deschedule)
{
    // Whether to remove the message from the send queue depends on more than
    // just the message status; only the caller has enough information to make
    // the decision.
    if (deschedule) {
        // TODO: with jumbo packets, single-packet messages may also be paced

        // An outgoing message is on the sendQueue iff. it's still in progress
        // and subject to the sender's packet pacing mechanism; test this
        // condition first to reduce the expensive locking operation
        if ((numPackets > 1) &&
            (getStatus() == OutMessage::Status::IN_PROGRESS)) {
            SpinLock::Lock lock_queue(sender->queueMutex);
            sender->sendQueue.remove(&queuedMessageInfo.sendQueueNode);
        }
    }

    state.store(newStatus, std::memory_order_release);

    // Cancel the timeouts if the message reaches an end state.
    if (needTimeouts && (newStatus == OutMessage::Status::CANCELED ||
                         newStatus == OutMessage::Status::DONE ||
                         newStatus == OutMessage::Status::FAILED)) {
        SpinLock::Lock lock(sender->timeoutMutex);
        sender->pingTimeouts.cancelTimeout(&pingTimeout);
    }

    // This method is not the right place to remove the message from the bucket;
    // it's the job of Message::release().
}

/**
 * @copydoc Homa::OutMessage::length()
 */
size_t
Sender::Message::length() const
{
    return Util::downCast<size_t>(messageLength - start);
}

/**
 * @copydoc Homa::OutMessage::prepend()
 */
void
Sender::Message::prepend(const void* source, size_t count)
{
    int _count = Util::downCast<int>(count);
    // Make sure there is enough space reserved.
    assert(_count <= start);
    start -= _count;

    int packetIndex = start / PACKET_DATA_LENGTH;
    int packetOffset = start % PACKET_DATA_LENGTH;
    int bytesCopied = 0;

    while (bytesCopied < _count) {
        int bytesToCopy =
            std::min(_count - bytesCopied, PACKET_DATA_LENGTH - packetOffset);
        Driver::Packet* packet = getPacket(packetIndex);
        assert(packet != nullptr);
        char* copyDst = static_cast<char*>(packet->payload);
        copyDst += packetOffset + TRANSPORT_HEADER_LENGTH;
        std::memcpy(copyDst, static_cast<const char*>(source) + bytesCopied,
                    bytesToCopy);
        bytesCopied += bytesToCopy;
        packetIndex++;
        packetOffset = 0;
    }
}

/**
 * @copydoc Homa::OutMessage::release()
 */
void
Sender::Message::release()
{
    held.store(false, std::memory_order_release);
    if (getStatus() != OutMessage::Status::IN_PROGRESS) {
        // Ok to delete immediately since we don't have to wait for the message
        // to be sent.
        sender->messageAllocator.destroy(this);
    } else {
        // Defer deletion and wait for the message to be SENT.
    }
    Perf::counters.released_tx_messages.add(1);
}

/**
 * @copydoc Homa::OutMessage::reserve()
 */
void
Sender::Message::reserve(size_t count)
{
    int _count = Util::downCast<int>(count);

    // Make sure there have been no prior calls to append or prepend.
    assert(start == messageLength);

    int packetIndex = start / PACKET_DATA_LENGTH;
    int packetOffset = start % PACKET_DATA_LENGTH;
    int bytesReserved = 0;
    int maxMessageLength = PACKET_DATA_LENGTH * MAX_MESSAGE_PACKETS;

    if (start + _count > maxMessageLength) {
        WARNING("Max message size limit (%uB) reached; %u of %u bytes reserved",
                maxMessageLength, maxMessageLength - start, _count);
        _count = maxMessageLength - start;
    }

    while (bytesReserved < _count) {
        int bytesToReserve =
            std::min(_count - bytesReserved, PACKET_DATA_LENGTH - packetOffset);
        Driver::Packet* packet = getOrAllocPacket(packetIndex);
        // TODO(cstlee): A Message probably shouldn't be in charge of setting
        //               the packet length.
        packet->length += bytesToReserve;
        assert(packet->length <= TRANSPORT_HEADER_LENGTH + PACKET_DATA_LENGTH);
        bytesReserved += bytesToReserve;
        packetIndex++;
        packetOffset = 0;
    }

    start += _count;
    messageLength += _count;
}

/**
 * @copydoc Homa::OutMessage::send()
 */
void
Sender::Message::send(SocketAddress destination,
                      Sender::Message::Options options)
{
    // Prepare the message
    this->destination = destination;

    // Set the behavior flags based on the requested message semantics.
    switch (options) {
        case Options::AT_MOST_ONCE:
            // TODO(cstlee): at-most-once semantics needs duplicate suppression
        case Options::PROBABLY_ONCE:
            // Strictly speaking, we don't need to wait for an ACK if the
            // message can be unreliable. However, we would like to reduce
            // the chance of having to drop the entire message just because
            // the last few packets are lost.
            needAck = (numPackets > 1);
            needTimeouts = (numPackets > 1);
            needRetry = false;
            break;
        case Options::AT_LEAST_ONCE:
            // If we want to ensure that the message is delivered at least once
            // before reporting success to the application, we need to wait for
            // an acknowledgement from the receiver. Furthermore, in the rare
            // case when the receiver complains the lack of knowledge about the
            // message (either the entire message or the ACK packet is lost),
            // we need to retransmit the entire message.
            needAck = true;
            needTimeouts = true;
            needRetry = true;
            break;
        default:
            // Dead code
            assert(false);
            break;
    }

    // Kick start the transmission.
    sender->startMessage(this, false);
}

/**
 * Return the Packet with the given index.
 *
 * @param index
 *      A Packet's index in the array of packets that form the message.
 *      "packet index = "packet message offset" / PACKET_DATA_LENGTH
 * @return
 *      Pointer to a Packet at the given index if it exists; nullptr otherwise.
 */
Driver::Packet*
Sender::Message::getPacket(size_t index) const
{
    if (occupied.test(index)) {
        return packets[index];
    }
    return nullptr;
}

/**
 * Return the Packet with the given index.  If the Packet does yet exist,
 * allocate a new Packet.
 *
 * @param index
 *      A Packet's index in the array of packets that form the message.
 *      "packet index = "packet message offset" / PACKET_DATA_LENGTH
 * @return
 *      Pointer to a Packet at the given index.
 */
Driver::Packet*
Sender::Message::getOrAllocPacket(size_t index)
{
    if (!occupied.test(index)) {
        packets[index] = driver->allocPacket();
        occupied.set(index);
        numPackets++;
        // TODO(cstlee): A Message probably shouldn't be in charge of setting
        //               the packet length.
        packets[index]->length = TRANSPORT_HEADER_LENGTH;
    }
    return packets[index];
}

/**
 * (Re)start the transmission of an outgoing message.
 *
 * @param message
 *      Sender::Message to be sent.
 * @param restart
 *      False if the message is new to the transport; true means the message is
 *      restarted by the transport.
 */
void
Sender::startMessage(Sender::Message* message, bool restart)
{
    // If we are restarting an existing message, make sure it's not in the
    // sendQueue before making any changes to it.
    message->setStatus(OutMessage::Status::IN_PROGRESS, restart);

    // Get the current policy for unscheduled bytes.
    Policy::Unscheduled policy = policyManager->getUnscheduledPolicy(
        message->destination.ip, message->messageLength);
    uint16_t unscheduledIndexLimit = Util::roundUpIntDiv(
        policy.unscheduledByteLimit, message->PACKET_DATA_LENGTH);

    if (!restart) {
        // Fill out packet headers.
        int actualMessageLen = 0;
        for (int i = 0; i < message->numPackets; ++i) {
            Driver::Packet* packet = message->getPacket(i);
            assert(packet != nullptr);
            new (packet->payload) Protocol::Packet::DataHeader(
                message->source.port, message->destination.port, message->id,
                Util::downCast<uint32_t>(message->messageLength),
                policy.version, unscheduledIndexLimit,
                Util::downCast<uint16_t>(i), message->needAck);
            actualMessageLen +=
                (packet->length - message->TRANSPORT_HEADER_LENGTH);
        }
        assert(message->messageLength == actualMessageLen);

        // Start tracking the new message
        MessageBucket* bucket = message->bucket;
        SpinLock::Lock lock(bucket->mutex);
        assert(!bucket->messages.contains(&message->bucketNode));
        bucket->messages.push_back(&message->bucketNode);
    } else {
        // Update the policy version for each packet
        for (int i = 0; i < message->numPackets; ++i) {
            Driver::Packet* packet = message->getPacket(i);
            assert(packet != nullptr);
            Protocol::Packet::DataHeader* header =
                static_cast<Protocol::Packet::DataHeader*>(packet->payload);
            header->policyVersion = policy.version;
            header->unscheduledIndexLimit = unscheduledIndexLimit;
        }
    }

    // Kick start the message.
    assert(message->numPackets > 0);
    if (message->numPackets == 1) {
        // If there is only one packet in the message, send it right away.
        Driver::Packet* dataPacket = message->getPacket(0);
        assert(dataPacket != nullptr);
        Perf::counters.tx_data_pkts.add(1);
        Perf::counters.tx_bytes.add(dataPacket->length);
        driver->sendPacket(dataPacket, message->destination.ip,
                           policy.priority);
        message->setStatus(message->needAck ? OutMessage::Status::SENT
                                            : OutMessage::Status::DONE,
                           false);
        // This message must be still be held by the application since the
        // message still exists (it would have been removed when dropped
        // because single packet messages are never IN_PROGRESS). Assuming
        // the message is still held, we can skip the auto removal of SENT
        // and !held messages.
        assert(message->held);
    } else {
        // Otherwise, queue the message to be sent in SRPT order. This message
        // is not visible to other threads yet, so we can elide the per-message
        // QueuedMessageInfo::mutex.
        QueuedMessageInfo* info = &message->queuedMessageInfo;
        // Some values need to be updated
        info->unsentBytes = message->messageLength;
        info->packetsGranted =
            std::min(unscheduledIndexLimit,
                     Util::downCast<uint16_t>(message->numPackets));
        info->priority = policy.priority;
        info->packetsSent = 0;
        // Insert and move message into the correct order in the priority queue.
        SpinLock::Lock lock_queue(queueMutex);
        sendQueue.push_front(&info->sendQueueNode);
        Intrusive::deprioritize<Message>(&sendQueue, &info->sendQueueNode);
        sendReady.store(true);
    }

    // Initialize the timeouts
    if (message->needTimeouts) {
        message->lastAliveTime.store(PerfUtils::Cycles::rdtsc(),
                                     std::memory_order_release);
        SpinLock::Lock lock(timeoutMutex);
        pingTimeouts.setTimeout(&message->pingTimeout);
    }
}

/**
 * Send out packets for any messages with unscheduled/granted bytes.
 *
 * Pulled out of poll() for ease of testing.
 */
void
Sender::trySend()
{
    Perf::Timer timer;
    bool idle = true;

    // Skip when there are no messages to send.
    if (!sendReady) {
        return;
    }

    // Skip sending if another thread is already working on it.
    if (sending.test_and_set()) {
        return;
    }

    // Used to hold data packets that will be sent out in this method.
    using DataPacket = std::tuple<Driver::Packet*, IpAddress, int>;
    std::array<DataPacket, 16> dataPackets;
    int numPackets = 0;

    /* The goal is to send out packets for messages that have bytes that have
     * been "granted" (both scheduled and unscheduled grants).  Messages with
     * the fewest remaining bytes to send (unsentBytes) are sent first (SRPT).
     * Each time this method is called we will try to send enough packet to keep
     * the NIC busy but not too many as to cause excessive queue in the NIC.
     */
    SpinLock::UniqueLock lock_queue(queueMutex);
    uint32_t queuedBytesEstimate = driver->getQueuedBytes();
    // Optimistically assume we will finish sending every granted packet this
    // round; we will set again sendReady if it turns out we don't finish.
    sendReady = false;
    auto it = sendQueue.begin();
    while (it != sendQueue.end()) {
        Message& message = *it;
        assert(message.getStatus() == OutMessage::Status::IN_PROGRESS);
        QueuedMessageInfo* info = &message.queuedMessageInfo;

        SpinLock::UniqueLock msg_lock(info->mutex);
        assert(info->packetsGranted <= message.numPackets);
        while (info->packetsSent < info->packetsGranted) {
            // There are packets to send
            idle = false;
            Driver::Packet* packet = message.getPacket(info->packetsSent);
            assert(packet != nullptr);
            queuedBytesEstimate += packet->length;
            // Check if the send limit would be reached...
            if (queuedBytesEstimate > DRIVER_QUEUED_BYTE_LIMIT) {
                break;
            }
            // ... if not, send away (conceptually)!
            dataPackets.at(numPackets) = {packet, message.destination.ip,
                                          info->priority};
            ++numPackets;
            int packetDataBytes =
                packet->length - message.TRANSPORT_HEADER_LENGTH;
            assert(info->unsentBytes >= packetDataBytes);
            info->unsentBytes -= packetDataBytes;
            // The Message's unsentBytes only ever decreases.  See if the
            // updated Message should move up in the queue.
            Intrusive::prioritize<Message>(&sendQueue, &info->sendQueueNode);
            ++info->packetsSent;
        }

        if (info->packetsSent >= message.numPackets) {
            // We have finished sending the message.

            // Note: instead of relying on setStatus(), manually deschedule this
            // message since we are already holding the queueMutex (our spinlock
            // is not reentrant).
            assert(message.numPackets > 1 &&
                   message.getStatus() == OutMessage::Status::IN_PROGRESS);
            it = sendQueue.remove(it);
            if (message.needAck) {
                message.setStatus(OutMessage::Status::SENT, false);
            } else {
                // Note: our locking principle dictates that the global timeout
                // mutex must be acquired before the fine-grained message mutex.
                msg_lock.unlock();
                message.setStatus(OutMessage::Status::DONE, false);
            }

            if (!message.held.load(std::memory_order_acquire)) {
                // Ok to delete now that the message has been sent. This is fine
                // even if the application requested at-least-once semantics for
                // this message initially as there is no way for the application
                // to find out the result of this message anyway.
                messageAllocator.destroy(&message);
            }
        } else if (info->packetsSent >= info->packetsGranted) {
            // We have sent every granted packet.
            ++it;
        } else {
            // We hit the DRIVER_QUEUED_BYTES_LIMIT; stop sending for now.
            // We didn't finish sending all granted packets.
            sendReady = true;
            break;
        }
    }

    // Send out the data packets after releasing the sendQueue mutex.
    lock_queue.unlock();
    for (int i = 0; i < numPackets; ++i) {
        auto& [packet, ip, priority] = dataPackets[i];
        Perf::counters.tx_data_pkts.add(1);
        Perf::counters.tx_bytes.add(packet->length);
        driver->sendPacket(packet, ip, priority);
    }

    sending.clear();

    if (!idle) {
        Perf::counters.active_cycles.add(timer.split());
    }
}

}  // namespace Core
}  // namespace Homa
