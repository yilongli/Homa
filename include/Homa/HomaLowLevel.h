/* Copyright (c) 2020 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#pragma once

#include "Homa.h"

namespace Homa {

/**
 * Low-level Homa APIs that might be useful for advanced users.
 *
 * These methods are initially declared to have non-public visibility so they
 * are hidden from normal users.
 */
struct LowLevelAPI {

    /// See Homa::Transport::checkTimeouts()
    static inline uint64_t TransportCheckTimeouts(Transport* transport)
    {
        return transport->checkTimeouts();
    }

    /// See Homa::Transport::processPacket()
    static inline void TransportProcessPacket(Transport* transport,
                                              Driver::Packet* packet,
                                              IpAddress source,
                                              Mailbox* mailbox)
    {
        transport->processPacket(packet, source, mailbox);
    }

    /// See Homa::InMessage::release()
    static inline void InMessageRelease(InMessage* inMessage)
    {
        inMessage->release();
    }

    /// See Homa::OutMessage::release()
    static inline void OutMessageRelease(OutMessage* outMessage)
    {
        outMessage->release();
    }
};

}  // namespace Homa