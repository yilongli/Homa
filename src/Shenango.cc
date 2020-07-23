/* Copyright (c) 2020, Stanford University
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

#include "Debug.h"
#include "Homa/HomaLowLevel.h"
#include "Homa/Shenango.h"

using namespace Homa;

/**
 * A simple shim driver that translates Driver operations to Shenango
 * functions.
 */
struct ShenangoDriver : public Driver {

    ShenangoDriver() : Driver() {}

    Packet* allocPacket() override { return (Packet*) homa_tx_alloc_mbuf(); }

    void
    sendPacket(Packet* packet, uint32_t destination, int priority) override
    {
        int ret = net_tx_ip(packet, proto, destination);
        if (ret != 0) {
           mbuf_free(packet);
        }
    }

    uint32_t
    receivePackets(uint32_t maxPackets, Packet* receivedPackets[]) override
    {
        PANIC("receivePackets must not be called when used with Shenango");
        return 0;
    }

    void
    releasePackets(Packet* packets[], uint16_t numPackets) override
    {
        for (uint16_t i = 0; i < numPackets; i++) {
            mbuf_free(packets[i]);
        }
    }

    uint32_t getMaxPayloadSize() override { return max_payload; }

    uint32_t getBandwidth() override { return link_speed; }

    uint32_t getLocalAddress() override { return local_ip; }

    uint32_t getQueuedBytes() override
    {
        // FIXME!
        return 0;
    }

    /// Protocol number reserved for Homa; defined as IPPROTO_HOMA in Shenango.
    uint8_t proto;

    /// Local IP address of the driver.
    uint32_t local_ip;

    /// # bytes in a payload
    uint32_t max_payload;

    /// Effective network bandwidth, in Mbits/second.
    uint32_t link_speed;

    // struct mbuf *homa_tx_alloc_mbuf(void)
    void* (*homa_tx_alloc_mbuf) ();

    // int net_tx_ip(struct mbuf *m, uint8_t proto, uint32_t daddr)
    int (*net_tx_ip) (void* mbuf, uint8_t proto, uint32_t daddr);

    // void net_rx_trans(struct mbuf **ms, const unsigned int nr)?
    // TODO: No! net_rx_trans can't be swapped out; need to swap out
    // void (*recv) (struct trans_entry *e, struct mbuf *m) in trans_ops
    // actually, it should be the other way around for ingress pkts,
    // we need to assign TransportImpl::processPacket(Driver::Packet* packet, IpAddress sourceIp)
    // to homa_trans_ops.recv; core homa should never invoke Driver::receivePackets!
//    uint32_t (*recv_pkt) (uint32_t max_pkts, Packet* pkts[]);

    // void net_tx_release_mbuf(struct mbuf *m) &
    // void net_rx_release_mbuf(struct mbuf *m)?
    // Or just static inline void mbuf_free(struct mbuf *m)?
//    void (*release_pkts) (Packet* pkts[], uint16_t num_pkts);
    void (*mbuf_free) (void* mbuf);
};

homa_driver
homa_driver_create(uint8_t proto, uint32_t local_ip, uint32_t max_payload,
                   uint32_t link_speed, void* (*homa_tx_alloc_mbuf) (),
                   int (*net_tx_ip) (void*, uint8_t, uint32_t),
                   void (*mbuf_free) (void*))
{
    ShenangoDriver* driver = new ShenangoDriver();
    driver->proto = proto;
    driver->local_ip = local_ip;
    driver->max_payload = max_payload;
    driver->link_speed = link_speed;
    driver->homa_tx_alloc_mbuf = homa_tx_alloc_mbuf;
    driver->net_tx_ip = net_tx_ip;
    driver->mbuf_free = mbuf_free;
    return homa_driver{driver};
}

uint32_t homa_driver_max_payload(homa_driver drv)
{
    return static_cast<Driver*>(drv.p)->getMaxPayloadSize();
}

struct ShenangoMailbox final : Mailbox {

    /// An opaque pointer to homaconn_t in Shenango.
    void* homa_conn;

    /// Same function in Shenango
    void (*homa_mb_deliver) (void* homa_conn, homa_inmsg in_msg, uint32_t ip,
                             uint16_t port);

    void deliver(InMessage* message, uint32_t ip, uint16_t port) final
    {
        homa_mb_deliver(homa_conn, homa_inmsg{message}, ip, port);
    }
};

homa_mailbox homa_mailbox_create(void* homa_conn,
        void (*homa_mb_deliver) (void*, homa_inmsg, uint32_t, uint16_t))
{
    ShenangoMailbox* mb = new ShenangoMailbox();
    mb->homa_conn = homa_conn;
    mb->homa_mb_deliver = homa_mb_deliver;
    return homa_mailbox{mb};
}

void homa_mailbox_free(homa_mailbox mb)
{
    delete (ShenangoMailbox*) mb.p;
}

void homa_inmsg_ack(homa_inmsg in_msg)
{
    static_cast<InMessage*>(in_msg.p)->acknowledge();
}

bool homa_inmsg_dropped(homa_inmsg in_msg)
{
    return static_cast<InMessage*>(in_msg.p)->dropped();
}

void homa_inmsg_fail(homa_inmsg in_msg)
{
    static_cast<InMessage*>(in_msg.p)->fail();
}

size_t homa_inmsg_get(homa_inmsg in_msg, size_t ofs, void* dst, size_t len)
{
    return static_cast<InMessage*>(in_msg.p)->get(ofs, dst, len);
}

size_t homa_inmsg_len(homa_inmsg in_msg)
{
    return static_cast<InMessage*>(in_msg.p)->length();
}

void homa_inmsg_release(homa_inmsg in_msg)
{
    LowLevelAPI::InMessageRelease(static_cast<InMessage*>(in_msg.p));
}

void homa_inmsg_strip(homa_inmsg in_msg, size_t n)
{
    static_cast<InMessage*>(in_msg.p)->strip(n);
}

void homa_outmsg_append(homa_outmsg out_msg, const void* buf, size_t len)
{
    static_cast<OutMessage*>(out_msg.p)->append(buf, len);
}

void homa_outmsg_cancel(homa_outmsg out_msg)
{
    static_cast<OutMessage*>(out_msg.p)->cancel();
}

int homa_outmsg_status(homa_outmsg out_msg)
{
    return int(static_cast<OutMessage*>(out_msg.p)->getStatus());
}

void homa_outmsg_prepend(homa_outmsg out_msg, const void* buf, size_t len)
{
    static_cast<OutMessage*>(out_msg.p)->prepend(buf, len);
}

void homa_outmsg_reserve(homa_outmsg out_msg, size_t n)
{
    static_cast<OutMessage*>(out_msg.p)->reserve(n);
}

void homa_outmsg_send(homa_outmsg out_msg, uint32_t ip, uint16_t port)
{
    static_cast<OutMessage*>(out_msg.p)->send({ip, port});
}

void homa_outmsg_register_cb(homa_outmsg out_msg, void (*cb) (void*),
                             void *data)
{
    static_cast<OutMessage*>(out_msg.p)->registerCallback(cb, data);
}

void homa_outmsg_release(homa_outmsg out_msg)
{
    LowLevelAPI::OutMessageRelease(static_cast<OutMessage*>(out_msg.p));
}

homa_trans homa_trans_create(homa_driver drv, uint64_t id)
{
    return homa_trans{Transport::create(static_cast<Driver*>(drv.p), id)};
}

homa_outmsg homa_trans_alloc(homa_trans trans, uint16_t sport)
{
    auto out_msg = static_cast<Transport*>(trans.p)->alloc(sport);
    return homa_outmsg{out_msg.release()};
}

uint64_t homa_trans_check_timeouts(homa_trans trans)
{
    return LowLevelAPI::TransportCheckTimeouts(
            static_cast<Transport*>(trans.p));
}


uint64_t homa_trans_id(homa_trans trans)
{
    return static_cast<Transport*>(trans.p)->getId();
}

void homa_trans_proc(homa_trans trans, void* mbuf, uint32_t src_ip,
                     homa_mailbox mb)
{
    LowLevelAPI::TransportProcessPacket(static_cast<Transport*>(trans.p),
        static_cast<Driver::Packet*>(mbuf), src_ip,
        static_cast<Mailbox*>(mb.p));
}
