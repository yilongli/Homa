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

/**
 * @file
 * TODO:
 * This file uses Shenango coding style.
 */

#pragma once

#include "BaseTypes.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Define handle types for various Homa objects.
 *
 * A handle type is essentially a thin wrapper around an opaque pointer.
 * Compared to generic pointers, using handle types in the C API enables
 * some type safety.
 */
#define DEFINE_HOMA_OBJ_HANDLE(x) typedef struct { void* p; } homa_##x;

DEFINE_HOMA_OBJ_HANDLE(driver)
DEFINE_HOMA_OBJ_HANDLE(inmsg)
DEFINE_HOMA_OBJ_HANDLE(outmsg)
DEFINE_HOMA_OBJ_HANDLE(mailbox)
DEFINE_HOMA_OBJ_HANDLE(trans)


/* ============================ */
/*     Homa::Driver API         */
/* ============================ */

/**
 * homa_driver_create - creates a shim driver that translates Homa::Driver
 * operation to Shenango functions
 * @proto: protocol number reserved for Homa transport protocol
 * @local_ip: local IP address of the driver
 * @max_payload: maximum number of bytes carried by the packet payload
 * @link_speed: effective network bandwidth, in Mbits/second
 * @homa_tx_alloc_mbuf: function to allocate a packet buffer from shenango
 * @net_tx_ip: function to transmit an IP packet using shenango
 * @mbuf_free: function to free a packet buffer allocated from shenango
 *
 * Returns a handle to the driver created.
 */
extern homa_driver homa_driver_create(uint8_t proto, uint32_t local_ip,
        uint32_t max_payload, uint32_t link_speed,
        void* (*homa_tx_alloc_mbuf) (),
        int (*net_tx_ip) (void*, uint8_t, uint32_t),
        void (*mbuf_free) (void*));

/**
 * homa_driver_max_payload - C-binding for Homa::Driver::getMaxPayload
 */
extern uint32_t homa_driver_max_payload(homa_driver drv);

/* ============================ */
/*     Homa::Mailbox API        */
/* ============================ */

/**
 * homa_mailbox_create - creates a Shenango-specific mailbox for receiving
 * ingress messages
 * @homa_conn: an opaque pointer to a homa socket in Shenango
 * @homa_mb_deliver: callback to invoke when ingress messages arrive
 *
 * Returns a handle to the mailbox created.
 */
extern homa_mailbox homa_mailbox_create(void* homa_conn,
        void (*homa_mb_deliver) (void*, homa_inmsg, uint32_t, uint16_t));

/**
 * homa_mailbox_free - frees a mailbox created by @homa_mailbox_create()
 * @mb: the mailbox to free
 */
extern void homa_mailbox_free(homa_mailbox mb);

/* ============================ */
/*     Homa::InMessage API      */
/* ============================ */

/**
 * homa_inmsg_ack - C-binding for Homa::InMessage::acknowledge
 */
extern void homa_inmsg_ack(homa_inmsg in_msg);

/**
 * homa_inmsg_dropped - C-binding for Homa::InMessage::dropped
 */
extern bool homa_inmsg_dropped(homa_inmsg in_msg);

/**
 * homa_inmsg_fail - C-binding for Homa::InMessage::fail
 */
extern void homa_inmsg_fail(homa_inmsg in_msg);

/**
 * homa_inmsg_get - C-binding for Homa::InMessage::get
 */
extern size_t homa_inmsg_get(homa_inmsg in_msg, size_t ofs, void* dst,
                             size_t len);

/**
 * homa_inmsg_len - C-binding for Homa::InMessage::length
 */
extern size_t homa_inmsg_len(homa_inmsg in_msg);

/**
 * homa_inmsg_release - C-binding for Homa::InMessage::release
 */
extern void homa_inmsg_release(homa_inmsg in_msg);

/**
 * homa_inmsg_strip - C-binding for Homa::InMessage::strip
 */
extern void homa_inmsg_strip(homa_inmsg in_msg, size_t n);


/* ============================ */
/*     Homa::OutMessage API     */
/* ============================ */

/**
 * homa_outmsg_append - C-binding for Homa::OutMessage::append
 */
extern void homa_outmsg_append(homa_outmsg out_msg, const void* buf,
                               size_t len);

/**
 * homa_outmsg_cancel - C-binding for Homa::OutMessage::cancel
 */
extern void homa_outmsg_cancel(homa_outmsg out_msg);

/**
 * homa_outmsg_status - C-binding for Homa::OutMessage::getStatus
 */
extern int homa_outmsg_status(homa_outmsg out_msg);

/**
 * homa_outmsg_prepend - C-binding for Homa::OutMessage::prepend
 */
extern void homa_outmsg_prepend(homa_outmsg out_msg, const void* buf,
                                size_t len);

/**
 * homa_outmsg_reserve - C-binding for Homa::OutMessage::reserve
 */
extern void homa_outmsg_reserve(homa_outmsg out_msg, size_t n);

/**
 * homa_outmsg_send - C-binding for Homa::OutMessage::send
 */
extern void homa_outmsg_send(homa_outmsg out_msg, uint32_t ip, uint16_t port);

/**
 * homa_outmsg_register_cb - C-binding for Homa::OutMessage::registerCallback
 */
extern void homa_outmsg_register_cb(homa_outmsg out_msg, void (*cb) (void*),
                                    void *data);

/**
 * homa_outmsg_release - C-binding for Homa::OutMessage::release
 */
extern void homa_outmsg_release(homa_outmsg out_msg);

/* ============================ */
/*     Homa::Transport API      */
/* ============================ */

/**
 * homa_trans_create - C-binding for Homa::Transport::create
 */
extern homa_trans homa_trans_create(homa_driver drv, uint64_t id);

/**
 * homa_trans_alloc - C-binding for Homa::Transport::alloc
 */
extern homa_outmsg homa_trans_alloc(homa_trans trans, uint16_t sport);

/**
 * homa_trans_check_timeouts - C-binding for Homa::Transport::checkTimeouts
 */
extern uint64_t homa_trans_check_timeouts(homa_trans trans);

/**
 * homa_trans_id - C-binding for Homa::Transport::getId
 */
extern uint64_t homa_trans_id(homa_trans trans);

/**
 * homa_trans_proc - C-binding for Homa::Transport::processPacket
 */
extern void homa_trans_proc(homa_trans trans, void* mbuf, uint32_t src_ip,
                            homa_mailbox mailbox);

#ifdef __cplusplus
}
#endif