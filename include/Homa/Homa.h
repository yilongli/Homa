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

/**
 * @file Homa/Homa.h
 *
 * Contains the Homa Transport API.  Applications of the Homa Transport library
 * should include this header.
 */

#ifndef HOMA_INCLUDE_HOMA_HOMA_H
#define HOMA_INCLUDE_HOMA_HOMA_H

#include <Homa/Driver.h>

namespace Homa {

/**
 * Shorthand for an std::unique_ptr with a customized deleter.
 *
 * This is used to implement the "borrow" semantics for interface classes like
 * InMessage, OutMessage, and Socket; that is, a user can obtain pointers to
 * these objects and use them to make function calls, but the user must always
 * return the objects back to the transport library eventually because the user
 * has no idea how to destruct the objects or reclaim memory.
 */
template <typename T>
using unique_ptr = std::unique_ptr<T, typename T::Deleter>;

/**
 * Represents a socket address to (from) which we can send (receive) messages.
 */
struct SocketAddress {
    /// IPv4 address in host byte order.
    IpAddress ip;

    /// Port number in host byte order.
    uint16_t port;
};

/**
 * Represents an array of bytes that has been received over the network.
 *
 * This class is NOT thread-safe.
 */
class InMessage {
  public:
    /**
     * Custom deleter for use with std::unique_ptr.
     */
    struct Deleter {
        void operator()(InMessage* message)
        {
            message->release();
        }
    };

    /**
     * Inform the sender that this message has been processed successfully.
     */
    virtual void acknowledge() const = 0;

    /**
     * Returns true if the sender is no longer waiting for this message to be
     * processed; false otherwise.
     */
    virtual bool dropped() const = 0;

    /**
     * Inform the sender that this message has failed to be processed.
     */
    virtual void fail() const = 0;

    /**
     * Get the contents of a specified range of bytes in the Message by
     * copying them into the provided destination memory region.
     *
     * @param offset
     *      The number of bytes in the Message preceding the range of bytes
     *      being requested.
     * @param destination
     *      The pointer to the memory region into which the requested byte
     *      range will be copied. The caller must ensure that the buffer is
     *      big enough to hold the requested number of bytes.
     * @param count
     *      The number of bytes being requested.
     *
     * @return
     *      The number of bytes actually copied out. This number may be less
     *      than "num" if the requested byte range exceeds the range of
     *      bytes in the Message.
     */
    virtual size_t get(size_t offset, void* destination,
                       size_t count) const = 0;

    /**
     * Return the remote address from which this Message is sent.
     */
    virtual SocketAddress getSourceAddress() const = 0;

    /**
     * Return the number of bytes this Message contains.
     */
    virtual size_t length() const = 0;

    /**
     * Remove a number of bytes from the beginning of the Message.
     *
     * @param count
     *      Number of bytes to remove.
     */
    virtual void strip(size_t count) = 0;

  protected:
    /**
     * Use protected destructor to prevent users from calling delete on pointers
     * to this interface.
     */
    ~InMessage() = default;

    /**
     * Signal that this message is no longer needed.  The caller should not
     * access this message following this call.
     */
    virtual void release() = 0;
};

/**
 * Represents an array of bytes that can be sent over the network.
 *
 * This class is NOT thread-safe.
 */
class OutMessage {
  public:
    /**
     * Defines the possible states of an OutMessage.
     */
    using Status = OutMessageStatus;

    /**
     * Custom deleter for use with std::unique_ptr.
     */
    struct Deleter {
        void operator()(OutMessage* message)
        {
            message->release();
        }
    };

    /**
     * Copy an array of bytes to the end of the Message.
     *
     * @param source
     *      Address of the first byte of data to be copied to the end of the
     *      Message.
     * @param count
     *      Number of bytes to be appended.
     */
    virtual void append(const void* source, size_t count) = 0;

    /**
     * Stop sending this message.
     */
    virtual void cancel() = 0;

    /**
     * Return the current state of this message.
     */
    virtual Status getStatus() const = 0;

    /**
     * Copy an array of bytes to the beginning of the Message.
     *
     * The number of bytes prepended must have been previously reserved;
     * otherwise, the behavior is undefined.
     *
     * @param source
     *      Address of the first byte of data (in a byte array) to be copied to
     *      the beginning of the Message.
     * @param num
     *      Number of bytes to be prepended.
     *
     * @sa Message::reserve()
     */
    virtual void prepend(const void* source, size_t count) = 0;

    /**
     * Register a callback function to be invoked when the status of this
     * message reaches the end states.
     *
     * @param func
     *      The callback function
     * @param data
     *      Argument to the callback function
     */
    virtual void registerCallback(void (*func) (void*), void* data) = 0;

    /**
     * Reserve a number of bytes at the beginning of the Message.
     *
     * The reserved space is used when bytes are prepended to the Message.
     * Sending a Message with unused reserved space will result in undefined
     * behavior.
     *
     * This method should be called before appending or prepending data to the
     * Message; otherwise, the behavior is undefined.
     *
     * @param count
     *      The number of bytes to be reserved.
     *
     * @sa Message::append(), Message::prepend()
     */
    virtual void reserve(size_t count) = 0;

    /**
     * Send this message to the destination.
     *
     * @param destination
     *      Network address to which this message will be sent.
     */
    virtual void send(SocketAddress destination) = 0;

  protected:
    /**
     * Use protected destructor to prevent users from calling delete on pointers
     * to this interface.
     */
    ~OutMessage() = default;

    /**
     * Signal that this message is no longer needed.  The caller should not
     * access this message following this call.
     */
    virtual void release() = 0;
};

/**
 * Represents a location which can hold incoming messages temporarily before
 * they are consumed by high-level applications.
 *
 * Despite a one-to-one relationship between Mailbox and Socket, this class
 * is pulled out of Socket to 1) provide a clean interface which allows users
 * to define their own mechanisms for message delivery (e.g., poll vs. push),
 * and 2) enable different lifetimes of a socket and its mailbox.
 *
 * In other words, methods in this class are NOT meant to be called by user
 * applications directly; instead, they are called by the Homa transport library
 * with user-defined implementations.
 *
 * This class is thread-safe.
 *
 * @sa MailboxDir
 */
class Mailbox {
  public:
    /**
     * Destructor.
     */
    virtual ~Mailbox() = default;

    /**
     * Signal that the caller will not access the mailbox after this call.
     * A mailbox will only be destroyed if it's removed from the directory
     * and closed by all openers.
     *
     * Not meant to be called by users.
     *
     * @sa MailboxDir::open()
     */
    virtual void close() = 0;

    /**
     * Used by a transport to deliver an ingress message to this mailbox.
     *
     * Not meant to be called by users.
     *
     * @param message
     *      An ingress message just completed by the transport.
     */
    virtual void deliver(InMessage* message) = 0;

    /**
     * Retrieve a message currently stored in the mailbox.
     *
     * Not meant to be called by users; use Socket::receive() instead.
     *
     * @param blocking
     *      When set to true, this method should not return until a message
     *      arrives or the corresponding socket is shut down.
     * @return
     *      A message previously delivered to this mailbox, if the mailbox is
     *      not empty; nullptr, otherwise.
     *
     * @sa Socket::receive()
     */
    virtual InMessage* retrieve(bool blocking) = 0;

    /**
     * Invoked when the corresponding socket of the mailbox is shut down.
     * All pending retrieve() requests must return immediately.
     */
    virtual void socketShutdown() = 0;
};

/**
 * Provides a means to keep track of the mailboxes that are currently in use
 * by Homa sockets.
 *
 * This class is separated out from Transport to allow users to apply their
 * own mechanisms (e.g., RCU) to provide synchronization and manage mailbox
 * lifetimes.
 *
 * Similar to Mailbox, methods in this class are NOT meant to be called by
 * user applications.
 *
 * This class is thread-safe.
 */
class MailboxDir {
  public:
    /**
     * Destructor.
     */
    virtual ~MailboxDir() = default;

    /**
     * Allocate a new mailbox in the directory.
     *
     * @param port
     *      Port number which identifies the mailbox.
     * @return
     *      Pointer to the new Mailbox on success; nullptr, if the port number
     *      is already in use.
     */
    virtual Mailbox* alloc(uint16_t port) = 0;

    /**
     * Find and open the mailbox that matches the given port number.  Once a
     * mailbox is opened, it's guaranteed to remain usable even if someone else
     * removes it from the directory.
     *
     * @param port
     *      Port number which identifies the mailbox.
     * @return
     *      Pointer to the opened mailbox on success; nullptr, if the desired
     *      mailbox doesn't exist.
     */
    virtual Mailbox* open(uint16_t port) = 0;

    // FIXME: move this into Mailbox? mailbox::remove()
    /**
     * Remove the mailbox that matches the given port number.
     *
     * @param port
     *      Port number of the mailbox that will be removed.
     * @return
     *      True on success; false, if the desired mailbox doesn't exist.
     */
    virtual bool remove(uint16_t port) = 0;
};

/**
 * Connection-less socket that can be used to send and receive Homa messages.
 *
 * This class is thread-safe.
 */
class Socket {
  public:
    using Address = SocketAddress;

    /**
     * Custom deleter for use with Homa::unique_ptr.
     */
    struct Deleter {
        void operator()(Socket* socket)
        {
            socket->close();
        }
    };

    /**
     * Allocate Message that can be sent with this Socket.
     *
     * @param sourcePort
     *      Port number of the socket from which the message will be sent.
     * @return
     *      A pointer to the allocated message or nullptr if the socket has
     *      been shut down.
     */
    virtual Homa::unique_ptr<Homa::OutMessage> alloc() = 0;

    /**
     * Check for and return a Message sent to this Socket if available.
     *
     * @param blocking
     *      When set to true, this method should not return until a message
     *      arrives or the socket is shut down.
     * @return
     *      Pointer to the received message, if any.  Otherwise, nullptr is
     *      returned if no message has been delivered or the socket has been
     *      shut down.
     */
    virtual Homa::unique_ptr<Homa::InMessage> receive(bool blocking) = 0;

    /**
     * Disable the socket.  Once a socket is shut down, all ongoing/subsequent
     * requests on the socket will return a failure.
     *
     * When multiple threads are working on a socket, this method can be used
     * to notify other threads to drop their references to this socket so that
     * the caller can safely close() the socket.
     */
    virtual void shutdown() = 0;

    /**
     * Check if the Socket has been shut down.
     */
    virtual bool isShutdown() const = 0;

    /**
     * Return the local IP address and port number of this Socket.
     */
    virtual Socket::Address getLocalAddress() const = 0;

  protected:
    /**
     * Use protected destructor to prevent users from calling delete on pointers
     * to this interface.
     */
    ~Socket() = default;

    /**
     * Signal that this Socket is no longer needed.  No one should access this
     * socket after this call.
     *
     * Note: outgoing messages already allocated from this socket will not be
     * affected.
     */
    virtual void close() = 0;
};

/**
 * Provides a means of communicating across the network using the Homa protocol.
 *
 * The transport is used to send and receive messages across the network using
 * the RemoteOp and ServerOp abstractions.  The execution of the transport is
 * driven through repeated calls to the Transport::poll() method; the transport
 * will not make any progress otherwise.
 *
 * This class is thread-safe.
 */
class Transport {
  public:
    /**
     * Custom deleter for use with std::unique_ptr.
     */
    struct Deleter {
        void operator()(Transport* transport)
        {
            transport->free();
        }
    };

    /**
     * Return a new instance of a Homa-based transport.
     *
     * @param driver
     *      Driver with which this transport should send and receive packets.
     * @param mailboxDir
     *      Mailbox directory with which this transport should decide where
     *      to deliver a message.
     * @param transportId
     *      This transport's unique identifier in the group of transports among
     *      which this transport will communicate.
     * @return
     *      Pointer to the new transport instance.
     */
    static Homa::unique_ptr<Transport> create(Driver* driver,
                                              MailboxDir* mailboxDir,
                                              uint64_t transportId);

    /**
     * Create a socket that can be used to send and receive messages.
     *
     * @param port
     *      The port number allocated to the socket.
     * @return
     *      Pointer to the new socket, if the port number is not in use;
     *      nullptr, otherwise.
     */
    virtual Homa::unique_ptr<Socket> open(uint16_t port) = 0;

    /**
     * Make incremental progress performing all Transport functionality.
     *
     * This method MUST be called for the Transport to make progress and should
     * be called frequently to ensure timely progress.
     */
    virtual void poll() = 0;

    /**
     * Return the driver that this transport uses to send and receive packets.
     */
    virtual Driver* getDriver() = 0;

    /**
     * Return this transport's unique identifier.
     */
    virtual uint64_t getId() = 0;

    // FIXME: the following (low-level) methods are used by Shenango which doesn't
    // use poll() to make progress
    // FIXME: get rid of poll() from the Transport interface? expose low-level
    // operations instead? i.e., processPacket, checkTimeouts, trySend, trySendGrant?

    /**
     * Process any timeouts that have expired.
     *
     * This method must be called periodically to ensure timely handling of
     * expired timeouts.
     *
     * @return
     *      The rdtsc cycle time when this method should be called again.
     */
    virtual uint64_t checkTimeouts() = 0;

    /**
     * Handle an ingress packet by running it through the transport protocol
     * stack.
     *
     * @param packet
     *      The ingress packet.
     * @param source
     *      IpAddress of the socket from which the packet is sent.
     */
    virtual void processPacket(Driver::Packet* packet, IpAddress source) = 0;

    // FIXME: return something?
    virtual void trySend() = 0;

    virtual void trySendGrants() = 0;

  protected:
    /**
     * Use protected destructor to prevent users from calling delete on pointers
     * to this interface.
     */
    ~Transport() = default;

    /**
     * Free this transport instance.  No one should not access this transport
     * following this call.
     */
    virtual void free() = 0;
};

}  // namespace Homa

#endif  // HOMA_INCLUDE_HOMA_HOMA_H
