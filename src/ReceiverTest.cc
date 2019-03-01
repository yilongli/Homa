/* Copyright (c) 2018-2019, Stanford University
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

#include <gtest/gtest.h>

#include "Receiver.h"

#include "MockDriver.h"
#include "OpContext.h"

#include <Homa/Debug.h>

#include <mutex>

namespace Homa {
namespace Core {
namespace {

using ::testing::Eq;
using ::testing::Matcher;
using ::testing::Mock;
using ::testing::NiceMock;
using ::testing::Pointee;
using ::testing::Return;

class ReceiverTest : public ::testing::Test {
  public:
    ReceiverTest()
        : mockDriver()
        , mockPacket(&payload)
        , scheduler()
        , receiver()
        , savedLogPolicy(Debug::getLogPolicy())
    {
        ON_CALL(mockDriver, getBandwidth).WillByDefault(Return(8000));
        ON_CALL(mockDriver, getMaxPayloadSize).WillByDefault(Return(1028));
        Debug::setLogPolicy(
            Debug::logPolicyFromString("src/ObjectPool@SILENT"));
        opContextPool = new OpContextPool();
        scheduler = new Scheduler(&mockDriver);
        receiver = new Receiver(scheduler);
    }

    ~ReceiverTest()
    {
        delete receiver;
        delete scheduler;
        delete opContextPool;
        Debug::setLogPolicy(savedLogPolicy);
    }

    NiceMock<MockDriver> mockDriver;
    NiceMock<MockDriver::MockPacket> mockPacket;
    char payload[1028];
    OpContextPool* opContextPool;
    Scheduler* scheduler;
    Receiver* receiver;
    std::vector<std::pair<std::string, std::string>> savedLogPolicy;
};

TEST_F(ReceiverTest, handleDataPacket)
{
    OpContext* op = opContextPool->construct();

    EXPECT_FALSE(op->inMessage);

    // receive packet 1
    Protocol::DataHeader* header =
        static_cast<Protocol::DataHeader*>(mockPacket.payload);
    header->common.messageId = {42, 1};
    header->index = 1;
    header->totalLength = 1420;
    std::string addressStr("remote-location");
    MockDriver::MockAddress mockAddress;
    mockPacket.address = &mockAddress;

    EXPECT_CALL(mockDriver, releasePackets(Pointee(&mockPacket), Eq(1)))
        .Times(0);
    EXPECT_CALL(mockAddress, toString)
        .Times(3)
        .WillRepeatedly(Return(addressStr))
        .RetiresOnSaturation();
    EXPECT_CALL(mockDriver,
                getAddress(Matcher<std::string const*>(Pointee(addressStr))))
        .WillOnce(Return(&mockAddress));
    char grantPayload[1028];
    MockDriver::MockPacket grantPacket(grantPayload);
    EXPECT_CALL(mockDriver, allocPacket).WillOnce(Return(&grantPacket));
    EXPECT_CALL(mockDriver, releasePackets(Pointee(&grantPacket), Eq(1)))
        .Times(1);

    receiver->handleDataPacket(op, &mockPacket, &mockDriver);

    EXPECT_TRUE(op->inMessage);
    Receiver::InboundMessage* message = op->inMessage.get();
    EXPECT_EQ(&mockAddress, message->address);
    EXPECT_EQ(1420U, message->messageLength);
    EXPECT_TRUE(message->occupied.test(1));
    EXPECT_EQ(1U, message->getNumPackets());
    EXPECT_EQ(1000U, message->PACKET_DATA_LENGTH);
    Protocol::GrantHeader* grantHeader =
        static_cast<Protocol::GrantHeader*>(grantPacket.payload);
    EXPECT_EQ(header->common.messageId, grantHeader->common.messageId);
    EXPECT_EQ(6000U, grantHeader->offset);
    EXPECT_EQ(sizeof(Protocol::GrantHeader), grantPacket.length);
    EXPECT_EQ(&mockAddress, grantPacket.address);
    EXPECT_FALSE(message->isReady());

    Mock::VerifyAndClearExpectations(&mockDriver);
    Mock::VerifyAndClearExpectations(&mockAddress);

    // receive packet 1 again; duplicate packet

    EXPECT_CALL(mockDriver, releasePackets(Pointee(&mockPacket), Eq(1)))
        .Times(1);
    EXPECT_CALL(mockDriver,
                getAddress(Matcher<std::string const*>(Pointee(addressStr))))
        .Times(0);
    EXPECT_CALL(mockAddress, toString)
        .Times(2)
        .WillRepeatedly(Return(addressStr))
        .RetiresOnSaturation();
    EXPECT_CALL(mockDriver, allocPacket).Times(0);
    EXPECT_CALL(mockDriver, releasePackets(Pointee(&grantPacket), Eq(1)))
        .Times(0);

    receiver->handleDataPacket(op, &mockPacket, &mockDriver);

    EXPECT_TRUE(message->occupied.test(1));
    EXPECT_EQ(1U, message->getNumPackets());
    EXPECT_EQ(1000U, message->PACKET_DATA_LENGTH);
    EXPECT_FALSE(message->isReady());

    Mock::VerifyAndClearExpectations(&mockDriver);
    Mock::VerifyAndClearExpectations(&mockAddress);

    // receive packet 0; complete the message
    header->index = 0;

    EXPECT_CALL(mockDriver, releasePackets(Pointee(&mockPacket), Eq(1)))
        .Times(0);
    EXPECT_CALL(mockDriver,
                getAddress(Matcher<std::string const*>(Pointee(addressStr))))
        .Times(0);
    EXPECT_CALL(mockAddress, toString)
        .Times(2)
        .WillRepeatedly(Return(addressStr))
        .RetiresOnSaturation();
    EXPECT_CALL(mockDriver, allocPacket).WillOnce(Return(&grantPacket));
    EXPECT_CALL(mockDriver, releasePackets(Pointee(&grantPacket), Eq(1)))
        .Times(1);

    receiver->handleDataPacket(op, &mockPacket, &mockDriver);

    EXPECT_TRUE(message->occupied.test(0));
    EXPECT_EQ(2U, message->getNumPackets());
    EXPECT_EQ(1000U, message->PACKET_DATA_LENGTH);
    EXPECT_EQ(header->common.messageId, grantHeader->common.messageId);
    EXPECT_EQ(7000U, grantHeader->offset);
    EXPECT_EQ(sizeof(Protocol::GrantHeader), grantPacket.length);
    EXPECT_EQ(&mockAddress, grantPacket.address);
    EXPECT_TRUE(message->isReady());

    Mock::VerifyAndClearExpectations(&mockDriver);
    Mock::VerifyAndClearExpectations(&mockAddress);

    // receive packet 0 again on a complete message
    EXPECT_CALL(mockDriver, releasePackets(Pointee(&mockPacket), Eq(1)))
        .Times(1);
    EXPECT_CALL(mockDriver,
                getAddress(Matcher<std::string const*>(Pointee(addressStr))))
        .Times(0);
    EXPECT_CALL(mockAddress, toString).Times(0);
    EXPECT_CALL(mockDriver, allocPacket).Times(0);
    EXPECT_CALL(mockDriver, releasePackets(Pointee(&grantPacket), Eq(1)))
        .Times(0);
    EXPECT_TRUE(message->isReady());

    receiver->handleDataPacket(op, &mockPacket, &mockDriver);

    Mock::VerifyAndClearExpectations(&mockDriver);
}

}  // namespace
}  // namespace Core
}  // namespace Homa
