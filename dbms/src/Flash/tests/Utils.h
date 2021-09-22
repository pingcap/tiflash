#pragma once

#include <Common/MPMCQueue.h>
#include <kvproto/mpp.pb.h>
#include <memory>

namespace DB::tests
{
using Packet = mpp::MPPDataPacket;
using PacketPtr = std::shared_ptr<Packet>;
using PacketQueue = MPMCQueue<PacketPtr>;
using PacketQueuePtr = std::shared_ptr<PacketQueue>;
} // DB::tests

