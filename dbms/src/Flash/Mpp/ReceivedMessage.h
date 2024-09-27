// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/FailPoint.h>
#include <Common/LooseBoundedMPMCQueue.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>

#include <memory>

namespace DB
{
class ReceivedMessage
{
    size_t source_index;
    String req_info;
    // shared_ptr<const MPPDataPacket> is copied to make sure error_ptr, resp_ptr and chunks are valid.
    const std::shared_ptr<DB::TrackedMppDataPacket> packet;
    const mpp::Error * error_ptr;
    const String * resp_ptr;
    std::vector<const String *> chunks;
    /// used for fine grained shuffle, remaining_consumers will be nullptr for non fine grained shuffle
    std::vector<std::vector<const String *>> fine_grained_chunks;
    std::atomic<size_t> remaining_consumers;
    bool fine_grained_consumer_size;

public:
    // Constructor that move chunks.
    ReceivedMessage(
        size_t source_index_,
        const String & req_info_,
        const std::shared_ptr<DB::TrackedMppDataPacket> & packet_,
        const mpp::Error * error_ptr_,
        const String * resp_ptr_,
        std::vector<const String *> && chunks_,
        size_t fine_grained_consumer_size);

    size_t getSourceIndex() const { return source_index; }
    const String & getReqInfo() const { return req_info; }
    const mpp::Error * getErrorPtr() const { return error_ptr; }
    const String * getRespPtr(size_t stream_id) const { return stream_id == 0 ? resp_ptr : nullptr; }
    std::atomic<size_t> & getRemainingConsumers() { return remaining_consumers; }
    const std::vector<const String *> & getChunks(size_t stream_id) const;
    const mpp::MPPDataPacket & getPacket() const { return packet->packet; }
    bool containUsefulMessage() const;
};
} // namespace DB
