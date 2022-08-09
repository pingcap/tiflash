// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Logger.h>
#include <common/logger_useful.h>
#include <common/types.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <Common/MemoryTracker.h>
#include <grpcpp/server_context.h>
#include <kvproto/mpp.pb.h>
#include <kvproto/tikvpb.grpc.pb.h>
#pragma GCC diagnostic pop
#include <memory>

namespace DB
{
inline size_t estimateAllocatedSize(const mpp::MPPDataPacket & data)
{
    size_t ret = data.data().size();
    for (int i = 0; i < data.chunks_size(); i++)
    {
        ret += data.chunks(i).size();
    }
    return ret;
}


struct TrackedMppDataPacket
{
    explicit TrackedMppDataPacket(const mpp::MPPDataPacket & data, MemoryTracker * memory_tracker)
        : memory_tracker(memory_tracker)
    {
        size = estimateAllocatedSize(data);
        trackAlloc();
        packet = std::make_shared<mpp::MPPDataPacket>(data);
    }

    explicit TrackedMppDataPacket(const std::shared_ptr<mpp::MPPDataPacket> & packet_, MemoryTracker * memory_tracker)
        : memory_tracker(memory_tracker)
    {
        size = estimateAllocatedSize(*packet_);
        trackAlloc();
        packet = packet_;
    }

    void trackAlloc();

    void trackFree() const;

    ~TrackedMppDataPacket()
    {
        trackFree();
    }

    MemoryTracker * memory_tracker = nullptr;
    int size;
    std::shared_ptr<mpp::MPPDataPacket> packet;
};

struct TmpMemTracker
{
    TmpMemTracker(size_t size)
        : size(size)
    {
        if (current_memory_tracker)
            current_memory_tracker->alloc(size);
    }
    void alloc(size_t delta)
    {
        if (current_memory_tracker)
        {
            current_memory_tracker->alloc(delta);
            size += delta;
        }
    }
    ~TmpMemTracker()
    {
        if (current_memory_tracker)
            current_memory_tracker->free(size);
    }
    size_t size;
};
} // namespace DB
