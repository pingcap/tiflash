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

#include "Common/Logger.h"
#include "common/logger_useful.h"
#include "common/types.h"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include "Common/MemoryTracker.h"
#include "grpcpp/server_context.h"
#include "kvproto/mpp.pb.h"
#include "kvproto/tikvpb.grpc.pb.h"
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
    TrackedMppDataPacket(const mpp::MPPDataPacket & data)
    {
        size = estimateAllocatedSize(data);
        alloc();
        //        if (size)
        //        {
        //            CurrentMemoryTracker::alloc(size);
        //        }
        packet = std::make_shared<mpp::MPPDataPacket>(data);
    }

    TrackedMppDataPacket(const std::shared_ptr<mpp::MPPDataPacket> &packet_)
    {
        size = estimateAllocatedSize(*packet_);
        alloc();
        //        if (size)
        //        {
        //            CurrentMemoryTracker::alloc(size);
        //        }
        packet = packet_;
    }

    TrackedMppDataPacket()
        : size(0)
        , packet(std::make_shared<mpp::MPPDataPacket>())
    {}


    void setPacket(const std::shared_ptr<mpp::MPPDataPacket> & new_packet)
    {
        free();
        //        if (size)
        //        {
        //            CurrentMemoryTracker::free(size);
        //        }
        size = estimateAllocatedSize(*new_packet);
        alloc();
        //        if (size)
        //        {
        //            CurrentMemoryTracker::alloc(size);
        //        }
        packet = new_packet;
    }

    void alloc()
    {
        if (size)
        {
            try
            {
                CurrentMemoryTracker::alloc(size);
            }
            catch (...)
            {
                has_err = true;
                std::rethrow_exception(std::current_exception());
            }
        }
    }

    void free()
    {
        if (size && !has_err)
            CurrentMemoryTracker::free(size);
    }

    ~TrackedMppDataPacket()
    {
        if (size)
            CurrentMemoryTracker::free(size);
    }

    int size;
    bool has_err = false;
    std::shared_ptr<mpp::MPPDataPacket> packet;
};
} // namespace DB