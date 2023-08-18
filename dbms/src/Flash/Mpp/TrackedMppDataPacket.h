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

#include <Common/Exception.h>
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
#include <tipb/select.pb.h>
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

struct MemTrackerWrapper
{
    MemTrackerWrapper(size_t _size, MemoryTracker * memory_tracker_)
        : memory_tracker(memory_tracker_)
        , size(0)
    {
        alloc(_size);
    }

    explicit MemTrackerWrapper(MemoryTracker * memory_tracker_)
        : memory_tracker(memory_tracker_)
        , size(0)
    {}

    void alloc(size_t delta)
    {
        if (delta)
        {
            if likely (memory_tracker)
            {
                memory_tracker->alloc(delta);
                size += delta;
            }
        }
    }

    void free(size_t delta)
    {
        if (delta)
        {
            if likely (memory_tracker)
            {
                memory_tracker->free(delta);
                size -= delta;
            }
        }
    }

    void switchMemTracker(MemoryTracker * new_memory_tracker)
    {
        if (new_memory_tracker != memory_tracker)
        {
            int bak_size = size;
            freeAll();
            memory_tracker = new_memory_tracker;
            alloc(bak_size);
        }
    }
    ~MemTrackerWrapper() { freeAll(); }

    void freeAll() { free(size); }

    MemoryTracker * memory_tracker;
    size_t size = 0;
};

struct TrackedMppDataPacket
{
    TrackedMppDataPacket(const mpp::MPPDataPacket & data, MemoryTracker * memory_tracker)
        : mem_tracker_wrapper(estimateAllocatedSize(data), memory_tracker)
    {
        packet = data;
    }

    explicit TrackedMppDataPacket(int64_t version)
        : mem_tracker_wrapper(current_memory_tracker)
    {
        packet.set_version(version);
    }

    explicit TrackedMppDataPacket(MemoryTracker * memory_tracker, int64_t version)
        : mem_tracker_wrapper(memory_tracker)
    {
        packet.set_version(version);
    }

    TrackedMppDataPacket(const mpp::MPPDataPacket & data, size_t size, MemoryTracker * memory_tracker)
        : mem_tracker_wrapper(size, memory_tracker)
        , packet(data)
    {}

    void addChunk(std::string && value)
    {
        mem_tracker_wrapper.alloc(value.size());
        packet.add_chunks(std::move(value));
    }

    void serializeByResponse(const tipb::SelectResponse & response)
    {
        mem_tracker_wrapper.alloc(response.ByteSizeLong());
        if (!response.SerializeToString(packet.mutable_data()))
        {
            mem_tracker_wrapper.free(response.ByteSizeLong());
            throw Exception(fmt::format("Fail to serialize response, response size: {}", response.ByteSizeLong()));
        }
    }

    void read(const std::unique_ptr<::grpc::ClientAsyncReader<::mpp::MPPDataPacket>> & reader, void * callback)
    {
        reader->Read(&packet, callback);
        need_recompute = true;
        //we shouldn't update tracker now, since it's an async reader!!
    }

    // we need recompute in some cases we can't update memory counter timely, such as async read
    void recomputeTrackedMem()
    {
        if (need_recompute)
        {
            try
            {
                mem_tracker_wrapper.freeAll();
                mem_tracker_wrapper.alloc(estimateAllocatedSize(packet));
                need_recompute = false;
            }
            catch (...)
            {
                error_message = getCurrentExceptionMessage(false);
            }
        }
    }

    bool read(const std::unique_ptr<::grpc::ClientReader<::mpp::MPPDataPacket>> & reader)
    {
        bool ret = reader->Read(&packet);
        mem_tracker_wrapper.freeAll();
        mem_tracker_wrapper.alloc(estimateAllocatedSize(packet));
        return ret;
    }

    void switchMemTracker(MemoryTracker * new_memory_tracker)
    {
        mem_tracker_wrapper.switchMemTracker(new_memory_tracker);
    }

    bool hasError() const { return !error_message.empty() || packet.has_error(); }

    const String & error() const { return error_message.empty() ? packet.error().msg() : error_message; }

    mpp::MPPDataPacket & getPacket() { return packet; }

    std::shared_ptr<DB::TrackedMppDataPacket> copy() const
    {
        return std::make_shared<TrackedMppDataPacket>(
            packet,
            mem_tracker_wrapper.size,
            mem_tracker_wrapper.memory_tracker);
    }

    MemTrackerWrapper mem_tracker_wrapper;
    mpp::MPPDataPacket packet;
    bool need_recompute = false;
    String error_message;
};
using TrackedMppDataPacketPtr = std::shared_ptr<DB::TrackedMppDataPacket>;
using TrackedMppDataPacketPtrs = std::vector<TrackedMppDataPacketPtr>;

struct TrackedSelectResp
{
    explicit TrackedSelectResp()
        : memory_tracker(current_memory_tracker)
    {}

    void addChunk(std::string && value)
    {
        memory_tracker.alloc(value.size());
        auto * dag_chunk = response.add_chunks();
        dag_chunk->set_rows_data(std::move(value));
    }

    tipb::SelectResponse & getResponse() { return response; }

    void setEncodeType(::tipb::EncodeType value) { response.set_encode_type(value); }

    tipb::ExecutorExecutionSummary * addExecutionSummary() { return response.add_execution_summaries(); }

    MemTrackerWrapper memory_tracker;
    tipb::SelectResponse response;
};

} // namespace DB
