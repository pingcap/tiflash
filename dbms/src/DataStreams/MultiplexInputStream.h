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

#include <Common/MPMCQueue.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/ParallelInputsProcessor.h>
#include <Storages/DeltaMerge/DMSegmentThreadInputStream.h>

#include "Storages/DeltaMerge/DMSegmentThreadInputStream.h"


//#include "Storages/DeltaMerge/DMSegmentThreadInputStream.h"

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


namespace MultiplexDMSegInputStreamImpl
{
//template <StreamUnionMode mode>
//struct OutputData;

/// A block or an exception.
struct OutputData
{
    Block block;
    std::exception_ptr exception;

    OutputData() = default;
    explicit OutputData(Block & block_)
        : block(block_)
    {}
    explicit OutputData(const std::exception_ptr & exception_)
        : exception(exception_)
    {}
};

} // namespace MultiplexDMSegInputStreamImpl

//class DMSegmentThreadInputStream;


struct DMSegThdInputStreamPool
{
    std::vector<std::shared_ptr<std::queue<std::shared_ptr<DM::DMSegmentThreadInputStream>>>> streams_queue_by_partition;
    //    std::list<std::queue<std::shared_ptr<DM::DMSegmentThreadInputStream>>>::iterator a = streams_queue_by_partition.begin();
    int streams_queue_id = 0;
    std::mutex mu;
    std::shared_ptr<DM::DMSegmentThreadInputStream> pickOne()
    {
        std::unique_lock lk(mu);
        if (streams_queue_by_partition.empty())
            return nullptr;
        if (streams_queue_id >= static_cast<int>(streams_queue_by_partition.size()))
            streams_queue_id = 0;

        auto & q_ptr = streams_queue_by_partition[streams_queue_id];
        auto & q = *q_ptr;
        std::shared_ptr<DM::DMSegmentThreadInputStream> ret = nullptr;
        assert(!q.empty());
        ret = q.front();
        q.pop();
        if (q.empty())
        {
            streams_queue_id = removeQueue(streams_queue_id);
        }
        return ret;
    }

    int removeQueue(int queue_id)
    {
        streams_queue_by_partition[queue_id] = nullptr;
        if (queue_id != static_cast<int>(streams_queue_by_partition.size()) - 1)
        {
            swap(streams_queue_by_partition[queue_id], streams_queue_by_partition.back());
        }
        streams_queue_by_partition.pop_back();
        return nextQueueId(queue_id);
    }

    int nextQueueId(int queue_id)
    {
        if (queue_id + 1 >= static_cast<int>(streams_queue_by_partition.size()))
        {
            return 0;
        }
        else
        {
            return queue_id + 1;
        }
    }

    static void swap(std::shared_ptr<std::queue<std::shared_ptr<DM::DMSegmentThreadInputStream>>> & a, std::shared_ptr<std::queue<std::shared_ptr<DM::DMSegmentThreadInputStream>>> & b)
    {
        auto tmp = a;
        a = b;
        b = tmp;
    }
};

class MultiplexDMSegInputStream final : public IProfilingBlockInputStream
{
public:
    using ExceptionCallback = std::function<void()>;

private:
    static constexpr auto NAME = "MultiplexDMSeg";

public:
    MultiplexDMSegInputStream(
        BlockInputStreams inputs,
        std::shared_ptr<DMSegThdInputStreamPool> & shared_pool,
        const String & req_id)
        : log(Logger::get(NAME, req_id))
        , shared_pool(shared_pool)
    {
        // TODO: assert capacity of output_queue is not less than processor.getMaxThreads()
        children = inputs;

        size_t num_children = children.size();
        if (num_children > 1)
        {
            Block header = children.at(0)->getHeader();
            for (size_t i = 1; i < num_children; ++i)
                assertBlocksHaveEqualStructure(children[i]->getHeader(), header, "MULTIPLEX_DMSEG");
        }
    }

    String getName() const override { return NAME; }

    ~MultiplexDMSegInputStream() override
    {
        try
        {
            if (!all_read)
                cancel(false);

            //            finalize();
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }

    /** Different from the default implementation by trying to stop all sources,
      * skipping failed by execution.
      */
    void cancel(bool kill) override
    {
        if (kill)
            is_killed = true;

        bool old_val = false;
        if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
            return;

        if (cur_stream)
        {
            cur_stream->cancel(kill);
        }
        //        processor.cancel(kill);
    }

    BlockExtraInfo getBlockExtraInfo() const override
    {
        throw Exception(
            "Method getBlockExtraInfo is not supported for mode StreamUnionMode::Basic",
            ErrorCodes::NOT_IMPLEMENTED);
    }

    Block getHeader() const override { return children.at(0)->getHeader(); }


protected:
    /// Do nothing, to make the preparation for the query execution in parallel, in ParallelInputsProcessor.
    void readPrefix() override
    {
    }

    /** The following options are possible:
      * 1. `readImpl` function is called until it returns an empty block.
      *  Then `readSuffix` function is called and then destructor.
      * 2. `readImpl` function is called. At some point, `cancel` function is called perhaps from another thread.
      *  Then `readSuffix` function is called and then destructor.
      * 3. At any time, the object can be destroyed (destructor called).
      */

    Block readImpl() override
    {
        if (all_read)
            return {};
        if (!cur_stream)
        {
            cur_stream = shared_pool->pickOne();
            if (!cur_stream)
            { // shared_pool is empty
                all_read = true;
                return {};
            }
        }

        Block ret;
        while (!(ret = cur_stream->read()))
        {
            cur_stream->readSuffix(); // release old inputstream
            cur_stream = shared_pool->pickOne();
            if (!cur_stream)
            { // shared_pool is empty
                all_read = true;
                return {};
            }
            cur_stream->readPrefix();
        }
        return ret;
    }

    /// Called either after everything is read, or after cancel.
    void readSuffix() override
    {
        if (!all_read && !is_cancelled)
            throw Exception("readSuffix called before all data is read", ErrorCodes::LOGICAL_ERROR);

        if (cur_stream)
        {
            cur_stream->readSuffix();
            cur_stream = nullptr;
        }
    }

private:
    LoggerPtr log;

    std::shared_ptr<DMSegThdInputStreamPool> shared_pool;
    std::shared_ptr<DM::DMSegmentThreadInputStream> cur_stream;

    bool all_read = false;
};

} // namespace DB
