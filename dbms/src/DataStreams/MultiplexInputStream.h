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

#include <DataStreams/IProfilingBlockInputStream.h>

#include <memory>
#include <queue>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

class MultiPartitionStreamPool
{
public:
    MultiPartitionStreamPool() = default;

    void addPartitionStreams(const BlockInputStreams & cur_streams)
    {
        if (cur_streams.empty())
            return;
        std::unique_lock lk(mu);
        streams_queue_by_partition.push_back(
            std::make_shared<std::queue<std::shared_ptr<IBlockInputStream>>>());
        for (const auto & stream : cur_streams)
            streams_queue_by_partition.back()->push(stream);
        added_streams.insert(added_streams.end(), cur_streams.begin(), cur_streams.end());
    }

    std::shared_ptr<IBlockInputStream> pickOne()
    {
        std::unique_lock lk(mu);
        if (streams_queue_by_partition.empty())
            return nullptr;
        if (streams_queue_id >= static_cast<int>(streams_queue_by_partition.size()))
            streams_queue_id = 0;

        auto & q = *streams_queue_by_partition[streams_queue_id];
        std::shared_ptr<IBlockInputStream> ret = nullptr;
        assert(!q.empty());
        ret = q.front();
        q.pop();
        if (q.empty())
            streams_queue_id = removeQueue(streams_queue_id);
        else
            streams_queue_id = nextQueueId(streams_queue_id);
        return ret;
    }

    int exportAddedStreams(BlockInputStreams & ret_streams)
    {
        std::unique_lock lk(mu);
        for (auto & stream : added_streams)
            ret_streams.push_back(stream);
        return added_streams.size();
    }

    int addedStreamsCnt()
    {
        std::unique_lock lk(mu);
        return added_streams.size();
    }

private:
    int removeQueue(int queue_id)
    {
        streams_queue_by_partition[queue_id] = nullptr;
        if (queue_id != static_cast<int>(streams_queue_by_partition.size()) - 1)
        {
            swap(streams_queue_by_partition[queue_id], streams_queue_by_partition.back());
            streams_queue_by_partition.pop_back();
            return queue_id;
        }
        else
        {
            streams_queue_by_partition.pop_back();
            return 0;
        }
    }

    int nextQueueId(int queue_id) const
    {
        if (queue_id + 1 < static_cast<int>(streams_queue_by_partition.size()))
            return queue_id + 1;
        else
            return 0;
    }

    static void swap(std::shared_ptr<std::queue<std::shared_ptr<IBlockInputStream>>> & a,
                     std::shared_ptr<std::queue<std::shared_ptr<IBlockInputStream>>> & b)
    {
        a.swap(b);
    }

    std::vector<
        std::shared_ptr<std::queue<
            std::shared_ptr<IBlockInputStream>>>>
        streams_queue_by_partition;
    std::vector<std::shared_ptr<IBlockInputStream>> added_streams;
    int streams_queue_id = 0;
    std::mutex mu;
};

class MultiplexInputStream final : public IProfilingBlockInputStream
{
private:
    static constexpr auto NAME = "Multiplex";

public:
    MultiplexInputStream(
        std::shared_ptr<MultiPartitionStreamPool> & shared_pool,
        const String & req_id)
        : log(Logger::get(req_id))
        , shared_pool(shared_pool)
    {
        shared_pool->exportAddedStreams(children);
        size_t num_children = children.size();
        if (num_children > 1)
        {
            Block header = children.at(0)->getHeader();
            for (size_t i = 1; i < num_children; ++i)
                assertBlocksHaveEqualStructure(
                    children[i]->getHeader(),
                    header,
                    "MULTIPLEX");
        }
    }

    String getName() const override { return NAME; }

    ~MultiplexInputStream() override
    {
        try
        {
            if (!all_read)
                cancel(false);
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
        if (!is_cancelled.compare_exchange_strong(
                old_val,
                true,
                std::memory_order_seq_cst,
                std::memory_order_relaxed))
            return;

        if (cur_stream)
        {
            if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*cur_stream))
            {
                child->cancel(kill);
            }
        }
    }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    /// Do nothing, to make the preparation when underlying InputStream is picked from the pool
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

        Block ret;
        while (!cur_stream || !(ret = cur_stream->read()))
        {
            if (cur_stream)
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

    std::shared_ptr<MultiPartitionStreamPool> shared_pool;
    std::shared_ptr<IBlockInputStream> cur_stream;

    bool all_read = false;
};

} // namespace DB
