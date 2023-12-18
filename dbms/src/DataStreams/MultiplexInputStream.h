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

    void cancel(bool kill)
    {
        std::deque<BlockInputStreamPtr> tmp_streams;
        {
            std::unique_lock lk(mu);
            if (is_cancelled)
                return;

            is_cancelled = true;
            tmp_streams.swap(added_streams);
        }

        for (auto & stream : tmp_streams)
            if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
            {
                p_stream->cancel(kill);
            }
    }

    void addPartitionStreams(const BlockInputStreams & cur_streams)
    {
        if (cur_streams.empty())
            return;
        std::unique_lock lk(mu);
        added_streams.insert(added_streams.end(), cur_streams.begin(), cur_streams.end());
    }

    BlockInputStreamPtr pickOne()
    {
        std::unique_lock lk(mu);
        if (added_streams.empty())
            return nullptr;

        auto ret = std::move(added_streams.front());
        added_streams.pop_front();
        return ret;
    }

    void exportAddedStreams(BlockInputStreams & ret_streams)
    {
        std::unique_lock lk(mu);
        for (auto & stream : added_streams)
            ret_streams.push_back(stream);
    }

    int addedStreamsCnt()
    {
        std::unique_lock lk(mu);
        return added_streams.size();
    }

private:
    std::deque<BlockInputStreamPtr> added_streams;
    bool is_cancelled;
    std::mutex mu;
};

class MultiplexInputStream final : public IProfilingBlockInputStream
{
private:
    static constexpr auto NAME = "Multiplex";

public:
    MultiplexInputStream(std::shared_ptr<MultiPartitionStreamPool> & shared_pool, const String & req_id)
        : log(Logger::get(req_id))
        , shared_pool(shared_pool)
    {
        shared_pool->exportAddedStreams(children);
        size_t num_children = children.size();
        if (num_children > 1)
        {
            Block header = children.at(0)->getHeader();
            for (size_t i = 1; i < num_children; ++i)
                assertBlocksHaveEqualStructure(children[i]->getHeader(), header, "MULTIPLEX");
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
        if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
            return;

        if (cur_stream)
        {
            if (auto * child = dynamic_cast<IProfilingBlockInputStream *>(&*cur_stream))
            {
                child->cancel(kill);
            }
        }

        shared_pool->cancel(kill);
    }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    /// Do nothing, to make the preparation when underlying InputStream is picked from the pool
    void readPrefix() override {}

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
