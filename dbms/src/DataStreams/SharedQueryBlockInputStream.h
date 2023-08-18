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
#include <Common/MPMCQueue.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <Common/typeid_cast.h>
#include <DataStreams/IProfilingBlockInputStream.h>

#include <thread>

namespace DB
{
namespace FailPoints
{
extern const char random_sharedquery_failpoint[];
} // namespace FailPoints

/** This block input stream is used by SharedQuery.
  * It enable multiple threads read from one stream.
 */
class SharedQueryBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "SharedQuery";

public:
    SharedQueryBlockInputStream(
        size_t clients,
        Int64 max_buffered_bytes,
        const BlockInputStreamPtr & in_,
        const String & req_id)
        : queue(CapacityLimits(clients, max_buffered_bytes), [](const Block & block) { return block.allocatedBytes(); })
        , log(Logger::get(req_id))
        , in(in_)
    {
        children.push_back(in);
    }

    ~SharedQueryBlockInputStream()
    {
        try
        {
            cancel(false);
            readSuffix();
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }

    String getName() const override { return NAME; }

    Block getHeader() const override { return children.back()->getHeader(); }

    void readPrefix() override
    {
        std::unique_lock lock(mutex);

        if (read_prefixed)
            return;
        read_prefixed = true;
        /// Start reading thread.
        thread_manager = newThreadManager();
        thread_manager->schedule(true, "SharedQuery", [this] { fetchBlocks(); });
    }

    void readSuffix() override
    {
        std::unique_lock lock(mutex);

        if (read_suffixed)
            return;
        read_suffixed = true;
        queue.cancel();
        waitThread();
    }

    /** Different from the default implementation by trying to cancel the queue
      */
    void cancel(bool kill) override
    {
        if (kill)
            is_killed = true;

        bool old_val = false;
        if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
            return;

        // notify the thread exit asap.
        queue.cancel();

        auto * ptr = dynamic_cast<IProfilingBlockInputStream *>(in.get());
        assert(ptr);

        ptr->cancel(kill);
    }

    virtual void collectNewThreadCountOfThisLevel(int & cnt) override { ++cnt; }

protected:
    /// The BlockStreamProfileInfo of SharedQuery is useless,
    /// and it will trigger tsan UT fail because of data race.
    /// So overriding method `read` here.
    Block read(FilterPtr &, bool) override
    {
        std::unique_lock lock(mutex);

        if (!read_prefixed)
            throw Exception("read operation called before readPrefix");
        if (read_suffixed)
            throw Exception("read operation called after readSuffix");

        Block block;
        if (queue.pop(block) != MPMCQueueResult::OK)
        {
            if (!isCancelled())
            {
                // must be `fetchBlocks` finished, `waitThread` to get its final status
                waitThread();
            }
            return {};
        }

        return block;
    }
    Block readImpl() override { throw Exception("Unsupport"); }

    void fetchBlocks()
    {
        try
        {
            in->readPrefix();
            while (true)
            {
                FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_sharedquery_failpoint);
                Block block = in->read();
                // in is finished or queue is canceled
                if (!block || queue.push(block) != MPMCQueueResult::OK)
                    break;
            }
            in->readSuffix();
        }
        catch (Exception & e)
        {
            exception_msg = e.message();
        }
        catch (std::exception & e)
        {
            exception_msg = e.what();
        }
        catch (...)
        {
            exception_msg = "other error";
        }
        queue.finish();
    }

    void waitThread()
    {
        if (thread_manager)
        {
            thread_manager->wait();
            thread_manager.reset();
        }
        if (!exception_msg.empty())
            throw Exception(exception_msg);
    }

    uint64_t collectCPUTimeNsImpl(bool /*is_thread_runner*/) override
    {
        // `SharedQueryBlockInputStream` does not count its own execute time,
        // whether `SharedQueryBlockInputStream` is `thread-runner` or not,
        // because `SharedQueryBlockInputStream` basically does not use cpu, only `condition_cv.wait`.
        uint64_t cpu_time_ns = 0;
        forEachChild([&](IBlockInputStream & child) {
            // Each of SharedQueryBlockInputStream's children is a thread-runner.
            cpu_time_ns += child.collectCPUTimeNs(true);
            return false;
        });
        return cpu_time_ns;
    }

private:
    MPMCQueue<Block> queue;

    bool read_prefixed = false;
    bool read_suffixed = false;

    std::mutex mutex;
    std::shared_ptr<ThreadManager> thread_manager;

    std::string exception_msg;

    LoggerPtr log;
    BlockInputStreamPtr in;
};
} // namespace DB
