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

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <Common/typeid_cast.h>
#include <DataStreams/IProfilingBlockInputStream.h>

#include <thread>

namespace DB
{
/** This block input stream is used by SharedQuery.
  * It enable multiple threads read from one stream.
 */
class SharedQueryBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "SharedQuery";

public:
    SharedQueryBlockInputStream(size_t clients, const BlockInputStreamPtr & in_, const String & req_id)
        : queue(clients)
        , log(Logger::get(NAME, req_id))
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
        std::lock_guard<std::mutex> lock(mutex);

        if (read_prefixed)
            return;
        read_prefixed = true;
        /// Start reading thread.
        thread_manager = newThreadManager();
        thread_manager->schedule(true, "SharedQuery", [this] { fetchBlocks(); });
    }

    void readSuffix() override
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (read_suffixed)
            return;
        read_suffixed = true;
        if (thread_manager)
            thread_manager->wait();
        if (!exception_msg.empty())
            throw Exception(exception_msg);
    }

    virtual void collectNewThreadCountOfThisLevel(int & cnt) override
    {
        ++cnt;
    }

protected:
    Block readImpl() override
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (!read_prefixed)
            throw Exception("read operation called before readPrefix");

        Block block;
        do
        {
            if (!exception_msg.empty())
            {
                throw Exception(exception_msg);
            }
            if (isCancelled() || read_suffixed)
                return {};
        } while (!queue.tryPop(block, try_action_millisecionds));

        return block;
    }

    void fetchBlocks()
    {
        try
        {
            in->readPrefix();
            while (!isCancelled())
            {
                Block block = in->read();
                do
                {
                    if (isCancelled() || read_suffixed)
                    {
                        // Notify waiting client.
                        queue.tryEmplace(0);
                        break;
                    }
                } while (!queue.tryPush(block, try_action_millisecionds));

                if (!block)
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
    }

private:
    static constexpr UInt64 try_action_millisecionds = 200;

    ConcurrentBoundedQueue<Block> queue;

    bool read_prefixed = false;
    bool read_suffixed = false;

    std::mutex mutex;
    std::shared_ptr<ThreadManager> thread_manager;

    std::string exception_msg;

    LoggerPtr log;
    BlockInputStreamPtr in;
};
} // namespace DB
