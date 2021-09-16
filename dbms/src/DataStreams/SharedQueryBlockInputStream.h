#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <DataStreams/IProfilingBlockInputStream.h>

#include <thread>

namespace DB
{

/**
 * This block input stream is used by SharedQuery.
 * It enable multiple threads read from one stream.
 */
class SharedQueryBlockInputStream : public IProfilingBlockInputStream
{
public:
    SharedQueryBlockInputStream(size_t clients, const BlockInputStreamPtr & in_)
        : queue(clients), log(&Logger::get("SharedQueryBlockInputStream")), in(in_)
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
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    String getName() const override { return "SharedQuery"; }

    Block getHeader() const override { return children.back()->getHeader(); }

    void readPrefix() override
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (read_prefixed)
            return;
        read_prefixed = true;

        /// Start reading thread.
        thread = std::thread(&SharedQueryBlockInputStream::fetchBlocks, this);
    }

    void readSuffix() override
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (read_suffixed)
            return;
        read_suffixed = true;

        if (thread.joinable())
            thread.join();
        if (!exception_msg.empty())
            throw Exception(exception_msg);
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

    std::thread thread;
    std::mutex mutex;

    std::string exception_msg;

    Logger * log;
    BlockInputStreamPtr in;
};
} // namespace DB
