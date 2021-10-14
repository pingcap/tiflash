#pragma once

#include <Common/MPMCQueue.h>
#include <Common/ThreadFactory.h>
#include <Common/typeid_cast.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Mpp/getMPPTaskLog.h>

#include <thread>

namespace DB
{
/** This block input stream is used by SharedQuery.
  * It enable multiple threads read from one stream.
 */
class SharedQueryBlockInputStream : public IProfilingBlockInputStream
{
public:
    SharedQueryBlockInputStream(size_t clients, const BlockInputStreamPtr & in_, const LogWithPrefixPtr & log_)
        : queue(clients * 4)
        , log(getMPPTaskLog(log_, getName()))
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
        thread = ThreadFactory(true, "SharedQuery").newThread([this] { fetchBlocks(); });
    }

    void readSuffix() override
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (read_suffixed)
            return;
        read_suffixed = true;
        queue.cancel();

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

        while (true)
        {
            if (!exception_msg.empty())
            {
                throw Exception(exception_msg);
            }
            if (isCancelled())
                return {};
            auto res = queue.tryPop(try_action_timeout);
            if (res.has_value())
                return res.value();
            if (queue.getStatus() == MPMCQueueStatus::CANCELLED)
                return {};
        };
    }

    void fetchBlocks()
    {
        try
        {
            in->readPrefix();
            while (!isCancelled())
            {
                Block block = in->read();
                auto queue_status = MPMCQueueStatus::NORMAL;
                while (true)
                {
                    if (isCancelled())
                    {
                        // Notify waiting client.
                        queue.cancel();
                        break;
                    }
                    auto res = queue.tryPush(block, try_action_timeout);
                    if (res || (queue_status = queue.getStatus()) == MPMCQueueStatus::CANCELLED)
                        break;
                }

                if (!block || queue_status == MPMCQueueStatus::CANCELLED)
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
    static constexpr auto try_action_timeout = std::chrono::milliseconds(200);

    MPMCQueue<Block> queue;

    bool read_prefixed = false;
    bool read_suffixed = false;

    std::thread thread;
    std::mutex mutex;

    std::string exception_msg;

    LogWithPrefixPtr log;
    BlockInputStreamPtr in;
};
} // namespace DB
