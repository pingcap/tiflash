#pragma once

#include <Common/FiberPool.hpp>
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
    SharedQueryBlockInputStream(
        size_t clients [[maybe_unused]],
        const BlockInputStreamPtr & in_,
        const LogWithPrefixPtr & log_,
        bool run_in_thread_)
        : queue(1024)
        , run_in_thread(run_in_thread_)
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
        std::lock_guard lock(mutex);

        if (read_prefixed)
            return;
        read_prefixed = true;

        /// Start reading thread.
        if (run_in_thread)
            thread = std::make_unique<std::thread>(ThreadFactory(true, "SharedQuery").newThread([this] { fetchBlocks(); }));
        else
            future = DefaultFiberPool::submit_job([this] { fetchBlocks(); });
    }

    void readSuffix() override
    {
        std::lock_guard lock(mutex);

        if (read_suffixed)
            return;
        read_suffixed = true;
        queue.close();

        if (run_in_thread)
        {
            if (thread && thread->joinable())
            {
                thread->join();
                thread.reset();
            }
        }
        else
        {
            if (future.has_value())
                future.value().wait();
        }
        if (!exception_msg.empty())
            throw Exception(exception_msg);
    }

protected:
    Block readImpl() override
    {
        std::lock_guard lock(mutex);

        if (!read_prefixed)
            throw Exception("read operation called before readPrefix");

        Block block;
        while (true)
        {
            if (!exception_msg.empty())
            {
                throw Exception(exception_msg);
            }
            if (isCancelled())
                return {};
            auto res = queue.pop_wait_for(block, try_action_timeout);
            if (res == boost::fibers::channel_op_status::success)
                return block;
            if (res == boost::fibers::channel_op_status::closed)
                return {};
        }
    }

    void fetchBlocks()
    {
        try
        {
            in->readPrefix();
            while (!isCancelled())
            {
                Block block = in->read();
                while (true)
                {
                    if (isCancelled())
                    {
                        // Notify waiting client.
                        queue.close();
                        break;
                    }
                    auto res = queue.push_wait_for(block, try_action_timeout);
                    if (res == boost::fibers::channel_op_status::success ||
                        res == boost::fibers::channel_op_status::closed)
                        break;
                }

                if (!block || queue.is_closed())
                    break;
            }
            if (!isCancelled())
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

    boost::fibers::buffered_channel<Block> queue;

    bool run_in_thread;
    bool read_prefixed = false;
    bool read_suffixed = false;

    std::optional<boost::fibers::future<void>> future;
    std::unique_ptr<std::thread> thread;
    boost::fibers::mutex mutex;

    std::string exception_msg;

    LogWithPrefixPtr log;
    BlockInputStreamPtr in;
};
} // namespace DB
