#pragma once

#include <Common/ConcurrentBoundedQueue.h>
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
        : buffer_size(clients)
        , queue(clients)
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

    void dumpExtra(std::ostream & ostr) const override
    {
        ostr << "buffer_size: " << buffer_size;
    }

private:
    static constexpr UInt64 try_action_millisecionds = 200;

    size_t buffer_size;

    ConcurrentBoundedQueue<Block> queue;

    bool read_prefixed = false;
    bool read_suffixed = false;

    std::thread thread;
    std::mutex mutex;

    std::string exception_msg;

    LogWithPrefixPtr log;
    BlockInputStreamPtr in;
};
} // namespace DB
