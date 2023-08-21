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

#if !(defined(__FreeBSD__) || defined(__APPLE__) || defined(_MSC_VER))

#include <Common/Exception.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <linux/aio_abi.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <boost/noncopyable.hpp>
#include <boost/range/iterator_range.hpp>
#include <condition_variable>
#include <ext/singleton.h>
#include <future>
#include <map>
#include <mutex>


/** Small wrappers for asynchronous I/O.
  */


inline int io_setup(unsigned nr, aio_context_t * ctxp)
{
    return syscall(__NR_io_setup, nr, ctxp);
}

inline int io_destroy(aio_context_t ctx)
{
    return syscall(__NR_io_destroy, ctx);
}

/// last argument is an array of pointers technically speaking
inline int io_submit(aio_context_t ctx, Int64 nr, struct iocb * iocbpp[])
{
    return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

inline int io_getevents(aio_context_t ctx, Int64 min_nr, Int64 max_nr, io_event * events, struct timespec * timeout)
{
    return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}


struct AIOContext : private boost::noncopyable
{
    aio_context_t ctx;

    explicit AIOContext(unsigned int nr_events = 128)
    {
        ctx = 0;
        if (io_setup(nr_events, &ctx) < 0)
            DB::throwFromErrno("io_setup failed");
    }

    ~AIOContext() { io_destroy(ctx); }
};


namespace DB
{
namespace ErrorCodes
{
extern const int AIO_COMPLETION_ERROR;
extern const int AIO_SUBMIT_ERROR;
} // namespace ErrorCodes


class AIOContextPool : public ext::Singleton<AIOContextPool>
{
    friend class ext::Singleton<AIOContextPool>;

    static const auto max_concurrent_events = 128;
    static const auto timeout_sec = 1;

    AIOContext aio_context{max_concurrent_events};

    using ID = size_t;
    using BytesRead = ssize_t;

    /// Autoincremental id used to identify completed requests
    ID id{};
    mutable std::mutex mutex;
    mutable std::condition_variable have_resources;
    std::map<ID, std::promise<BytesRead>> promises;

    std::atomic<bool> cancelled{false};
    std::thread io_completion_monitor{&AIOContextPool::doMonitor, this};

    ~AIOContextPool()
    {
        cancelled.store(true, std::memory_order_relaxed);
        io_completion_monitor.join();
    }

    void doMonitor()
    {
        /// continue checking for events unless cancelled
        while (!cancelled.load(std::memory_order_relaxed))
            waitForCompletion();

        /// wait until all requests have been completed
        while (!promises.empty())
            waitForCompletion();
    }

    void waitForCompletion()
    {
        /// array to hold completion events
        io_event events[max_concurrent_events];

        try
        {
            const auto num_events = getCompletionEvents(events, max_concurrent_events);
            fulfillPromises(events, num_events);
            notifyProducers(num_events);
        }
        catch (...)
        {
            /// there was an error, log it, return to any producer and continue
            reportExceptionToAnyProducer();
            tryLogCurrentException("AIOContextPool::waitForCompletion()");
        }
    }

    int getCompletionEvents(io_event events[], const int max_events) const
    {
        timespec timeout{timeout_sec, 0};

        auto num_events = 0;

        /// request 1 to `max_events` events
        while ((num_events = io_getevents(aio_context.ctx, 1, max_events, events, &timeout)) < 0)
            if (errno != EINTR)
                throwFromErrno(
                    "io_getevents: Failed to wait for asynchronous IO completion",
                    ErrorCodes::AIO_COMPLETION_ERROR,
                    errno);

        return num_events;
    }

    void fulfillPromises(const io_event events[], const int num_events)
    {
        if (num_events == 0)
            return;

        const std::lock_guard lock{mutex};

        /// look at returned events and find corresponding promise, set result and erase promise from map
        for (const auto & event : boost::make_iterator_range(events, events + num_events))
        {
            /// get id from event
            const auto id = event.data;

            /// set value via promise and release it
            const auto it = promises.find(id);
            if (it == std::end(promises))
            {
                LOG_ERROR(Logger::get("AIOcontextPool"), "Found io_event with unknown id {}", id);
                continue;
            }

            it->second.set_value(event.res);
            promises.erase(it);
        }
    }

    void notifyProducers(const int num_producers) const
    {
        if (num_producers == 0)
            return;

        if (num_producers > 1)
            have_resources.notify_all();
        else
            have_resources.notify_one();
    }

    void reportExceptionToAnyProducer()
    {
        const std::lock_guard lock{mutex};

        const auto any_promise_it = std::begin(promises);
        any_promise_it->second.set_exception(std::current_exception());
    }

public:
    /// Request AIO read operation for iocb, returns a future with number of bytes read
    std::future<BytesRead> post(struct iocb & iocb)
    {
        std::unique_lock lock{mutex};

        /// get current id and increment it by one
        const auto request_id = id++;

        /// create a promise and put request in "queue"
        promises.emplace(request_id, std::promise<BytesRead>{});
        /// store id in AIO request for further identification
        iocb.aio_data = request_id;

        struct iocb * requests[]{&iocb};

        /// submit a request
        while (io_submit(aio_context.ctx, 1, requests) < 0)
        {
            if (errno == EAGAIN)
                /// wait until at least one event has been completed (or a spurious wakeup) and try again
                have_resources.wait(lock);
            else if (errno != EINTR)
                throwFromErrno(
                    "io_submit: Failed to submit a request for asynchronous IO",
                    ErrorCodes::AIO_SUBMIT_ERROR,
                    errno);
        }

        return promises[request_id].get_future();
    }
};


} // namespace DB

#endif
