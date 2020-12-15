#include <cassert>

#include <Common/CurrentMetrics.h>
#include <Common/TiFlashMetrics.h>
#include <Encryption/RateLimiter.h>

namespace CurrentMetrics
{
extern const Metric RateLimiterPendingWriteRequest;
}

DB::RateLimiter::RateLimiter(TiFlashMetricsPtr metrics_, UInt64 rate_limit_per_sec_, UInt64 refill_period_ms_)
    : refill_period_ms{refill_period_ms_},
      refill_balance_per_period{calculateRefillBalancePerPeriod(rate_limit_per_sec_)},
      available_balance{refill_balance_per_period},
      total_bytes_through{0},
      stop{false},
      metrics{std::move(metrics_)}
{}

DB::RateLimiter::~RateLimiter()
{
    std::unique_lock<std::mutex> lock(request_mutex);
    stop = true;
    requests_to_wait = req_queue.size();
    for (auto * r : req_queue)
        r->cv.notify_one();
    while (requests_to_wait > 0)
        exit_cv.wait(lock);
}

void DB::RateLimiter::request(UInt64 bytes)
{
    std::unique_lock<std::mutex> lock(request_mutex);

    if (stop)
        return;

    // 0 means no limit
    if (!refill_balance_per_period)
        return;

    if (metrics)
        GET_METRIC(metrics, tiflash_storage_rate_limiter_total_request_bytes).Increment(bytes);

    if (available_balance >= bytes)
    {
        if (metrics)
            GET_METRIC(metrics, tiflash_storage_rate_limiter_total_alloc_bytes).Increment(bytes);
        total_bytes_through += bytes;
        available_balance -= bytes;
        return;
    }

    CurrentMetrics::Increment pending_request{CurrentMetrics::RateLimiterPendingWriteRequest};

    // request cannot be satisfied at this moment, enqueue
    Request r(bytes);
    req_queue.push_back(&r);
    while (!r.granted)
    {
        assert(!req_queue.empty());

        bool timed_out = false;
        // if this request is in the front of req_queue,
        // then it is responsible to trigger the refill process.
        if (req_queue.front() == &r)
        {
            if (refill_stop_watch.elapsedMilliseconds() >= refill_period_ms)
            {
                timed_out = true;
            }
            else
            {
                auto status = r.cv.wait_for(lock, std::chrono::milliseconds(refill_period_ms));
                timed_out = (status == std::cv_status::timeout);
            }
            if (timed_out)
            {
                refill_stop_watch.restart();
            }
        }
        else
        {
            // Not at the front of queue, just wait
            r.cv.wait(lock);
        }

        // request_mutex is held from now on
        if (stop)
        {
            requests_to_wait--;
            exit_cv.notify_one();
            return;
        }

        // time to do refill
        if (req_queue.front() == &r && timed_out)
        {
            refillAndAlloc();

            if (r.granted)
            {
                // current leader is granted with enough balance,
                // notify the current header of the queue.
                if (!req_queue.empty())
                    req_queue.front()->cv.notify_one();
                break;
            }
        }
    }
}

void DB::RateLimiter::refillAndAlloc()
{
    if (available_balance < refill_balance_per_period)
        available_balance += refill_balance_per_period;

    assert(!req_queue.empty());
    auto * head_req = req_queue.front();
    while (!req_queue.empty())
    {
        auto * next_req = req_queue.front();
        if (available_balance < next_req->remaining_bytes)
        {
            // Decrease remaining_bytes to avoid starvation of this request
            next_req->remaining_bytes -= available_balance;
            total_bytes_through += available_balance;
            available_balance = 0;
            break;
        }
        available_balance -= next_req->remaining_bytes;
        total_bytes_through += next_req->remaining_bytes;
        next_req->remaining_bytes = 0;
        next_req->granted = true;
        req_queue.pop_front();
        if (metrics)
            GET_METRIC(metrics, tiflash_storage_rate_limiter_total_alloc_bytes).Increment(next_req->bytes);
        // quota granted, signal the thread
        if (next_req != head_req)
            next_req->cv.notify_one();
    }
}
