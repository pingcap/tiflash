#include <Encryption/RateLimiter.h>

DB::RateLimiter::RateLimiter(UInt64 rate_limit_per_sec_, UInt64 refill_period_ms_)
: refill_period_ms{refill_period_ms_},
      refill_balance_per_period{calculateRefillBalancePerPeriod(rate_limit_per_sec_)},
      available_balance{refill_balance_per_period}, stop{false}
{}

DB::RateLimiter::~RateLimiter()
{
    std::unique_lock<std::mutex> lock(request_mutex);
    stop = true;
    for (auto * r : req_queue)
        r->cv.notify_one();
    while (!req_queue.empty())
        exit_cv.wait(lock);
}

void DB::RateLimiter::request(UInt64 bytes)
{
    std::unique_lock<std::mutex> lock(request_mutex);

    if (stop)
        return;

    if (available_balance >= bytes)
    {
        available_balance -= bytes;
        return;
    }

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
        if (available_balance < next_req->request_bytes)
        {
            // avoid starvation
            next_req->request_bytes -= available_balance;
            available_balance = 0;
            break;
        }
        available_balance -= next_req->request_bytes;
        next_req->request_bytes = 0;
        next_req->granted = true;
        req_queue.pop_front();
        // quota granted, signal the thread
        if (next_req != head_req)
            next_req->cv.notify_one();
    }
}
