#include <Common/TiFlashMetrics.h>
#include <Storages/RateLimiter.h>

namespace DB
{

RateLimiter::RateLimiter(Context & db_context, Int64 rate_limit_, Int64 burst_rate_limit_, Int64 max_balance_)
    : context{db_context},
      rate_limit{rate_limit_},
      burst_rate_limit{burst_rate_limit_},
      max_balance{max_balance_},
      available_bytes{max_balance_},
      prev_alloc_balance{0},
      log{&Logger::get("RateLimiter")}
{
    LOG_INFO(log,
        "Creating RateLimiter with balance increase rate: " << rate_limit << ", allocate soft limit: " << burst_rate_limit
                                                            << ", allocate hard limit: " << max_balance);
    GET_METRIC(context.getTiFlashMetrics(), tiflash_storage_rate_limiter_balance).Set(available_bytes);
}

size_t RateLimiter::request(Int64 bytes)
{
    std::scoped_lock lock{mutex};

    // no limit
    if (rate_limit <= 0)
        return 0;

    refillIfNeed();

    Int64 alloc_bytes = 0;
    size_t elapsed_alloc_seconds = alloc_stop_watch.elapsedSeconds();
    //   1. if prev_alloc_balance - (current_time - prev_alloc_time) * rate_limit >= burst_rate_limit,
    //      then <allocated_bytes> = 0
    Int64 prev_alloc_working_balance = prev_alloc_balance - elapsed_alloc_seconds * rate_limit;
    if (prev_alloc_working_balance < burst_rate_limit)
    {
        alloc_bytes = bytes < available_bytes ? bytes : available_bytes;
        available_bytes = bytes < available_bytes ? available_bytes - bytes : 0;
    }
    alloc_stop_watch.restart();
    prev_alloc_balance = prev_alloc_working_balance + alloc_bytes;

    GET_METRIC(context.getTiFlashMetrics(), tiflash_storage_rate_limiter_consume_balance).Increment(alloc_bytes);
    GET_METRIC(context.getTiFlashMetrics(), tiflash_storage_rate_limiter_balance).Set(available_bytes);

    return bytes - alloc_bytes;
}

void RateLimiter::refillIfNeed()
{
    if (available_bytes >= max_balance)
        return;

    size_t elapsed_seconds = refill_stop_watch.elapsedSeconds();
    available_bytes = available_bytes + elapsed_seconds * rate_limit;
    if (available_bytes > max_balance)
        available_bytes = max_balance;
    refill_stop_watch.restart();
}

} // namespace DB
