#include <Common/TiFlashMetrics.h>
#include <Storages/RateLimiter.h>

namespace DB
{

RateLimiter::RateLimiter(
    Context & db_context, Int64 balance_increase_rate_, Int64 alloc_balance_soft_limit_, Int64 alloc_balance_hard_limit_)
    : context{db_context},
      balance_increase_rate{balance_increase_rate_},
      alloc_balance_soft_limit{alloc_balance_soft_limit_},
      alloc_balance_hard_limit{alloc_balance_hard_limit_},
      available_bytes{alloc_balance_hard_limit_},
      prev_alloc_balance{0},
      log{&Logger::get("RateLimiter")}
{
    LOG_INFO(log,
        "Creating RateLimiter with balance increase rate: " << balance_increase_rate
                                                            << ", allocate soft limit: " << alloc_balance_soft_limit
                                                            << ", allocate hard limit: " << alloc_balance_hard_limit);
    GET_METRIC(context.getTiFlashMetrics(), tiflash_storage_rate_limiter_balance).Set(available_bytes);
}

size_t RateLimiter::request(Int64 bytes)
{
    std::scoped_lock lock{mutex};

    // no limit
    if (balance_increase_rate <= 0)
        return 0;

    refillIfNeed();

    Int64 alloc_bytes = 0;
    size_t elapsed_alloc_seconds = alloc_stop_watch.elapsedSeconds();
    //   1. if prev_alloc_balance - (current_time - prev_alloc_time) * balance_increase_rate >= alloc_balance_soft_limit,
    //      then <allocated_bytes> = 0
    Int64 prev_alloc_working_balance = prev_alloc_balance - elapsed_alloc_seconds * balance_increase_rate;
    if (prev_alloc_working_balance < alloc_balance_soft_limit)
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
    if (available_bytes >= alloc_balance_hard_limit)
        return;

    size_t elapsed_seconds = refill_stop_watch.elapsedSeconds();
    available_bytes = available_bytes + elapsed_seconds * balance_increase_rate;
    if (available_bytes > alloc_balance_hard_limit)
        available_bytes = alloc_balance_hard_limit;
    refill_stop_watch.restart();
}

} // namespace DB
