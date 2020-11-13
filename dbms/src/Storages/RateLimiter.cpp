#include <Storages/RateLimiter.h>

namespace DB
{

size_t RateLimiter::request(Int64 bytes)
{
    std::scoped_lock lock{mutex};

    // no limit
    if (balance_increase_rate <= 0)
        return 0;

    refillIfNeed();

    Int64 alloc_bytes = 0;
    auto current_time = Clock::now();
    size_t elapsed_alloc_seconds
        = std::chrono::duration_cast<std::chrono::microseconds>(current_time - prev_alloc_time).count() / (1000 * 1000.0);
    //   1. if prev_alloc_balance - (current_time - prev_alloc_time) * balance_increase_rate >= alloc_balance_soft_limit,
    //      then <allocated_bytes> = 0
    Int64 prev_alloc_working_balance = prev_alloc_balance - elapsed_alloc_seconds * balance_increase_rate;
    if (prev_alloc_working_balance < alloc_balance_soft_limit)
    {
        alloc_bytes = bytes < available_bytes ? bytes : available_bytes;
        available_bytes = bytes < available_bytes ? available_bytes - bytes : 0;
    }
    prev_alloc_time = current_time;
    prev_alloc_balance = prev_alloc_working_balance + alloc_bytes;

    return bytes - alloc_bytes;
}

void RateLimiter::refillIfNeed()
{
    if (available_bytes >= alloc_balance_hard_limit)
        return;

    auto current_time = Clock::now();
    size_t elapsed_seconds
        = std::chrono::duration_cast<std::chrono::microseconds>(current_time - prev_refilled_time).count() / (1000 * 1000.0);
    available_bytes = available_bytes + elapsed_seconds * balance_increase_rate;
    if (available_bytes > alloc_balance_hard_limit)
        available_bytes = alloc_balance_hard_limit;
    prev_refilled_time = current_time;
}

} // namespace DB
