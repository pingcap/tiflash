#include <Storages/RateLimiter.h>

namespace DB
{

size_t RateLimiter::request(Int64 bytes)
{
    std::scoped_lock lock{mutex};

    if (max_rate_bytes_balance <= 0)
    {
        return 0;
    }

    refillIfNeed();
    if (available_bytes >= bytes)
    {
        available_bytes -= bytes;
        return 0;
    }
    else
    {
        bytes -= available_bytes;
        available_bytes = 0;
        return bytes;
    }
}

void RateLimiter::refillIfNeed()
{
    if (available_bytes >= max_rate_bytes_balance)
        return;

    auto current_time = Clock::now();
    auto rate_bytes_per_refill_period = max_rate_bytes_balance / max_refill_period_count;
    size_t elapsed_refill_period_num
        = std::chrono::duration_cast<std::chrono::microseconds>(current_time - refilled_time).count() / refill_period_us;
    elapsed_refill_period_num = std::min(elapsed_refill_period_num, max_refill_period_count);
    available_bytes = available_bytes + elapsed_refill_period_num * rate_bytes_per_refill_period;
    if (available_bytes > max_rate_bytes_balance)
        available_bytes = max_rate_bytes_balance;
    refilled_time = current_time;
}

} // namespace DB
