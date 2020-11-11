#include <Storages/RateLimiter.h>

namespace DB
{

size_t RateLimiter::request(size_t bytes)
{
    std::scoped_lock lock{mutex};

    if (rate_bytes_per_sec <= 0)
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
    if (available_bytes >= rate_bytes_per_sec)
        return;

    auto current_time = Clock::now();
    size_t elapsed_refill_period_num
        = std::chrono::duration_cast<std::chrono::microseconds>(current_time - refilled_time).count() / refill_period_us;
    auto rate_bytes_per_refill_period = rate_bytes_per_sec / (1000 * 1000 / refill_period_us);
    available_bytes = std::min(rate_bytes_per_sec, available_bytes + elapsed_refill_period_num * rate_bytes_per_refill_period);
    refilled_time = current_time;
}

} // namespace DB
