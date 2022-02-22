#include <Encryption/RateLimiter.h>

namespace DB
{
class MockReadLimiter final : public ReadLimiter
{
public:
    MockReadLimiter(
        std::function<Int64()> getIOStatistic_,
        Int64 rate_limit_per_sec_,
        LimiterType type_ = LimiterType::UNKNOW,
        Int64 get_io_stat_period_us = 2000,
        UInt64 refill_period_ms_ = 100)
        : ReadLimiter(getIOStatistic_, rate_limit_per_sec_, type_, get_io_stat_period_us, refill_period_ms_)
    {
    }

protected:
    void consumeBytes(Int64 bytes) override
    {
        // Need soft limit here.
        WriteLimiter::consumeBytes(bytes);
    }
};

} // namespace DB