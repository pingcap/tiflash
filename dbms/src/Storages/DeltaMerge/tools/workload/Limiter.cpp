#include <Encryption/RateLimiter.h>
#include <Storages/DeltaMerge/tools/workload/Limiter.h>
#include <Storages/DeltaMerge/tools/workload/Options.h>
#include <fmt/core.h>

#include <cmath>

namespace DB::DM::tests
{
class ConstantLimiter : public Limiter
{
public:
    ConstantLimiter(uint64_t rate_per_sec)
        : limiter(rate_per_sec, LimiterType::UNKNOW)
    {}
    virtual void request() override
    {
        limiter.request(1);
    }

private:
    WriteLimiter limiter;
};

std::unique_ptr<Limiter> Limiter::create(const WorkloadOptions & opts)
{
    uint64_t per_sec = std::ceil(static_cast<double>(opts.max_write_per_sec / opts.write_thread_count));
    return std::make_unique<ConstantLimiter>(per_sec);
}

} // namespace DB::DM::tests