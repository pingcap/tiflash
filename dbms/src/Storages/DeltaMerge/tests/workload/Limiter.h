#pragma once
#include <memory>
namespace DB::DM::tests
{
struct WorkloadOptions;

class Limiter
{
public:
    static std::unique_ptr<Limiter> create(const WorkloadOptions & opts);
    virtual void request() = 0;
    virtual ~Limiter() {}
};
} // namespace DB::DM::tests