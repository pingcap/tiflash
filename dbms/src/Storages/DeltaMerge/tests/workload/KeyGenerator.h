#pragma once
#include <memory>
namespace DB::DM::tests
{
class WorkloadOptions;

class KeyGenerator
{
public:
    static std::unique_ptr<KeyGenerator> create(const WorkloadOptions & opts);

    KeyGenerator() {}
    virtual ~KeyGenerator() {}

    virtual uint64_t get64() = 0;
};
} // namespace DB::DM::tests