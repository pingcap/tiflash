#pragma once

#include <Core/Types.h>
#include <mutex>

namespace DB
{
namespace DM
{
namespace tests
{
class IDGenerator
{
public:
    IDGenerator() : id{0} {}

    UInt64 get()
    {
        std::lock_guard<std::mutex> guard{mutex};
        return id++;
    }

private:
    std::mutex mutex;
    UInt64     id;
};
} // namespace tests
} // namespace DM
} // namespace DB
