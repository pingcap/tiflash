#include <Storages/Page/PageUtil.h>

namespace DB::PageUtil
{
UInt32 randInt(const UInt32 min, const UInt32 max)
{
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<UInt32> distribution(min, max);
    return distribution(generator);
}

} // namespace DB::PageUtil
