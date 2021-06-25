#include <Common/Stopwatch.h>

namespace DB
{
namespace DM
{

struct DurationStat
{
    UInt64 lock_ns = 0;

    double getLockSeconds() { return lock_ns / 1000000000ULL; }
};

#define LOCK_AND_ADD_DURATION(mtx, stat) \
    Stopwatch        watch;              \
    std::scoped_lock lock(mtx);          \
    if ((stat) != nullptr)               \
        (stat)->lock_ns += watch.elapsed();

} // namespace DM
} // namespace DB