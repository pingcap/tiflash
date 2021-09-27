
#include <Common/TiFlashMetrics.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>

namespace DB
{
namespace
{
struct ThreadTracker
{
    ThreadTracker()
    {
        GET_METRIC(tiflash_hash_join_gauge, type_build_threads).Increment();
    }

    ~ThreadTracker()
    {
        GET_METRIC(tiflash_hash_join_gauge, type_build_threads).Decrement();
    }
};

} // namespace

Block HashJoinBuildBlockInputStream::readImpl()
{
    thread_local std::unique_ptr<ThreadTracker> tracker;
    if (!tracker)
        tracker = std::make_unique<ThreadTracker>();

    Block block = children.back()->read();
    if (!block)
        return block;

    GET_METRIC(tiflash_hash_join_counter, type_build_in_bytes).Increment(block.bytes());
    GET_METRIC(tiflash_hash_join_counter, type_build_in_blocks).Increment();
    GET_METRIC(tiflash_hash_join_counter, type_build_ops).Increment();

    auto begin_ts = std::chrono::steady_clock::now();
    join->insertFromBlock(block, stream_index);
    auto end_ts = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ts - begin_ts).count();

    GET_METRIC(tiflash_hash_join_counter, type_build_duration).Increment(duration);

    return block;
}

} // namespace DB
