// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/MemoryTrackerSetter.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/ReadThread/WorkQueue.h>
#include <Storages/DeltaMerge/RowKeyRangeUtils.h>
#include <Storages/DeltaMerge/SegmentReadResultChannel_fwd.h>

#include <memory>
#include <optional>

namespace DB::DM
{
constexpr inline UInt64 STORE_ID_I_DONT_CARE = 0;

struct StoreAndTableId
{
    UInt64 store_id;
    Int64 table_id;

    bool operator==(const StoreAndTableId & other) const
    {
        return store_id == other.store_id && table_id == other.table_id;
    }
};
} // namespace DB::DM

namespace std
{

template <>
struct hash<DB::DM::StoreAndTableId>
{
    std::size_t operator()(const DB::DM::StoreAndTableId & v) const
    {
        // See https://stackoverflow.com/a/17017281
        size_t res = 17;
        res = res * 31 + hash<UInt64>()(v.store_id);
        res = res * 31 + hash<Int64>()(v.table_id);
        return res;
    }
};

} // namespace std

namespace DB
{
namespace DM
{
struct SegmentReadTask;
class Segment;
using SegmentPtr = std::shared_ptr<Segment>;
struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;

using SegmentReadTaskPtr = std::shared_ptr<SegmentReadTask>;
using SegmentReadTasks = std::list<SegmentReadTaskPtr>;
using AfterSegmentRead = std::function<void(const DMContextPtr &, const SegmentPtr &)>;

struct SegmentReadTask
{
    SegmentPtr segment;
    SegmentSnapshotPtr read_snapshot;
    RowKeyRanges ranges;

    SegmentReadTask(
        const SegmentPtr & segment_,
        const SegmentSnapshotPtr & read_snapshot_,
        const RowKeyRanges & ranges_ = {});

    ~SegmentReadTask();

    std::pair<size_t, size_t> getRowsAndBytes() const;

    void addRange(const RowKeyRange & range) { ranges.push_back(range); }

    void mergeRanges() { ranges = DM::tryMergeRanges(std::move(ranges), 1); }

    static SegmentReadTasks trySplitReadTasks(const SegmentReadTasks & tasks, size_t expected_size);
};

enum class ReadMode
{
    /**
     * Read in normal mode. Data is ordered by PK, and only the most recent version is returned.
     */
    Normal,

    /**
     * Read in fast mode. Data is not sort merged, and all versions are returned. However, deleted records (del_mark=1)
     * will be still filtered out.
     */
    Fast,

    /**
     * Read in raw mode, for example, for statements like `SELRAW *`. In raw mode, data is not sort merged and all versions
     * are just returned.
     */
    Raw,

    Bitmap,
};

// If `enable_read_thread_` is true, `SegmentReadTasksWrapper` use `std::unordered_map` to index `SegmentReadTask` by segment id,
// else it is the same as `SegmentReadTasks`, a `std::list` of `SegmentReadTask`.
// `SegmeneReadTasksWrapper` is not thread-safe.
class SegmentReadTasksWrapper
{
public:
    SegmentReadTasksWrapper(bool enable_read_thread_, SegmentReadTasks && ordered_tasks_);

    // `nextTask` pops task sequentially. This function is used when `enable_read_thread` is false.
    SegmentReadTaskPtr nextTask();

    // `getTask` and `getTasks` are used when `enable_read_thread` is true.
    SegmentReadTaskPtr getTask(UInt64 seg_id);
    const std::unordered_map<UInt64, SegmentReadTaskPtr> & getTasks() const;

    bool empty() const;

private:
    bool enable_read_thread;
    SegmentReadTasks ordered_tasks;
    std::unordered_map<UInt64, SegmentReadTaskPtr> unordered_tasks;
};

/// Note 1: The output of the SegmentReadTaskPool may contain extra TableID column.
/// Note 2: You must ensure all segments in the pool comes from the same physical table.
/// Note 3: In disaggregated mode, you must ensure all segments comes from the same Write Node.
class SegmentReadTaskPool
    : private boost::noncopyable
    , public std::enable_shared_from_this<SegmentReadTaskPool>
{
public:
    /// In disaggregated mode, store_id must be specified, in order to distinguish
    /// segments from different stores.
    /// In non-disaggregated mode, you may simply pass STORE_ID_I_DONT_CARE.
    SegmentReadTaskPool(
        uint64_t store_id_,
        int64_t physical_table_id_,
        size_t extra_table_id_index_,
        const DMContextPtr & dm_context_,
        const ColumnDefines & columns_to_read_,
        const PushDownFilterPtr & filter_,
        uint64_t max_version_,
        size_t expected_block_size_,
        ReadMode read_mode_,
        SegmentReadTasks && tasks_,
        AfterSegmentRead after_segment_read_,
        const String & debug_tag,
        bool enable_read_thread_,
        Int64 num_streams_,
        SegmentReadResultChannelPtr result_channel_ = nullptr);

    void initDefaultResultChannel();

    SegmentReadTaskPtr nextTask();
    const std::unordered_map<UInt64, SegmentReadTaskPtr> & getTasks();
    SegmentReadTaskPtr getTask(UInt64 seg_id);

    BlockInputStreamPtr buildInputStream(SegmentReadTaskPtr & t);

    bool readOneBlock(BlockInputStreamPtr & stream, const SegmentPtr & seg);

    std::unordered_map<uint64_t, std::vector<uint64_t>>::const_iterator scheduleSegment(
        const std::unordered_map<uint64_t, std::vector<uint64_t>> & segments,
        uint64_t expected_merge_count);

    Int64 getFreeActiveSegments() const;

    StoreAndTableId getStoreAndTableId() const
    {
        return StoreAndTableId{
            .store_id = store_id,
            .table_id = physical_table_id,
        };
    }

    SegmentReadResultChannelPtr getResultChannel() const
    {
        RUNTIME_CHECK(result_channel != nullptr);
        return result_channel;
    }

    bool valid() const;

public:
    const uint64_t pool_id;
    const uint64_t store_id;
    const int64_t physical_table_id;

    // The memory tracker of MPPTask.
    const MemoryTrackerPtr mem_tracker;

private:
    Int64 getFreeActiveSegmentsUnlock() const;
    void finishSegment(const SegmentPtr & seg);

    const String debug_tag;
    const size_t extra_table_id_index;
    DMContextPtr dm_context;
    ColumnDefines columns_to_read;
    PushDownFilterPtr filter;
    const uint64_t max_version;
    const size_t expected_block_size;
    const ReadMode read_mode;
    SegmentReadTasksWrapper tasks_wrapper;
    AfterSegmentRead after_segment_read;
    mutable std::mutex mutex;
    std::unordered_set<uint64_t> active_segment_ids;

    std::atomic<bool> all_finished = false;

    SegmentReadResultChannelPtr result_channel;

    LoggerPtr log;

    const Int64 block_slot_limit;
    const Int64 active_segment_limit;

    inline static std::atomic<uint64_t> pool_id_gen{1};
    static uint64_t nextPoolId()
    {
        return pool_id_gen.fetch_add(1, std::memory_order_relaxed);
    }
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;
using SegmentReadTaskPools = std::vector<SegmentReadTaskPoolPtr>;

} // namespace DM
} // namespace DB
