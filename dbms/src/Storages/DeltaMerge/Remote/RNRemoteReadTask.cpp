// Copyright 2023 PingCAP, Inc.
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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/TiFlashMetrics.h>
#include <Common/UniThreadPool.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <IO/IOThreadPools.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/RNDataProvider.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask.h>
#include <Storages/DeltaMerge/Remote/RNRemoteSegmentThreadInputStream.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Transaction/Types.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/types.h>

#include <future>
#include <magic_enum.hpp>
#include <memory>
#include <mutex>
namespace DB::DM
{

RNRemoteReadTask::RNRemoteReadTask(std::vector<RNRemoteStoreReadTaskPtr> && input_tasks_)
    : num_segments(0)
    , log(Logger::get())
{
    for (const auto & store_task : input_tasks_)
    {
        if (!store_task)
            continue;
        auto res = store_tasks.emplace(store_task->store_id, store_task);
        RUNTIME_CHECK_MSG(res.second, "Duplicated task from store_id={}", store_task->store_id);
        num_segments += store_task->numRemainTasks();

        // Push all inited tasks to ready queue
        for (const auto & table_task : store_task->table_read_tasks)
        {
            for (const auto & seg_task : table_task->tasks)
            {
                // TODO: If all pages are ready in local
                // cache, and the segment does not contains any
                // blocks on write node's mem-table, then we
                // can simply skip the fetch page pharse and
                // push it into ready queue
                ready_segment_tasks[seg_task->state].push_back(seg_task);
            }
        }
    }
    curr_store = store_tasks.begin();
}

RNRemoteReadTask::~RNRemoteReadTask()
{
    cv_ready_tasks.notify_all();
}

size_t RNRemoteReadTask::numSegments() const
{
    return num_segments;
}

RNRemoteSegmentReadTaskPtr RNRemoteReadTask::nextFetchTask()
{
    // A simple scheduling policy that try to execute the segment tasks
    // from different stores in parallel
    std::lock_guard gurad(mtx_tasks);
    while (true)
    {
        if (store_tasks.empty())
            return nullptr;

        if (curr_store->second->numRemainTasks() > 0)
        {
            auto task = curr_store->second->nextTask();
            // Move to next store
            curr_store++;
            if (curr_store == store_tasks.end())
                curr_store = store_tasks.begin();
            return task;
        }
        // No tasks left in this store, erase and try to pop task from the next store
        curr_store = store_tasks.erase(curr_store);
        if (curr_store == store_tasks.end())
            curr_store = store_tasks.begin();
    }
}

void RNRemoteReadTask::updateTaskState(const RNRemoteSegmentReadTaskPtr & seg_task, SegmentReadTaskState target_state, bool meet_error)
{
    {
        std::unique_lock ready_lock(mtx_ready_tasks);
        const auto old_state = seg_task->state;
        auto state_iter = ready_segment_tasks.find(old_state);
        RUNTIME_CHECK(state_iter != ready_segment_tasks.end());

        // TODO: make it an unordered_map
        bool found = false;
        for (auto task_iter = state_iter->second.begin(); task_iter != state_iter->second.end(); task_iter++)
        {
            auto & task = *task_iter;
            if (task->store_id != seg_task->store_id
                || task->ks_table_id != seg_task->ks_table_id
                || task->segment_id != seg_task->segment_id)
            {
                continue;
            }
            seg_task->state = meet_error ? SegmentReadTaskState::Error : target_state;
            found = true;
            // Move it into the right state, note `task`/`task_iter` is invalid
            state_iter->second.erase(task_iter);
            if (state_iter->second.empty())
                ready_segment_tasks.erase(state_iter);

            insertTask(seg_task, ready_lock);
            break;
        }
        RUNTIME_CHECK(found);
    }

    cv_ready_tasks.notify_one();
}

void RNRemoteReadTask::allDataReceive(const String & end_err_msg)
{
    {
        std::unique_lock ready_lock(mtx_ready_tasks);
        // set up the error message
        if (err_msg.empty() && !end_err_msg.empty())
            err_msg = end_err_msg;

        for (auto iter = ready_segment_tasks.begin(); iter != ready_segment_tasks.end(); /* empty */)
        {
            const auto state = iter->first;
            const auto & tasks = iter->second;
            if (state != SegmentReadTaskState::Init && state != SegmentReadTaskState::Receiving)
            {
                ++iter;
                continue;
            }

            // init or receiving -> all data ready
            for (const auto & seg_task : tasks)
            {
                auto old_state = seg_task->state;
                seg_task->state = SegmentReadTaskState::DataReady;
                LOG_DEBUG(
                    log,
                    "seg_task: {} from {} to {}",
                    seg_task->segment_id,
                    magic_enum::enum_name(old_state),
                    magic_enum::enum_name(seg_task->state));
                insertTask(seg_task, ready_lock);
            }

            iter = ready_segment_tasks.erase(iter);
        }
    }
    cv_ready_tasks.notify_all();
}


void RNRemoteReadTask::insertTask(const RNRemoteSegmentReadTaskPtr & seg_task, std::unique_lock<std::mutex> &)
{
    if (auto state_iter = ready_segment_tasks.find(seg_task->state);
        state_iter != ready_segment_tasks.end())
        state_iter->second.push_back(seg_task);
    else
        ready_segment_tasks.emplace(seg_task->state, std::list<RNRemoteSegmentReadTaskPtr>{seg_task});
}

RNRemoteSegmentReadTaskPtr RNRemoteReadTask::nextTaskForPrepare()
{
    std::unique_lock ready_lock(mtx_ready_tasks);
    RNRemoteSegmentReadTaskPtr seg_task = nullptr;
    cv_ready_tasks.wait(ready_lock, [this, &seg_task, &ready_lock] {
        // All segment task are processed, return a nullptr
        if (doneOrErrorHappen())
            return true;

        // Check whether there are segment task ready for place index
        if (auto iter = ready_segment_tasks.find(SegmentReadTaskState::DataReady); iter != ready_segment_tasks.end())
        {
            if (iter->second.empty())
                return false; // yield for another awake
            seg_task = iter->second.front();
            iter->second.pop_front();
            if (iter->second.empty())
            {
                ready_segment_tasks.erase(iter);
            }

            const auto old_state = seg_task->state;
            seg_task->state = SegmentReadTaskState::DataReadyAndPrepraring;
            LOG_DEBUG(
                log,
                "seg_task: {} from {} to {}",
                seg_task->segment_id,
                magic_enum::enum_name(old_state),
                magic_enum::enum_name(seg_task->state));
            insertTask(seg_task, ready_lock);
            return true;
        }
        // If there exist some task that will become "DataReady", then we should
        // wait. Else we should return true to end the wait.
        bool has_more_tasks = (ready_segment_tasks.count(SegmentReadTaskState::Init) > 0
                               || ready_segment_tasks.count(SegmentReadTaskState::Receiving) > 0);
        return !has_more_tasks;
    });
    return seg_task;
}

RNRemoteSegmentReadTaskPtr RNRemoteReadTask::nextReadyTask()
{
    std::unique_lock ready_lock(mtx_ready_tasks);
    RNRemoteSegmentReadTaskPtr seg_task = nullptr;
    cv_ready_tasks.wait(ready_lock, [this, &seg_task] {
        // All segment task are processed, return a nullptr
        if (doneOrErrorHappen())
            return true;

        // First check whether there are prepared segment task
        if (auto iter = ready_segment_tasks.find(SegmentReadTaskState::DataReadyAndPrepared); iter != ready_segment_tasks.end())
        {
            if (!iter->second.empty())
            {
                seg_task = iter->second.front();
                iter->second.pop_front();
                if (iter->second.empty())
                {
                    ready_segment_tasks.erase(iter);
                }
                return true;
            }
        }
        // Else fallback to check whether there are segment task ready for reading
        if (auto iter = ready_segment_tasks.find(SegmentReadTaskState::DataReady); iter != ready_segment_tasks.end())
        {
            if (iter->second.empty())
                return false; // yield and wait for next check
            seg_task = iter->second.front();
            iter->second.pop_front();
            if (iter->second.empty())
            {
                ready_segment_tasks.erase(iter);
            }
            return true;
        }
        return false; // yield and wait for next check
    });

    return seg_task;
}

const String & RNRemoteReadTask::getErrorMessage() const
{
    std::unique_lock ready_lock(mtx_ready_tasks);
    return err_msg;
}

bool RNRemoteReadTask::doneOrErrorHappen() const
{
    // All finished
    if (ready_segment_tasks.empty())
        return true;
    auto iter = ready_segment_tasks.find(SegmentReadTaskState::Error);
    if (iter != ready_segment_tasks.end() && !iter->second.empty())
    {
        // some tasks run into error when fetching pages
        return true; // NOLINT(readability-simplify-boolean-expr)
    }
    return false;
}

/// RNRemoteStoreReadTask ///

RNRemoteStoreReadTask::RNRemoteStoreReadTask(
    StoreID store_id_,
    std::vector<RNRemotePhysicalTableReadTaskPtr> table_read_tasks_)
    : store_id(store_id_)
    , table_read_tasks(std::move(table_read_tasks_))
{
    cur_table_task = table_read_tasks.begin();
}

size_t RNRemoteStoreReadTask::numRemainTasks() const
{
    std::lock_guard guard(mtx_tasks);
    size_t num_segments = 0;
    for (const auto & table_task : table_read_tasks)
    {
        num_segments += table_task->numRemainTasks();
    }
    return num_segments;
}

RNRemoteSegmentReadTaskPtr RNRemoteStoreReadTask::nextTask()
{
    std::lock_guard guard(mtx_tasks);
    while (cur_table_task != table_read_tasks.end())
    {
        if (auto seg_task = (*cur_table_task)->nextTask(); seg_task != nullptr)
            return seg_task;
        ++cur_table_task;
    }
    return {};
}

/// RNRemotePhysicalTableReadTask ///

RNRemotePhysicalTableReadTaskPtr RNRemotePhysicalTableReadTask::buildFrom(
    const Context & db_context,
    const ScanContextPtr & scan_context,
    const StoreID store_id,
    const String & address,
    const DisaggTaskId & snapshot_id,
    const RemotePb::RemotePhysicalTable & remote_table,
    const LoggerPtr & log)
{
    // Deserialize from `DisaggregatedPhysicalTable`, this should also
    // ensure the local cache pages.
    auto table_task = std::make_shared<RNRemotePhysicalTableReadTask>(
        store_id,
        KeyspaceTableID{remote_table.keyspace_id(), remote_table.table_id()},
        snapshot_id,
        address);

    std::vector<std::future<RNRemoteSegmentReadTaskPtr>> futures;

    auto size = static_cast<size_t>(remote_table.segments().size());
    for (size_t idx = 0; idx < size; ++idx)
    {
        const auto & remote_seg = remote_table.segments(idx);

        auto task = std::make_shared<std::packaged_task<RNRemoteSegmentReadTaskPtr()>>([&, idx, size] {
            Stopwatch watch;
            SCOPE_EXIT({
                LOG_DEBUG(log, "Build RNRemoteSegmentReadTask finished, elapsed={}s task_idx={} task_total={} segment_id={}", watch.elapsedSeconds(), idx, size, remote_seg.segment_id());
            });

            return RNRemoteSegmentReadTask::buildFrom(
                db_context,
                scan_context,
                remote_seg,
                snapshot_id,
                table_task->store_id,
                table_task->ks_table_id,
                table_task->address,
                log);
        });

        futures.emplace_back(task->get_future());
        RNRemoteReadTaskPool::get().scheduleOrThrowOnError([task] { (*task)(); });
    }

    for (auto & f : futures)
        table_task->tasks.push_back(f.get());

    return table_task;
}

RNRemoteSegmentReadTaskPtr RNRemotePhysicalTableReadTask::nextTask()
{
    std::lock_guard gurad(mtx_tasks);
    if (tasks.empty())
        return nullptr;
    auto task = tasks.front();
    tasks.pop_front();
    return task;
}

/**
 * Remote segment
 */

Allocator<false> RNRemoteSegmentReadTask::allocator;

RNRemoteSegmentReadTask::RNRemoteSegmentReadTask(
    DisaggTaskId snapshot_id_,
    StoreID store_id_,
    KeyspaceTableID ks_table_id_,
    UInt64 segment_id_,
    String address_,
    LoggerPtr log_)
    : snapshot_id(std::move(snapshot_id_))
    , store_id(store_id_)
    , ks_table_id(ks_table_id_)
    , segment_id(segment_id_)
    , address(std::move(address_))
    , log(std::move(log_))
{
}

RNRemoteSegmentReadTaskPtr RNRemoteSegmentReadTask::buildFrom(
    const Context & db_context,
    const ScanContextPtr & scan_context,
    const RemotePb::RemoteSegment & proto,
    const DisaggTaskId & snapshot_id,
    StoreID store_id,
    KeyspaceTableID ks_table_id,
    const String & address,
    const LoggerPtr & log)
{
    RowKeyRange segment_range;
    {
        ReadBufferFromString rb(proto.key_range());
        segment_range = RowKeyRange::deserialize(rb);
    }
    RowKeyRanges read_ranges(proto.read_key_ranges_size());
    for (int i = 0; i < proto.read_key_ranges_size(); ++i)
    {
        ReadBufferFromString rb(proto.read_key_ranges(i));
        read_ranges[i] = RowKeyRange::deserialize(rb);
    }

    auto task = std::make_shared<RNRemoteSegmentReadTask>(
        snapshot_id,
        store_id,
        ks_table_id,
        proto.segment_id(),
        address,
        log);

    task->dm_context = std::make_shared<DMContext>(
        db_context,
        /* path_pool */ nullptr,
        /* storage_pool */ nullptr,
        /* min_version */ 0,
        ks_table_id.first,
        ks_table_id.second,
        /* is_common_handle */ segment_range.is_common_handle,
        /* rowkey_column_size */ segment_range.rowkey_column_size,
        db_context.getSettingsRef(),
        scan_context);

    task->segment = std::make_shared<Segment>(
        log,
        /*epoch*/ 0,
        segment_range,
        proto.segment_id(),
        /*next_segment_id*/ 0,
        nullptr,
        nullptr);
    task->read_ranges = std::move(read_ranges);

    task->segment_snap = Remote::Serializer::deserializeSegmentSnapshotFrom(
        *(task->dm_context),
        store_id,
        ks_table_id.second,
        proto);

    // Note: At this moment, we still cannot read from `task->segment_snap`,
    // because they are constructed using ColumnFileDataProviderNop.

    {
        auto persisted_cfs = task->segment_snap->delta->getPersistedFileSetSnapshot();
        std::vector<UInt64> persisted_ids;
        std::vector<size_t> persisted_sizes;
        persisted_ids.reserve(persisted_cfs->getColumnFileCount());
        persisted_sizes.reserve(persisted_cfs->getColumnFileCount());
        for (const auto & cfs : persisted_cfs->getColumnFiles())
        {
            if (auto * tiny = cfs->tryToTinyFile(); tiny)
            {
                persisted_ids.emplace_back(tiny->getDataPageId());
                persisted_sizes.emplace_back(tiny->getDataPageSize());
            }
        }

        task->delta_tinycf_page_ids = persisted_ids;
        task->delta_tinycf_page_sizes = persisted_sizes;

        LOG_INFO(log,
                 "Build RemoteSegmentReadTask, store_id={} keyspace_id={} table_id={} memtable_cfs={} persisted_cfs={}",
                 task->store_id,
                 task->ks_table_id.first,
                 task->ks_table_id.second,
                 task->segment_snap->delta->getMemTableSetSnapshot()->getColumnFileCount(),
                 task->segment_snap->delta->getPersistedFileSetSnapshot()->getColumnFileCount());
    }

    return task;
}

void RNRemoteSegmentReadTask::initColumnFileDataProvider(Remote::RNLocalPageCacheGuardPtr pages_guard)
{
    auto & data_provider = segment_snap->delta->getPersistedFileSetSnapshot()->data_provider;
    RUNTIME_CHECK(std::dynamic_pointer_cast<DM::ColumnFileDataProviderNop>(data_provider));

    auto page_cache = dm_context->db_context.getSharedContextDisagg()->rn_page_cache;
    data_provider = std::make_shared<Remote::ColumnFileDataProviderRNLocalPageCache>(
        page_cache,
        pages_guard,
        store_id,
        ks_table_id);
}

void RNRemoteSegmentReadTask::receivePage(RemotePb::RemotePage && remote_page)
{
    std::lock_guard lock(mtx_queue);
    const size_t buf_size = remote_page.data().size();

    // Use LocalPageCache
    auto oid = Remote::PageOID{
        .store_id = store_id,
        .ks_table_id = ks_table_id,
        .page_id = remote_page.page_id(),
    };
    auto read_buffer = std::make_shared<ReadBufferFromMemory>(remote_page.data().data(), buf_size);
    PageFieldSizes field_sizes;
    field_sizes.reserve(remote_page.field_sizes_size());
    for (const auto & field_sz : remote_page.field_sizes())
    {
        field_sizes.emplace_back(field_sz);
    }
    auto & page_cache = dm_context->db_context.getSharedContextDisagg()->rn_page_cache;
    page_cache->write(oid, std::move(read_buffer), buf_size, std::move(field_sizes));
    LOG_DEBUG(log, "receive page, oid={} segment_id={}", oid, segment->segmentId());
}

bool RNRemoteSegmentReadTask::addConsumedMsg()
{
    num_msg_consumed += 1;
    RUNTIME_CHECK(
        num_msg_consumed <= num_msg_to_consume,
        num_msg_consumed,
        num_msg_to_consume,
        segment->segmentId());

    // return there are more pending msg or not
    return num_msg_consumed < num_msg_to_consume;
}

void RNRemoteSegmentReadTask::prepare()
{
    // Do place index for full segment
    segment->placeDeltaIndex(*dm_context, segment_snap);
}

BlockInputStreamPtr RNRemoteSegmentReadTask::getInputStream(
    const ColumnDefines & columns_to_read,
    const RowKeyRanges & key_ranges,
    UInt64 read_tso,
    const PushDownFilterPtr & push_down_filter,
    size_t expected_block_size,
    ReadMode read_mode)
{
    return segment->getInputStream(
        read_mode,
        *dm_context,
        columns_to_read,
        segment_snap,
        key_ranges,
        push_down_filter,
        read_tso,
        expected_block_size);
}

} // namespace DB::DM
