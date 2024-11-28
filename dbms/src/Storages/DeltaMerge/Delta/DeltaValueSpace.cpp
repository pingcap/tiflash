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

#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Functions/FunctionHelpers.h>
#include <IO/Buffer/MemoryReadWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/DeltaIndexManager.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/PathPool.h>

#include <ext/scope_guard.h>

namespace DB::DM
{

// ================================================
// Public methods
// ================================================
DeltaValueSpace::DeltaValueSpace(
    PageIdU64 id_,
    const ColumnFilePersisteds & persisted_files,
    const ColumnFiles & in_memory_files)
    : persisted_file_set(std::make_shared<ColumnFilePersistedSet>(id_, persisted_files))
    , mem_table_set(std::make_shared<MemTableSet>(in_memory_files))
    , delta_index(std::make_shared<DeltaIndex>())
    , log(Logger::get())
{}

DeltaValueSpace::DeltaValueSpace(ColumnFilePersistedSetPtr && persisted_file_set_)
    : persisted_file_set(std::move(persisted_file_set_))
    , mem_table_set(std::make_shared<MemTableSet>())
    , delta_index(std::make_shared<DeltaIndex>())
    , log(Logger::get())
{}

void DeltaValueSpace::abandon(DMContext & dm_context)
{
    bool v = false;
    if (!abandoned.compare_exchange_strong(v, true))
        throw Exception("Try to abandon a already abandoned DeltaValueSpace", ErrorCodes::LOGICAL_ERROR);

    if (auto manager = dm_context.global_context.getDeltaIndexManager(); manager)
        manager->deleteRef(delta_index);
}

DeltaValueSpacePtr DeltaValueSpace::restore(DMContext & context, const RowKeyRange & segment_range, PageIdU64 id)
{
    auto persisted_file_set = ColumnFilePersistedSet::restore(context, segment_range, id);
    return std::make_shared<DeltaValueSpace>(std::move(persisted_file_set));
}

DeltaValueSpacePtr DeltaValueSpace::restore(
    DMContext & context,
    const RowKeyRange & segment_range,
    ReadBuffer & buf,
    PageIdU64 id)
{
    auto persisted_file_set = ColumnFilePersistedSet::restore(context, segment_range, buf, id);
    return std::make_shared<DeltaValueSpace>(std::move(persisted_file_set));
}

DeltaValueSpacePtr DeltaValueSpace::createFromCheckpoint( //
    const LoggerPtr & parent_log,
    DMContext & context,
    UniversalPageStoragePtr temp_ps,
    const RowKeyRange & segment_range,
    PageIdU64 delta_id,
    WriteBatches & wbs)
{
    auto persisted_file_set
        = ColumnFilePersistedSet::createFromCheckpoint(parent_log, context, temp_ps, segment_range, delta_id, wbs);
    return std::make_shared<DeltaValueSpace>(std::move(persisted_file_set));
}

void DeltaValueSpace::saveMeta(WriteBuffer & buf) const
{
    persisted_file_set->saveMeta(buf);
}

void DeltaValueSpace::saveMeta(WriteBatches & wbs) const
{
    persisted_file_set->saveMeta(wbs);
}

std::string DeltaValueSpace::serializeMeta() const
{
    WriteBufferFromOwnString wb;
    saveMeta(wb);
    return wb.releaseStr();
}

template <class ColumnFilePtrT>
std::vector<ColumnFilePtrT> CloneColumnFilesHelper<ColumnFilePtrT>::clone(
    DMContext & dm_context,
    const std::vector<ColumnFilePtrT> & src,
    const RowKeyRange & target_range,
    WriteBatches & wbs)
{
    std::vector<ColumnFilePtrT> cloned;
    cloned.reserve(src.size());

    for (auto & column_file : src)
    {
        if constexpr (std::is_same_v<ColumnFilePtrT, ColumnFilePtr>)
        {
            if (auto * b = column_file->tryToInMemoryFile(); b)
            {
                auto new_column_file = b->clone();

                // No matter or what, don't append to column files which cloned from old column file again.
                // Because they could shared the same cache. And the cache can NOT be inserted from different column files in different delta.
                new_column_file->disableAppend();
                cloned.push_back(new_column_file);
                continue;
            }
        }

        if (auto * dr = column_file->tryToDeleteRange(); dr)
        {
            auto new_dr = dr->getDeleteRange().shrink(target_range);
            if (!new_dr.none())
            {
                // Only use the available delete_range column file.
                cloned.push_back(dr->cloneWith(new_dr));
            }
        }
        else if (auto * t = column_file->tryToTinyFile(); t)
        {
            // Use a newly created page_id to reference the data page_id of current column file.
            PageIdU64 new_data_page_id = dm_context.storage_pool->newLogPageId();
            wbs.log.putRefPage(new_data_page_id, t->getDataPageId());
            if (auto index_infos = t->getIndexInfos(); index_infos)
            {
                auto new_index_infos = std::make_shared<ColumnFileTiny::IndexInfos>();
                new_index_infos->reserve(index_infos->size());
                // Use a newly created page_id to reference the index page_id of current column file.
                for (auto & index_info : *index_infos)
                {
                    auto new_index_page_id = dm_context.storage_pool->newLogPageId();
                    wbs.log.putRefPage(new_index_page_id, index_info.index_page_id);
                    new_index_infos->emplace_back(new_index_page_id, index_info.vector_index);
                }
                auto new_column_file = t->cloneWith(new_data_page_id, new_index_infos);
                cloned.push_back(new_column_file);
                continue;
            }
            auto new_column_file = t->cloneWith(new_data_page_id);
            cloned.push_back(new_column_file);
        }
        else if (auto * f = column_file->tryToBigFile(); f)
        {
            auto delegator = dm_context.path_pool->getStableDiskDelegator();
            auto new_page_id = dm_context.storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
            // Note that the file id may has already been mark as deleted. We must
            // create a reference to the page id itself instead of create a reference
            // to the file id.
            wbs.data.putRefPage(new_page_id, f->getDataPageId());
            auto file_id = f->getFile()->fileId();
            auto old_dmfile = f->getFile();
            auto file_parent_path = old_dmfile->parentPath();
            if (!dm_context.global_context.getSharedContextDisagg()->remote_data_store)
            {
                RUNTIME_CHECK(file_parent_path == delegator.getDTFilePath(file_id));
            }
            auto new_file = DMFile::restore(
                dm_context.global_context.getFileProvider(),
                file_id,
                /* page_id= */ new_page_id,
                file_parent_path,
                DMFileMeta::ReadMode::all(),
                old_dmfile->metaVersion(),
                dm_context.keyspace_id);
            auto new_column_file = f->cloneWith(dm_context, new_file, target_range);
            cloned.push_back(new_column_file);
        }
        else
        {
            throw Exception("Meet unknown type of column file", ErrorCodes::LOGICAL_ERROR);
        }
    }
    return cloned;
}

template struct CloneColumnFilesHelper<ColumnFilePtr>;
template struct CloneColumnFilesHelper<ColumnFilePersistedPtr>;

std::pair<ColumnFiles, ColumnFilePersisteds> DeltaValueSpace::cloneNewlyAppendedColumnFiles(
    const DeltaValueSpace::Lock &,
    DMContext & context,
    const RowKeyRange & target_range,
    const DeltaValueSnapshot & update_snapshot,
    WriteBatches & wbs) const
{
    RUNTIME_CHECK(update_snapshot.is_update);

    const auto & snapshot_mem_files = update_snapshot.getMemTableSetSnapshot()->getColumnFiles();
    const auto & snapshot_persisted_files = update_snapshot.getPersistedFileSetSnapshot()->getColumnFiles();

    auto [new_mem_files, flushed_mem_files] = mem_table_set->diffColumnFiles(snapshot_mem_files);
    ColumnFiles head_persisted_files;
    head_persisted_files.reserve(snapshot_persisted_files.size() + flushed_mem_files.size());
    head_persisted_files.insert(
        head_persisted_files.end(),
        snapshot_persisted_files.begin(),
        snapshot_persisted_files.end());
    // If there were flush since the snapshot, the flushed files should be behind the files in the snapshot.
    // So let's place these "flused files" after the persisted files in snapshot.
    head_persisted_files.insert(head_persisted_files.end(), flushed_mem_files.begin(), flushed_mem_files.end());

    auto new_persisted_files = persisted_file_set->diffColumnFiles(head_persisted_files);

    // The "newly added column files" + "files in snapshot" should be equal to current files.
    RUNTIME_CHECK(
        mem_table_set->getColumnFileCount() + persisted_file_set->getColumnFileCount() == //
        snapshot_mem_files.size() + snapshot_persisted_files.size() + //
            new_mem_files.size() + new_persisted_files.size());

    return {
        CloneColumnFilesHelper<ColumnFilePtr>::clone(context, new_mem_files, target_range, wbs),
        CloneColumnFilesHelper<ColumnFilePersistedPtr>::clone(context, new_persisted_files, target_range, wbs),
    };
}

std::pair<ColumnFiles, ColumnFilePersisteds> DeltaValueSpace::cloneAllColumnFiles(
    const DeltaValueSpace::Lock &,
    DMContext & context,
    const RowKeyRange & target_range,
    WriteBatches & wbs) const
{
    auto [new_mem_files, flushed_mem_files] = mem_table_set->diffColumnFiles({});
    // As we are diffing the current memtable with an empty snapshot,
    // we expect everything in the current memtable are "newly added" compared to this "empty snapshot",
    // and none of the files in the "empty snapshot" was flushed.
    RUNTIME_CHECK(flushed_mem_files.empty());
    RUNTIME_CHECK(new_mem_files.size() == mem_table_set->getColumnFileCount());

    // We are diffing with an empty list, so everything in the current persisted layer
    // should be considered as "newly added".
    auto new_persisted_files = persisted_file_set->diffColumnFiles({});
    RUNTIME_CHECK(new_persisted_files.size() == persisted_file_set->getColumnFileCount());

    return {
        CloneColumnFilesHelper<ColumnFilePtr>::clone(context, new_mem_files, target_range, wbs),
        CloneColumnFilesHelper<ColumnFilePersistedPtr>::clone(context, new_persisted_files, target_range, wbs),
    };
}

size_t DeltaValueSpace::getTotalCacheRows() const
{
    std::scoped_lock lock(mutex);
    return mem_table_set->getRows();
}

size_t DeltaValueSpace::getTotalCacheBytes() const
{
    std::scoped_lock lock(mutex);
    return mem_table_set->getBytes();
}

size_t DeltaValueSpace::getValidCacheRows() const
{
    std::scoped_lock lock(mutex);
    return mem_table_set->getRows();
}

void DeltaValueSpace::recordRemoveColumnFilesPages(WriteBatches & wbs) const
{
    persisted_file_set->recordRemoveColumnFilesPages(wbs);
    // there could be some persisted column files in the `mem_table_set` which should be removed.
    mem_table_set->recordRemoveColumnFilesPages(wbs);
}

bool DeltaValueSpace::appendColumnFile(DMContext & /*context*/, const ColumnFilePtr & column_file)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    mem_table_set->appendColumnFile(column_file);
    return true;
}

bool DeltaValueSpace::appendToCache(DMContext & context, const Block & block, size_t offset, size_t limit)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    mem_table_set->appendToCache(context, block, offset, limit);
    return true;
}

bool DeltaValueSpace::appendDeleteRange(DMContext & /*context*/, const RowKeyRange & delete_range)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    mem_table_set->appendDeleteRange(delete_range);
    return true;
}

bool DeltaValueSpace::ingestColumnFiles(
    DMContext & /*context*/,
    const RowKeyRange & range,
    const ColumnFiles & column_files,
    bool clear_data_in_range)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    mem_table_set->ingestColumnFiles(range, column_files, clear_data_in_range);
    return true;
}

bool DeltaValueSpace::flush(DMContext & context)
{
    bool v = false;
    if (!is_flushing.compare_exchange_strong(v, true))
    {
        // other thread is flushing, just return.
        LOG_DEBUG(log, "Flush stop because other thread is flushing, delta={}", simpleInfo());
        return false;
    }
    SCOPE_EXIT({
        bool v = true;
        if (!is_flushing.compare_exchange_strong(v, false))
            throw Exception(
                fmt::format("Delta is expected to be flushing, delta={}", simpleInfo()),
                ErrorCodes::LOGICAL_ERROR);
    });

    LOG_DEBUG(log, "Flush start, delta={}", info());

    /// We have two types of data needed to flush to disk:
    ///  1. The cache data in ColumnFileInMemory
    ///  2. The serialized metadata of column files in DeltaValueSpace

    ColumnFileFlushTaskPtr flush_task;
    WriteBatches wbs(*context.storage_pool, context.getWriteLimiter());
    DeltaIndexPtr cur_delta_index;
    {
        /// Prepare data which will be written to disk.
        std::scoped_lock lock(mutex);
        if (abandoned.load(std::memory_order_relaxed))
        {
            LOG_DEBUG(log, "Flush stop because abandoned, delta={}", simpleInfo());
            return false;
        }
        flush_task = mem_table_set->buildFlushTask(
            context,
            persisted_file_set->getRows(),
            persisted_file_set->getDeletes(),
            persisted_file_set->getCurrentFlushVersion());
        cur_delta_index = delta_index;
    }

    // No update, return successfully.
    if (!flush_task)
    {
        LOG_DEBUG(log, "Flush cancel because nothing to flush, delta={}", simpleInfo());
        return true;
    }

    /// Write prepared data to disk.
    auto delta_index_updates = flush_task->prepare(wbs);
    DeltaIndexPtr new_delta_index;
    if (!delta_index_updates.empty())
    {
        LOG_DEBUG(log, "Update index start, delta={}", simpleInfo());
        new_delta_index = cur_delta_index->cloneWithUpdates(delta_index_updates);
        LOG_DEBUG(log, "Update index done, delta={}", simpleInfo());
    }
    GET_METRIC(tiflash_storage_subtask_throughput_bytes, type_delta_flush).Increment(flush_task->getFlushBytes());
    GET_METRIC(tiflash_storage_subtask_throughput_rows, type_delta_flush).Increment(flush_task->getFlushRows());

    SYNC_FOR("after_DeltaValueSpace::flush|prepare_flush");

    {
        /// If this instance is still valid, then commit.
        std::scoped_lock lock(mutex);
        if (abandoned.load(std::memory_order_relaxed))
        {
            // Delete written data.
            wbs.setRollback();
            LOG_DEBUG(log, "Flush stop because abandoned, delta={}", simpleInfo());
            return false;
        }

        if (!flush_task->commit(persisted_file_set, wbs))
        {
            wbs.rollbackWrittenLogAndData();
            LOG_DEBUG(log, "Flush stop because structure got updated, delta={}", simpleInfo());
            return false;
        }

        /// Update delta tree
        if (new_delta_index)
        {
            delta_index = new_delta_index;

            // Indicate that the index with old epoch should not be used anymore.
            // This is useful in disaggregated mode which will invalidate the delta index cache in RN.
            delta_index_epoch = std::chrono::steady_clock::now().time_since_epoch().count();
        }

        LOG_DEBUG(
            log,
            "Flush end, flush_tasks={} flush_rows={} flush_bytes={} flush_deletes={} delta={}",
            flush_task->getTaskNum(),
            flush_task->getFlushRows(),
            flush_task->getFlushBytes(),
            flush_task->getFlushDeletes(),
            info());
    }
    return true;
}

bool DeltaValueSpace::compact(DMContext & context)
{
    if (!tryLockUpdating())
        return true;
    SCOPE_EXIT({
        auto released = releaseUpdating();
        RUNTIME_CHECK(released, simpleInfo());
    });

    LOG_DEBUG(log, "Compact start, delta={}", info());

    MinorCompactionPtr compaction_task;
    PageStorage::SnapshotPtr log_storage_snap;
    {
        std::scoped_lock lock(mutex);
        if (abandoned.load(std::memory_order_relaxed))
        {
            LOG_DEBUG(log, "Compact stop because abandoned, delta={}", simpleInfo());
            return false;
        }
        compaction_task = persisted_file_set->pickUpMinorCompaction(context);
        if (!compaction_task)
        {
            LOG_DEBUG(log, "Compact cancel because nothing to compact, delta={}", simpleInfo());
            return true;
        }
        log_storage_snap = context.storage_pool->logReader()->getSnapshot(
            /*tracing_id*/ fmt::format("minor_compact_{}", simpleInfo()));
    }

    WriteBatches wbs(*context.storage_pool, context.getWriteLimiter());
    {
        // do compaction task
        const auto & reader = context.storage_pool->newLogReader(context.getReadLimiter(), log_storage_snap);
        compaction_task->prepare(context, wbs, *reader);
        log_storage_snap.reset(); // release the snapshot ASAP
    }

    GET_METRIC(tiflash_storage_subtask_throughput_bytes, type_delta_compact)
        .Increment(compaction_task->getTotalCompactBytes());
    GET_METRIC(tiflash_storage_subtask_throughput_rows, type_delta_compact)
        .Increment(compaction_task->getTotalCompactRows());

    {
        std::scoped_lock lock(mutex);

        /// Check before commit.
        if (abandoned.load(std::memory_order_relaxed))
        {
            wbs.rollbackWrittenLogAndData();
            LOG_DEBUG(log, "Compact stop because abandoned, delta={}", simpleInfo());
            return false;
        }
        if (!compaction_task->commit(persisted_file_set, wbs))
        {
            LOG_WARNING(log, "Structure has been updated during compact, delta={}", simpleInfo());
            wbs.rollbackWrittenLogAndData();
            LOG_DEBUG(log, "Compact stop because structure got updated, delta={}", simpleInfo());
            return false;
        }
        // Reset to the index of first file that can be compacted if the minor compaction succeed,
        // and it may trigger another minor compaction if there is still too many column files.
        // This process will stop when there is no more minor compaction to be done.
        auto first_compact_index = compaction_task->getFirsCompactIndex();
        RUNTIME_ASSERT(first_compact_index != std::numeric_limits<size_t>::max());
        last_try_compact_column_files.store(first_compact_index);
        LOG_DEBUG(log, "{} delta={}", compaction_task->info(), info());
    }
    wbs.writeRemoves();

    return true;
}

} // namespace DB::DM
