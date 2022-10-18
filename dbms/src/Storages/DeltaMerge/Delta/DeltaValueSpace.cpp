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

#include <Functions/FunctionHelpers.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/DeltaIndexManager.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace DM
{
// ================================================
// Public methods
// ================================================
DeltaValueSpace::DeltaValueSpace(PageId id_, const ColumnFilePersisteds & persisted_files, const ColumnFiles & in_memory_files)
    : persisted_file_set(std::make_shared<ColumnFilePersistedSet>(id_, persisted_files))
    , mem_table_set(std::make_shared<MemTableSet>(persisted_file_set->getLastSchema(), in_memory_files))
    , delta_index(std::make_shared<DeltaIndex>())
    , log(Logger::get())
{}

DeltaValueSpace::DeltaValueSpace(ColumnFilePersistedSetPtr && persisted_file_set_)
    : persisted_file_set(std::move(persisted_file_set_))
    , mem_table_set(std::make_shared<MemTableSet>(persisted_file_set->getLastSchema()))
    , delta_index(std::make_shared<DeltaIndex>())
    , log(Logger::get())
{}

void DeltaValueSpace::abandon(DMContext & context)
{
    bool v = false;
    if (!abandoned.compare_exchange_strong(v, true))
        throw Exception("Try to abandon a already abandoned DeltaValueSpace", ErrorCodes::LOGICAL_ERROR);

    if (auto manager = context.db_context.getDeltaIndexManager(); manager)
        manager->deleteRef(delta_index);
}

DeltaValueSpacePtr DeltaValueSpace::restore(DMContext & context, const RowKeyRange & segment_range, PageId id)
{
    auto persisted_file_set = ColumnFilePersistedSet::restore(context, segment_range, id);
    return std::make_shared<DeltaValueSpace>(std::move(persisted_file_set));
}

void DeltaValueSpace::saveMeta(WriteBatches & wbs) const
{
    persisted_file_set->saveMeta(wbs);
}

std::pair<ColumnFilePersisteds, ColumnFiles>
DeltaValueSpace::checkHeadAndCloneTail(DMContext & context,
                                       const RowKeyRange & target_range,
                                       const ColumnFiles & head_column_files,
                                       WriteBatches & wbs) const
{
    auto tail_persisted_files = persisted_file_set->checkHeadAndCloneTail(context, target_range, head_column_files, wbs);
    auto memory_files = mem_table_set->cloneColumnFiles(context, target_range, wbs);
    return std::make_pair(std::move(tail_persisted_files), std::move(memory_files));
}

size_t DeltaValueSpace::getTotalCacheRows() const
{
    std::scoped_lock lock(mutex);
    return mem_table_set->getRows() + persisted_file_set->getTotalCacheRows();
}

size_t DeltaValueSpace::getTotalCacheBytes() const
{
    std::scoped_lock lock(mutex);
    return mem_table_set->getBytes() + persisted_file_set->getTotalCacheBytes();
}

size_t DeltaValueSpace::getValidCacheRows() const
{
    std::scoped_lock lock(mutex);
    return mem_table_set->getRows() + persisted_file_set->getValidCacheRows();
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

bool DeltaValueSpace::ingestColumnFiles(DMContext & /*context*/, const RowKeyRange & range, const ColumnFiles & column_files, bool clear_data_in_range)
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
            throw Exception(fmt::format("Delta is expected to be flushing, delta={}", simpleInfo()), ErrorCodes::LOGICAL_ERROR);
    });

    LOG_DEBUG(log, "Flush start, delta={}", info());

    /// We have two types of data needed to flush to disk:
    ///  1. The cache data in ColumnFileInMemory
    ///  2. The serialized metadata of column files in DeltaValueSpace

    ColumnFileFlushTaskPtr flush_task;
    WriteBatches wbs(context.storage_pool, context.getWriteLimiter());
    DeltaIndexPtr cur_delta_index;
    {
        /// Prepare data which will be written to disk.
        std::scoped_lock lock(mutex);
        if (abandoned.load(std::memory_order_relaxed))
        {
            LOG_DEBUG(log, "Flush stop because abandoned, delta={}", simpleInfo());
            return false;
        }
        flush_task = mem_table_set->buildFlushTask(context, persisted_file_set->getRows(), persisted_file_set->getDeletes(), persisted_file_set->getCurrentFlushVersion());
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
            delta_index = new_delta_index;

        LOG_DEBUG(log, "Flush end, flush_tasks={} flush_rows={} flush_deletes={} delta={}", flush_task->getTaskNum(), flush_task->getFlushRows(), flush_task->getFlushDeletes(), info());
    }
    return true;
}

bool DeltaValueSpace::compact(DMContext & context)
{
    bool v = false;
    // Other thread is doing structure update, just return.
    if (!is_updating.compare_exchange_strong(v, true))
    {
        LOG_DEBUG(log, "Compact stop because updating, delta={}", simpleInfo());
        return true;
    }
    SCOPE_EXIT({
        bool v = true;
        if (!is_updating.compare_exchange_strong(v, false))
            throw Exception(fmt::format("Delta is expected to be flushing, delta={}", simpleInfo()), ErrorCodes::LOGICAL_ERROR);
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
        log_storage_snap = context.storage_pool.logReader()->getSnapshot(/*tracing_id*/ fmt::format("minor_compact_{}", simpleInfo()));
    }

    // do compaction task
    WriteBatches wbs(context.storage_pool, context.getWriteLimiter());
    const auto & reader = context.storage_pool.newLogReader(context.getReadLimiter(), log_storage_snap);
    compaction_task->prepare(context, wbs, reader);

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

        LOG_DEBUG(log, "{} delta={}", compaction_task->info(), info());
    }
    wbs.writeRemoves();

    return true;
}
} // namespace DM
} // namespace DB
