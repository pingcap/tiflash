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

#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/MemTableSet.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/PathPool.h>

namespace DB::DM
{

void MemTableSet::appendColumnFileInner(const ColumnFilePtr & column_file)
{
    if (!column_files.empty())
    {
        // As we are now appending a new column file (which can be used for new appends),
        // let's simply mark the last column file as not appendable.
        auto & last_column_file = column_files.back();
        if (last_column_file->isAppendable())
            last_column_file->disableAppend();
    }

    column_files.push_back(column_file);
    column_files_count = column_files.size();

    rows += column_file->getRows();
    bytes += column_file->getBytes();
    deletes += column_file->getDeletes();
}

std::pair</* New */ ColumnFiles, /* Flushed */ ColumnFiles> MemTableSet::diffColumnFiles(
    const ColumnFiles & column_files_in_snapshot) const
{
    /**
     * Suppose we have A, B, C in the snapshot:
     * ┌───┬───┬───┐
     * │ A │ B │ C │
     * └───┴───┴───┘
     *
     * Case #1:
     *
     * The most simple case is that, there was no flush happened since the snapshot was created last time.
     * For example, our memtable may looks like:
     * ┌───┬───┬───┬───┬───┐
     * │ A │ B │ C │ D │ E │
     * └───┴───┴───┴───┴───┘
     * In this case, we return { D, E } as "new":
     *             ┌───┬───┐
     *             │ D │ E │  New Column Files
     *             └───┴───┘
     *              (empty)   Flushed Column Files
     *
     * Case #2:
     *
     * Then, let's think about flush. There could be flush since the snapshot was created last time.
     * For example, suppose we have these ops since taking the snapshot:
     * - write D
     * - flush A B (but not C)
     * - write E
     * - write F
     * Our memtable and persisted looks like this now:
     *         ┌───┬───┬───┬───┐
     *         │ C │ D │ E │ F │ MemTable
     *         └───┴───┴───┴───┘
     * ┌───┬───┐
     * │ A │ B │                 Persisted
     * └───┴───┘
     *
     * Remember our snapshot:
     * ┌───┬───┬───┐
     * │ A │ B │ C │
     * └───┴───┴───┘
     *
     * Our returned value is very simple. No need to do a full diff:
     * ┌───┬───┐
     * │ A │ B │ Flushed Column Files
     * └───┴───┘
     *             ┌───┬───┬───┐
     *             │ D │ E │ F │ New Column Files
     *             └───┴───┴───┘
     *
     * Finally, we can treat case #1 as a special case #2: the flushed_n == 0.
     */

    // Note: This implementation does not do a full diff.
    // It heavily relies on how Flush is working.

    if (column_files_in_snapshot.empty())
        return {/* new */ column_files, /* flushed */ {}};

    if (column_files.empty())
        return {/* new */ {}, /* flushed */ column_files_in_snapshot};


    //  ┌───┬───┬───┐
    //  │ A │ B │ C │               Snapshot
    //  └───┴───┴───┘
    //          ┌───┬───┬───┬───┐
    //          │ C │ D │ E │ F │   MemTable
    //          └───┴───┴───┴───┘
    //  ┌───┬───┐
    //  │ A │ B │                   Persisted
    //  └───┴───┘
    //          ^^^^^               unflushed_n = 1
    //  ^^^^^^^^                    flushed_n = 2


    // When there is a flush, may be not everything in the Snapshot is flushed.
    // It is possible that only a prefix of the Snapshot is flushed.
    // So let's check how long the flushed prefix is. The prefix could be 0.
    size_t flushed_n = 0;
    while (flushed_n < column_files_in_snapshot.size())
    {
        if (column_files[0]->getId() == column_files_in_snapshot[flushed_n]->getId())
            break;
        flushed_n++;
    }
    // For Snapshot CFs [0, flushed_n), they are flushed column files.
    // Remaining Snapshot CFs must be not flushed, remains in the memtable and is the prefix of the memtable.
    RUNTIME_CHECK(
        flushed_n <= column_files_in_snapshot.size(),
        columnFilesToString(column_files_in_snapshot),
        columnFilesToString(column_files));
    size_t unflushed_n = column_files_in_snapshot.size() - flushed_n;
    RUNTIME_CHECK( // Those unflushed CFs must be still in memtable.
        column_files.size() >= unflushed_n,
        columnFilesToString(column_files_in_snapshot),
        columnFilesToString(column_files));
    for (size_t i = 0; i < unflushed_n; ++i)
    {
        RUNTIME_CHECK( // Verify prefix
            column_files_in_snapshot[flushed_n + i]->getId() == column_files[i]->getId()
                && column_files_in_snapshot[flushed_n + i]->getType() == column_files[i]->getType()
                && column_files_in_snapshot[flushed_n + i]->getRows() == column_files[i]->getRows(),
            columnFilesToString(column_files_in_snapshot),
            columnFilesToString(column_files));
    }
    return {
        /* new */ std::vector<ColumnFilePtr>( //
            column_files.begin() + unflushed_n,
            column_files.end()),
        /* flushed */
        std::vector<ColumnFilePtr>( //
            column_files_in_snapshot.begin(),
            column_files_in_snapshot.begin() + flushed_n),
    };
}

void MemTableSet::recordRemoveColumnFilesPages(WriteBatches & wbs) const
{
    for (const auto & column_file : column_files)
    {
        if (auto * p = column_file->tryToColumnFilePersisted(); p)
        {
            p->removeData(wbs);
        }
    }
}

void MemTableSet::appendColumnFile(const ColumnFilePtr & column_file)
{
    appendColumnFileInner(column_file);
}

void MemTableSet::appendToCache(DMContext & context, const Block & block, size_t offset, size_t limit)
{
    // If the `column_files` is not empty, and the last `column_file` is a `ColumnInMemoryFile`, we will merge the newly block into the last `column_file`.
    // Otherwise, create a new `ColumnInMemoryFile` and write into it.
    bool success = false;
    size_t append_bytes = block.bytes(offset, limit);
    if (!column_files.empty())
    {
        auto & last_column_file = column_files.back();
        if (last_column_file->isAppendable())
            success = last_column_file->append(context, block, offset, limit, append_bytes);
    }

    if (!success)
    {
        auto schema = getSharedBlockSchemas(context)->getOrCreate(block);

        // Create a new column file.
        auto new_column_file = std::make_shared<ColumnFileInMemory>(schema);
        // Must append the empty `new_column_file` to `column_files` before appending data to it,
        // because `appendColumnFileInner` will update stats related to `column_files` but we will update stats relate to `new_column_file` here.
        appendColumnFileInner(new_column_file);
        success = new_column_file->append(context, block, offset, limit, append_bytes);
        if (unlikely(!success))
            throw Exception("Write to MemTableSet failed", ErrorCodes::LOGICAL_ERROR);
    }
    rows += limit;
    bytes += append_bytes;
}

void MemTableSet::appendDeleteRange(const RowKeyRange & delete_range)
{
    auto f = std::make_shared<ColumnFileDeleteRange>(delete_range);
    appendColumnFileInner(f);
}

void MemTableSet::ingestColumnFiles(
    const RowKeyRange & range,
    const ColumnFiles & new_column_files,
    bool clear_data_in_range)
{
    for (const auto & f : new_column_files)
        RUNTIME_CHECK(f->isBigFile());

    // Prepend a DeleteRange to clean data before applying column files
    if (clear_data_in_range)
    {
        auto f = std::make_shared<ColumnFileDeleteRange>(range);
        appendColumnFileInner(f);
    }

    for (const auto & f : new_column_files)
        appendColumnFileInner(f);
}

ColumnFileSetSnapshotPtr MemTableSet::createSnapshot(
    const IColumnFileDataProviderPtr & data_provider,
    bool disable_sharing)
{
    // Disable append, so that new writes will not touch the content of this snapshot.
    // This could lead to more fragmented IOs, so we don't do it for all snapshots.
    if (disable_sharing && !column_files.empty() && column_files.back()->isAppendable())
        column_files.back()->disableAppend();

    size_t total_rows = 0;
    size_t total_deletes = 0;
    ColumnFiles column_files_snap;
    column_files_snap.reserve(column_files.size());
    for (const auto & file : column_files)
    {
        // ColumnFile is not a thread-safe object, but only ColumnFileInMemory may be appendable after its creation.
        // So we only clone the instance of ColumnFileInMemory here.
        if (auto * m = file->tryToInMemoryFile(); m)
        {
            // Compact threads could update the value of ColumnFileInMemory,
            // and since ColumnFile is not multi-threads safe, we should create a new column file object.
            // TODO: When `disable_sharing == true`, may be we can safely use the same ptr without the clone.
            column_files_snap.emplace_back(m->clone());
        }
        else
        {
            column_files_snap.emplace_back(file);
        }
        total_rows += file->getRows();
        total_deletes += file->getDeletes();
    }

    // This may indicate that you forget to acquire a lock -- there are modifications
    // while this function is still running...
    RUNTIME_CHECK(
        total_rows == rows && total_deletes == deletes,
        total_rows,
        rows.load(),
        total_deletes,
        deletes.load());

    return std::make_shared<ColumnFileSetSnapshot>(data_provider, std::move(column_files_snap), rows, bytes, deletes);
}

ColumnFileFlushTaskPtr MemTableSet::buildFlushTask(
    DMContext & context,
    size_t rows_offset,
    size_t deletes_offset,
    size_t flush_version)
{
    if (column_files.empty())
        return nullptr;

    // Mark the last ColumnFile not appendable, so that `appendToCache` will not reuse it and we will be safe to flush it to disk.
    if (column_files.back()->isAppendable())
        column_files.back()->disableAppend();

    size_t cur_rows_offset = rows_offset;
    size_t cur_deletes_offset = deletes_offset;
    auto flush_task = std::make_shared<ColumnFileFlushTask>(context, this->shared_from_this(), flush_version);
    for (auto & column_file : column_files)
    {
        auto & task = flush_task->addColumnFile(column_file);
        if (auto * m_file = column_file->tryToInMemoryFile(); m_file)
        {
            // If the ColumnFile is not yet persisted in the disk, it will contain block data.
            // In this case, let's write the block data in the flush process as well.
            task.rows_offset = cur_rows_offset;
            task.deletes_offset = cur_deletes_offset;
            task.block_data = m_file->readDataForFlush();
        }
        cur_rows_offset += column_file->getRows();
        cur_deletes_offset += column_file->getDeletes();
    }
    if (unlikely(flush_task->getFlushRows() != rows || flush_task->getFlushDeletes() != deletes))
    {
        LOG_ERROR(
            log,
            "Rows and deletes check failed. Actual: rows[{}], deletes[{}]. Expected: rows[{}], deletes[{}]. Column "
            "Files: {}",
            flush_task->getFlushRows(),
            flush_task->getFlushDeletes(),
            rows.load(),
            deletes.load(),
            columnFilesToString(column_files));
        throw Exception("Rows and deletes check failed.", ErrorCodes::LOGICAL_ERROR);
    }

    return flush_task;
}

void MemTableSet::removeColumnFilesInFlushTask(const ColumnFileFlushTask & flush_task)
{
    const auto & tasks = flush_task.getAllTasks();
    // There may be new column files appended at back, but should never be files removed.
    {
        RUNTIME_CHECK(tasks.size() <= column_files.size());
        for (size_t i = 0; i < tasks.size(); ++i)
            RUNTIME_CHECK(tasks[i].column_file == column_files[i]);
    }

    ColumnFiles new_column_files;
    if (column_files.size() > tasks.size())
        new_column_files.reserve(column_files.size() - tasks.size());

    size_t new_rows = 0;
    size_t new_bytes = 0;
    size_t new_deletes = 0;
    for (size_t i = tasks.size(); i < column_files.size(); ++i)
    {
        auto & column_file = column_files[i];
        new_column_files.emplace_back(column_file);
        new_rows += column_file->getRows();
        new_bytes += column_file->getBytes();
        new_deletes += column_file->getDeletes();
    }
    column_files.swap(new_column_files);
    column_files_count = column_files.size();
    rows = new_rows;
    bytes = new_bytes;
    deletes = new_deletes;
}


} // namespace DB::DM
