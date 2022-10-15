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

#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/MemTableSet.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>

namespace DB
{
namespace DM
{
void MemTableSet::appendColumnFileInner(const ColumnFilePtr & column_file)
{
    // If this column file's schema is identical to last_schema, then use the last_schema instance (instead of the one in `column_file`),
    // so that we don't have to serialize my_schema instance.
    if (auto * m_file = column_file->tryToInMemoryFile(); m_file)
    {
        auto my_schema = m_file->getSchema();
        if (last_schema && my_schema && last_schema != my_schema && isSameSchema(*my_schema, *last_schema))
            m_file->resetIdenticalSchema(last_schema);
        else
            last_schema = my_schema;
    }
    else if (auto * t_file = column_file->tryToTinyFile(); t_file)
    {
        auto my_schema = t_file->getSchema();
        if (last_schema && my_schema && last_schema != my_schema && isSameSchema(*my_schema, *last_schema))
            t_file->resetIdenticalSchema(last_schema);
        else
            last_schema = my_schema;
    }

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

std::pair</* New */ ColumnFiles, /* Removed */ ColumnFiles> MemTableSet::diffColumnFiles(
    const ColumnFiles & column_files_in_snapshot) const
{
    // Use `ColumnFile *` is sufficient, because we never return it to outside and no need to worry
    // about the lifetime.
    std::map<PageId, ColumnFile *> ids_in_snapshot;
    for (const auto & file : column_files_in_snapshot)
    {
        RUNTIME_CHECK(ids_in_snapshot.find(file->getId()) == ids_in_snapshot.end());
        ids_in_snapshot[file->getId()] = file.get();
    }

    std::map<PageId, ColumnFile *> ids_in_memtable;
    for (const auto & file : column_files)
    {
        RUNTIME_CHECK(ids_in_memtable.find(file->getId()) == ids_in_memtable.end());
        ids_in_memtable[file->getId()] = file.get();
    }

    ColumnFiles new_files;
    new_files.reserve(column_files.size()); // At most, everything in memtable is new
    for (const auto & file : column_files)
    {
        auto it = ids_in_snapshot.find(file->getId());
        if (it == ids_in_snapshot.end())
        {
            new_files.push_back(file);
        }
        else
        {
            const auto * file_in_snapshot = it->second;
            RUNTIME_CHECK(
                file->getId() == file_in_snapshot->getId()
                    && file->getType() == file_in_snapshot->getType()
                    && file->getRows() == file_in_snapshot->getRows(),
                columnFilesToString(column_files_in_snapshot),
                columnFilesToString(column_files));
        }
    }

    ColumnFiles removed_files;
    removed_files.reserve(column_files_in_snapshot.size()); // At most, everything in snapshot is removed
    for (const auto & file : column_files_in_snapshot)
    {
        auto it = ids_in_memtable.find(file->getId());
        if (it == ids_in_memtable.end())
        {
            removed_files.push_back(file);
        }
        else
        {
            const auto * file_in_memtable = it->second;
            RUNTIME_CHECK(
                file->getId() == file_in_memtable->getId()
                    && file->getType() == file_in_memtable->getType()
                    && file->getRows() == file_in_memtable->getRows(),
                columnFilesToString(column_files_in_snapshot),
                columnFilesToString(column_files));
        }
    }

    // In addition to the simple diff, we also verify:
    //   Either all CFs in the snapshot are still in memtable (not flushed),
    //   or none CFs in the snapshot is in memtable (flushed).

    if (removed_files.empty())
    {
        // There is no flush happened, we expect all CFs in the snapshot are still in memtable, and is its head.
        RUNTIME_CHECK(column_files.size() >= column_files_in_snapshot.size());
        for (size_t i = 0; i < column_files_in_snapshot.size(); ++i)
        {
            RUNTIME_CHECK(
                column_files_in_snapshot[i]->getId() == column_files[i]->getId(),
                columnFilesToString(column_files_in_snapshot),
                columnFilesToString(column_files));
        }
    }
    else
    {
        // There is flush happened, we expect all CFs in the snapshot are removed,
        // so that `removed_files == column_files_in_snapshot`.
        RUNTIME_CHECK(removed_files.size() == column_files_in_snapshot.size());
        for (size_t i = 0; i < column_files_in_snapshot.size(); ++i)
        {
            RUNTIME_CHECK(
                column_files_in_snapshot[i]->getId() == removed_files[i]->getId(),
                columnFilesToString(column_files_in_snapshot),
                columnFilesToString(column_files));
        }
    }

    return {new_files, removed_files};
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
        // Create a new column file.
        auto my_schema = (last_schema && isSameSchema(block, *last_schema)) ? last_schema : std::make_shared<Block>(block.cloneEmpty());
        auto new_column_file = std::make_shared<ColumnFileInMemory>(my_schema);
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

void MemTableSet::ingestColumnFiles(const RowKeyRange & range, const ColumnFiles & new_column_files, bool clear_data_in_range)
{
    // Prepend a DeleteRange to clean data before applying column files
    if (clear_data_in_range)
    {
        auto f = std::make_shared<ColumnFileDeleteRange>(range);
        appendColumnFileInner(f);
    }

    for (const auto & f : new_column_files)
    {
        appendColumnFileInner(f);
    }
}

ColumnFileSetSnapshotPtr MemTableSet::createSnapshot(const StorageSnapshotPtr & storage_snap, bool disable_sharing)
{
    // Disable append, so that new writes will not touch the content of this snapshot.
    if (disable_sharing && !column_files.empty() && column_files.back()->isAppendable())
        column_files.back()->disableAppend();

    auto snap = std::make_shared<ColumnFileSetSnapshot>(storage_snap);
    snap->rows = rows;
    snap->bytes = bytes;
    snap->deletes = deletes;
    snap->column_files.reserve(column_files.size());

    size_t total_rows = 0;
    size_t total_deletes = 0;
    for (const auto & file : column_files)
    {
        // ColumnFile is not a thread-safe object, but only ColumnFileInMemory may be appendable after its creation.
        // So we only clone the instance of ColumnFileInMemory here.
        if (auto * m = file->tryToInMemoryFile(); m)
        {
            // Compact threads could update the value of ColumnFileInMemory,
            // and since ColumnFile is not multi-threads safe, we should create a new column file object.
            // TODO: When `disable_sharing == true`, may be we can safely use the same ptr without the clone.
            snap->column_files.push_back(m->clone());
        }
        else
        {
            snap->column_files.push_back(file);
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

    return snap;
}

ColumnFileFlushTaskPtr MemTableSet::buildFlushTask(DMContext & context, size_t rows_offset, size_t deletes_offset, size_t flush_version)
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
        LOG_ERROR(log, "Rows and deletes check failed. Actual: rows[{}], deletes[{}]. Expected: rows[{}], deletes[{}]. Column Files: {}", flush_task->getFlushRows(), flush_task->getFlushDeletes(), rows.load(), deletes.load(), columnFilesToString(column_files));
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


} // namespace DM
} // namespace DB
