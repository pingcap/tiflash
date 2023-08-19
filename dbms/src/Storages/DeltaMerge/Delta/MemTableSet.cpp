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
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>

namespace ProfileEvents
{
extern const Event DMWriteBytes;
}

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

ColumnFiles MemTableSet::cloneColumnFiles(DMContext & context, const RowKeyRange & target_range, WriteBatches & wbs)
{
    ColumnFiles cloned_column_files;
    for (const auto & column_file : column_files)
    {
        if (auto * dr = column_file->tryToDeleteRange(); dr)
        {
            auto new_dr = dr->getDeleteRange().shrink(target_range);
            if (!new_dr.none())
            {
                // Only use the available delete_range column file.
                cloned_column_files.push_back(dr->cloneWith(new_dr));
            }
        }
        else if (auto * b = column_file->tryToInMemoryFile(); b)
        {
            auto new_column_file = b->clone();

            // No matter or what, don't append to column files which cloned from old column file again.
            // Because they could shared the same cache. And the cache can NOT be inserted from different column files in different delta.
            new_column_file->disableAppend();
            cloned_column_files.push_back(new_column_file);
        }
        else if (auto * t = column_file->tryToTinyFile(); t)
        {
            // Use a newly created page_id to reference the data page_id of current column file.
            PageId new_data_page_id = context.storage_pool.newLogPageId();
            wbs.log.putRefPage(new_data_page_id, t->getDataPageId());
            auto new_column_file = t->cloneWith(new_data_page_id);

            cloned_column_files.push_back(new_column_file);
        }
        else if (auto * f = column_file->tryToBigFile(); f)
        {
            auto delegator = context.path_pool.getStableDiskDelegator();
            auto new_page_id = context.storage_pool.newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
            // Note that the file id may has already been mark as deleted. We must
            // create a reference to the page id itself instead of create a reference
            // to the file id.
            wbs.data.putRefPage(new_page_id, f->getDataPageId());
            auto file_id = f->getFile()->fileId();
            auto file_parent_path = delegator.getDTFilePath(file_id);
            auto new_file = DMFile::restore(context.db_context.getFileProvider(), file_id, /* page_id= */ new_page_id, file_parent_path, DMFile::ReadMetaMode::all());

            auto new_column_file = f->cloneWith(context, new_file, target_range);
            cloned_column_files.push_back(new_column_file);
        }
    }
    return cloned_column_files;
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

ColumnFileSetSnapshotPtr MemTableSet::createSnapshot(const StorageSnapshotPtr & storage_snap)
{
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
            // Compact threads could update the value of ColumnTinyFile::cache,
            // and since ColumnFile is not multi-threads safe, we should create a new column file object.
            snap->column_files.push_back(std::make_shared<ColumnFileInMemory>(*m));
        }
        else
        {
            snap->column_files.push_back(file);
        }
        total_rows += file->getRows();
        total_deletes += file->getDeletes();
    }

    if (unlikely(total_rows != rows || total_deletes != deletes))
    {
        LOG_FMT_ERROR(log, "Rows and deletes check failed. Actual: rows[{}], deletes[{}]. Expected: rows[{}], deletes[{}].", total_rows, total_deletes, rows.load(), deletes.load());
        throw Exception("Rows and deletes check failed.", ErrorCodes::LOGICAL_ERROR);
    }

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
        LOG_FMT_ERROR(log, "Rows and deletes check failed. Actual: rows[{}], deletes[{}]. Expected: rows[{}], deletes[{}]. Column Files: {}", flush_task->getFlushRows(), flush_task->getFlushDeletes(), rows.load(), deletes.load(), columnFilesToString(column_files));
        throw Exception("Rows and deletes check failed.", ErrorCodes::LOGICAL_ERROR);
    }

    return flush_task;
}

void MemTableSet::removeColumnFilesInFlushTask(const ColumnFileFlushTask & flush_task)
{
    const auto & tasks = flush_task.getAllTasks();
    if (unlikely(tasks.size() > column_files.size()))
        throw Exception("column_files num check failed", ErrorCodes::LOGICAL_ERROR);

    size_t flush_bytes = 0;
    auto column_file_iter = column_files.begin();
    for (const auto & task : tasks)
    {
        if (unlikely(column_file_iter == column_files.end() || *column_file_iter != task.column_file))
        {
            throw Exception("column_files check failed", ErrorCodes::LOGICAL_ERROR);
        }
        flush_bytes += task.column_file->getBytes();
        column_file_iter++;
    }
    ColumnFiles new_column_files;
    size_t new_rows = 0;
    size_t new_bytes = 0;
    size_t new_deletes = 0;
    while (column_file_iter != column_files.end())
    {
        new_column_files.emplace_back(*column_file_iter);
        new_rows += (*column_file_iter)->getRows();
        new_bytes += (*column_file_iter)->getBytes();
        new_deletes += (*column_file_iter)->getDeletes();
        column_file_iter++;
    }
    column_files.swap(new_column_files);
    column_files_count = column_files.size();
    rows = new_rows;
    bytes = new_bytes;
    deletes = new_deletes;

    ProfileEvents::increment(ProfileEvents::DMWriteBytes, flush_bytes);
}


} // namespace DM
} // namespace DB
