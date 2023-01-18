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
#include <Storages/DeltaMerge/Delta/ColumnFilePersistedSet.h>
#include <Storages/DeltaMerge/DeltaIndexManager.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace DM
{
inline void serializeColumnFilePersisteds(WriteBatches & wbs, PageId id, const ColumnFilePersisteds & persisted_files)
{
    MemoryWriteBuffer buf(0, COLUMN_FILE_SERIALIZE_BUFFER_SIZE);
    serializeSavedColumnFiles(buf, persisted_files);
    auto data_size = buf.count();
    wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

void ColumnFilePersistedSet::updateColumnFileStats()
{
    size_t new_rows = 0;
    size_t new_bytes = 0;
    size_t new_deletes = 0;
    for (auto & file : persisted_files)
    {
        new_rows += file->getRows();
        new_bytes += file->getBytes();
        new_deletes += file->getDeletes();
    }
    persisted_files_count = persisted_files.size();
    rows = new_rows;
    bytes = new_bytes;
    deletes = new_deletes;
}

void ColumnFilePersistedSet::checkColumnFiles(const ColumnFilePersisteds & new_column_files)
{
    if constexpr (!DM_RUN_CHECK)
        return;
    size_t new_rows = 0;
    size_t new_deletes = 0;
    for (const auto & file : new_column_files)
    {
        new_rows += file->getRows();
        new_deletes += file->isDeleteRange();
    }

    if (unlikely(new_rows != rows || new_deletes != deletes))
    {
        LOG_FMT_ERROR(log, "Rows and deletes check failed. Actual: rows[{}], deletes[{}]. Expected: rows[{}], deletes[{}]. Current column files: {}, new column files: {}.", //
                      new_rows,
                      new_deletes,
                      rows.load(),
                      deletes.load(),
                      columnFilesToString(persisted_files),
                      columnFilesToString(new_column_files));
        throw Exception("Rows and deletes check failed.", ErrorCodes::LOGICAL_ERROR);
    }
}

ColumnFilePersistedSet::ColumnFilePersistedSet(PageId metadata_id_, const ColumnFilePersisteds & persisted_column_files)
    : metadata_id(metadata_id_)
    , persisted_files(persisted_column_files)
    , log(&Poco::Logger::get("ColumnFilePersistedSet"))
{
    updateColumnFileStats();
}

ColumnFilePersistedSetPtr ColumnFilePersistedSet::restore(DMContext & context, const RowKeyRange & segment_range, PageId id)
{
    Page page = context.storage_pool.metaReader()->read(id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    auto column_files = deserializeSavedColumnFiles(context, segment_range, buf);
    return std::make_shared<ColumnFilePersistedSet>(id, column_files);
}

void ColumnFilePersistedSet::saveMeta(WriteBatches & wbs) const
{
    serializeColumnFilePersisteds(wbs, metadata_id, persisted_files);
}

void ColumnFilePersistedSet::recordRemoveColumnFilesPages(WriteBatches & wbs) const
{
    for (const auto & file : persisted_files)
        file->removeData(wbs);
}

BlockPtr ColumnFilePersistedSet::getLastSchema()
{
    for (auto it = persisted_files.rbegin(); it != persisted_files.rend(); ++it)
    {
        if (auto * t_file = (*it)->tryToTinyFile(); t_file)
            return t_file->getSchema();
    }
    return {};
}


ColumnFilePersisteds ColumnFilePersistedSet::checkHeadAndCloneTail(DMContext & context,
                                                                   const RowKeyRange & target_range,
                                                                   const ColumnFiles & head_column_files,
                                                                   WriteBatches & wbs) const
{
    auto it_1 = head_column_files.begin();
    auto it_2 = persisted_files.begin();
    bool check_success = true;
    if (likely(head_column_files.size() <= persisted_files_count.load()))
    {
        while (it_1 != head_column_files.end() && it_2 != persisted_files.end())
        {
            if ((*it_1)->getId() != (*it_2)->getId() || (*it_1)->getRows() != (*it_2)->getRows())
            {
                check_success = false;
                break;
            }
            it_1++;
            it_2++;
        }
    }
    else
    {
        check_success = false;
    }

    if (unlikely(!check_success))
    {
        LOG_FMT_ERROR(log, "{}, Delta Check head failed, unexpected size. head column files: {}, persisted column files: {}", info(), columnFilesToString(head_column_files), detailInfo());
        throw Exception("Check head failed, unexpected size", ErrorCodes::LOGICAL_ERROR);
    }

    ColumnFilePersisteds cloned_tail;
    while (it_2 != persisted_files.end())
    {
        const auto & column_file = *it_2;
        if (auto * d_file = column_file->tryToDeleteRange(); d_file)
        {
            auto new_dr = d_file->getDeleteRange().shrink(target_range);
            if (!new_dr.none())
            {
                // Only use the available delete_range column file.
                cloned_tail.push_back(d_file->cloneWith(new_dr));
            }
        }
        else if (auto * t_file = column_file->tryToTinyFile(); t_file)
        {
            // Use a newly created page_id to reference the data page_id of current column file.
            PageId new_data_page_id = context.storage_pool.newLogPageId();
            wbs.log.putRefPage(new_data_page_id, t_file->getDataPageId());
            auto new_column_file = t_file->cloneWith(new_data_page_id);
            cloned_tail.push_back(new_column_file);
        }
        else if (auto * b_file = column_file->tryToBigFile(); b_file)
        {
            auto delegator = context.path_pool.getStableDiskDelegator();
            auto new_page_id = context.storage_pool.newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
            // Note that the file id may has already been mark as deleted. We must
            // create a reference to the page id itself instead of create a reference
            // to the file id.
            wbs.data.putRefPage(new_page_id, b_file->getDataPageId());
            auto file_id = b_file->getFile()->fileId();
            auto file_parent_path = delegator.getDTFilePath(file_id);
            auto new_file = DMFile::restore(context.db_context.getFileProvider(), file_id, /* page_id= */ new_page_id, file_parent_path, DMFile::ReadMetaMode::all());

            auto new_big_file = b_file->cloneWith(context, new_file, target_range);
            cloned_tail.push_back(new_big_file);
        }
        else
        {
            throw Exception("Meet unknown type of column file", ErrorCodes::LOGICAL_ERROR);
        }
        it_2++;
    }

    return cloned_tail;
}

size_t ColumnFilePersistedSet::getTotalCacheRows() const
{
    size_t cache_rows = 0;
    for (const auto & file : persisted_files)
    {
        if (auto * tf = file->tryToTinyFile(); tf)
        {
            if (auto && c = tf->getCache(); c)
                cache_rows += c->block.rows();
        }
    }
    return cache_rows;
}

size_t ColumnFilePersistedSet::getTotalCacheBytes() const
{
    size_t cache_bytes = 0;
    for (const auto & file : persisted_files)
    {
        if (auto * tf = file->tryToTinyFile(); tf)
        {
            if (auto && c = tf->getCache(); c)
                cache_bytes += c->block.allocatedBytes();
        }
    }
    return cache_bytes;
}

size_t ColumnFilePersistedSet::getValidCacheRows() const
{
    size_t cache_rows = 0;
    for (const auto & file : persisted_files)
    {
        if (auto * tf = file->tryToTinyFile(); tf)
        {
            if (auto && c = tf->getCache(); c)
                cache_rows += tf->getRows();
        }
    }
    return cache_rows;
}

bool ColumnFilePersistedSet::checkAndIncreaseFlushVersion(size_t task_flush_version)
{
    if (task_flush_version != flush_version)
    {
        LOG_FMT_DEBUG(log, "{} Stop flush because structure got updated", simpleInfo());
        return false;
    }
    flush_version += 1;
    return true;
}

bool ColumnFilePersistedSet::appendPersistedColumnFiles(const ColumnFilePersisteds & column_files, WriteBatches & wbs)
{
    ColumnFilePersisteds new_persisted_files;
    for (const auto & file : persisted_files)
    {
        new_persisted_files.push_back(file);
    }
    for (const auto & file : column_files)
    {
        new_persisted_files.push_back(file);
    }
    /// Save the new metadata of column files to disk.
    serializeColumnFilePersisteds(wbs, metadata_id, new_persisted_files);
    wbs.writeMeta();

    /// Commit updates in memory.
    persisted_files.swap(new_persisted_files);
    updateColumnFileStats();
    LOG_FMT_DEBUG(log, "{}, after append {} column files, persisted column files: {}", info(), column_files.size(), detailInfo());

    return true;
}

MinorCompactionPtr ColumnFilePersistedSet::pickUpMinorCompaction(DMContext & context)
{
    // Every time we try to compact all column files.
    // For ColumnFileTiny, we will try to combine small `ColumnFileTiny`s to a bigger one.
    // For ColumnFileDeleteRange and ColumnFileBig, we keep them intact.
    // And only if there exists some small `ColumnFileTiny`s which can be combined, we will actually do the compaction.
    auto compaction = std::make_shared<MinorCompaction>(minor_compaction_version);
    if (!persisted_files.empty())
    {
        bool is_all_trivial_move = true;
        MinorCompaction::Task cur_task;
        auto pack_up_cur_task = [&]() {
            bool is_trivial_move = compaction->packUpTask(std::move(cur_task));
            is_all_trivial_move = is_all_trivial_move && is_trivial_move;
            cur_task = {};
        };
        size_t index = 0;
        for (auto & file : persisted_files)
        {
            if (auto * t_file = file->tryToTinyFile(); t_file)
            {
                bool cur_task_full = cur_task.total_rows >= context.delta_small_column_file_rows;
                bool small_column_file = t_file->getRows() < context.delta_small_column_file_rows;
                bool schema_ok = cur_task.to_compact.empty();

                if (!schema_ok)
                {
                    if (auto * last_t_file = cur_task.to_compact.back()->tryToTinyFile(); last_t_file)
                        schema_ok = t_file->getSchema() == last_t_file->getSchema();
                }

                if (cur_task_full || !small_column_file || !schema_ok)
                    pack_up_cur_task();

                cur_task.addColumnFile(file, index);
            }
            else
            {
                pack_up_cur_task();
                cur_task.addColumnFile(file, index);
            }

            ++index;
        }
        pack_up_cur_task();

        if (!is_all_trivial_move)
            return compaction;
    }
    return nullptr;
}

bool ColumnFilePersistedSet::installCompactionResults(const MinorCompactionPtr & compaction, WriteBatches & wbs)
{
    if (compaction->getCompactionVersion() != minor_compaction_version)
    {
        LOG_FMT_WARNING(log, "Structure has been updated during compact");
        return false;
    }
    minor_compaction_version += 1;
    LOG_FMT_DEBUG(log, "{}, before commit compaction, persisted column files: {}", info(), detailInfo());
    ColumnFilePersisteds new_persisted_files;
    for (const auto & task : compaction->getTasks())
    {
        if (task.is_trivial_move)
            new_persisted_files.push_back(task.to_compact[0]);
        else
            new_persisted_files.push_back(task.result);
    }
    auto old_persisted_files_iter = persisted_files.begin();
    for (const auto & task : compaction->getTasks())
    {
        for (const auto & file : task.to_compact)
        {
            if (unlikely(old_persisted_files_iter == persisted_files.end()
                         || (file->getId() != (*old_persisted_files_iter)->getId())
                         || (file->getRows() != (*old_persisted_files_iter)->getRows())))
            {
                throw Exception("Compaction algorithm broken", ErrorCodes::LOGICAL_ERROR);
            }
            old_persisted_files_iter++;
        }
    }
    while (old_persisted_files_iter != persisted_files.end())
    {
        new_persisted_files.emplace_back(*old_persisted_files_iter);
        old_persisted_files_iter++;
    }

    checkColumnFiles(new_persisted_files);

    /// Save the new metadata of column files to disk.
    serializeColumnFilePersisteds(wbs, metadata_id, new_persisted_files);
    wbs.writeMeta();

    /// Commit updates in memory.
    persisted_files.swap(new_persisted_files);
    updateColumnFileStats();
    LOG_FMT_DEBUG(log, "{}, after commit compaction, persisted column files: {}", info(), detailInfo());

    return true;
}

ColumnFileSetSnapshotPtr ColumnFilePersistedSet::createSnapshot(const StorageSnapshotPtr & storage_snap)
{
    auto snap = std::make_shared<ColumnFileSetSnapshot>(storage_snap);
    snap->rows = rows;
    snap->bytes = bytes;
    snap->deletes = deletes;

    size_t total_rows = 0;
    size_t total_deletes = 0;
    for (const auto & file : persisted_files)
    {
        if (auto * t = file->tryToTinyFile(); (t && t->getCache()))
        {
            // Compact threads could update the value of ColumnTinyFile::cache,
            // and since ColumnFile is not multi-threads safe, we should create a new column file object.
            snap->column_files.push_back(std::make_shared<ColumnFileTiny>(*t));
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
} // namespace DM
} // namespace DB
