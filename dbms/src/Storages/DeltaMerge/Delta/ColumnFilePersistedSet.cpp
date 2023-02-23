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
#include <Storages/Page/universal/Readers.h>
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

    RUNTIME_CHECK_MSG(new_rows == rows && new_deletes == deletes,
                      "Rows and deletes check failed. Actual: rows[{}], deletes[{}]. Expected: rows[{}], deletes[{}]. Current column files: {}, new column files: {}.", //
                      new_rows,
                      new_deletes,
                      rows.load(),
                      deletes.load(),
                      columnFilesToString(persisted_files),
                      columnFilesToString(new_column_files));
}

ColumnFilePersistedSet::ColumnFilePersistedSet( //
    PageId metadata_id_,
    const ColumnFilePersisteds & persisted_column_files)
    : metadata_id(metadata_id_)
    , persisted_files(persisted_column_files)
    , log(Logger::get())
{
    updateColumnFileStats();
}

ColumnFilePersistedSetPtr ColumnFilePersistedSet::restore( //
    DMContext & context,
    const RowKeyRange & segment_range,
    PageId id)
{
    Page page = context.storage_pool->metaReader()->read(id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    auto column_files = deserializeSavedColumnFiles(context, segment_range, buf);
    return std::make_shared<ColumnFilePersistedSet>(id, column_files);
}

ColumnFilePersistedSetPtr ColumnFilePersistedSet::restoreFromCheckpoint( //
    DMContext & context,
    UniversalPageStoragePtr temp_ps,
    const PS::V3::CheckpointInfo & checkpoint_info,
    const RowKeyRange & segment_range,
    NamespaceId ns_id,
    PageId id,
    WriteBatches & wbs)
{
    auto & storage_pool = context.storage_pool;
    auto target_id = StorageReader::toFullUniversalPageId(getStoragePrefix(TableStorageTag::Meta), ns_id, id);
    auto meta_page = temp_ps->read(target_id);
    ReadBufferFromMemory meta_buf(meta_page.data.begin(), meta_page.data.size());
    auto column_files = deserializeSavedRemoteColumnFiles(
        context,
        segment_range,
        meta_buf,
        temp_ps,
        checkpoint_info.checkpoint_store_id,
        ns_id,
        wbs);
    ColumnFilePersisteds new_column_files;
    for (auto & column_file : column_files)
    {
        if (auto * t = column_file->tryToTinyFile(); t)
        {
            auto target_cf_id = StorageReader::toFullUniversalPageId(getStoragePrefix(TableStorageTag::Log), ns_id, t->getDataPageId());
            auto entry = temp_ps->getEntryV3(target_cf_id, nullptr);
            auto new_cf_id = storage_pool->newLogPageId();
            wbs.log.putRemotePage(new_cf_id, 0, entry.remote_info->data_location, std::move(entry.field_offsets));
            new_column_files.push_back(t->cloneWith(new_cf_id));
        }
        else if (auto * d = column_file->tryToDeleteRange(); d)
        {
            new_column_files.push_back(column_file);
        }
        else if (auto * b = column_file->tryToBigFile(); b)
        {
            auto old_page_id = b->getDataPageId();
            auto old_file_id = b->getFile()->fileId();
            auto delegator = context.path_pool->getStableDiskDelegator();
            auto parent_path = delegator.getDTFilePath(old_file_id);
            auto new_file_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
            auto new_dmfile = DMFile::restore(context.db_context.getFileProvider(), old_file_id, new_file_id, parent_path, DMFile::ReadMetaMode::all());
            wbs.data.putRefPage(new_file_id, old_page_id);
            auto new_column_file = b->cloneWith(context, new_dmfile, segment_range);
            new_column_files.push_back(new_column_file);
        }
        else
        {
            RUNTIME_CHECK_MSG(false, "shouldn't reach here");
        }
    }
    auto new_delta_id = storage_pool->newMetaPageId();
    auto new_persisted_set = std::make_shared<ColumnFilePersistedSet>(new_delta_id, new_column_files);
    new_persisted_set->saveMeta(wbs);
    return new_persisted_set;
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


ColumnFilePersisteds ColumnFilePersistedSet::diffColumnFiles(const ColumnFiles & previous_column_files) const
{
    // It should not be not possible that files in the snapshots are removed when calling this
    // function. So we simply expect there are more column files now.
    // Major compaction and minor compaction are segment updates, which should be blocked by
    // the for_update snapshot.
    // TODO: We'd better enforce user to specify a for_update snapshot in the args, to ensure
    //       that this function is called under a for_update snapshot context.
    RUNTIME_CHECK(previous_column_files.size() <= getColumnFileCount());

    auto it_1 = previous_column_files.begin();
    auto it_2 = persisted_files.begin();
    bool check_success = true;
    if (likely(previous_column_files.size() <= persisted_files_count.load()))
    {
        while (it_1 != previous_column_files.end() && it_2 != persisted_files.end())
        {
            // We allow passing unflushed memtable files to `previous_column_files`, these heads will be skipped anyway.
            if (!(*it_2)->mayBeFlushedFrom(&(**it_1)) && !(*it_2)->isSame(&(**it_1)))
            {
                check_success = false;
                break;
            }
            if ((*it_1)->getRows() != (*it_2)->getRows() || (*it_1)->getBytes() != (*it_2)->getBytes())
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
        LOG_ERROR(log, "{}, Delta Check head failed, unexpected size. head column files: {}, persisted column files: {}", info(), columnFilesToString(previous_column_files), detailInfo());
        throw Exception("Check head failed, unexpected size", ErrorCodes::LOGICAL_ERROR);
    }

    ColumnFilePersisteds tail;
    while (it_2 != persisted_files.end())
    {
        const auto & column_file = *it_2;
        tail.push_back(column_file);
        it_2++;
    }

    return tail;
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
        LOG_DEBUG(log, "{} Stop flush because structure got updated", simpleInfo());
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
    LOG_DEBUG(log, "{}, after append {} column files, persisted column files: {}", info(), column_files.size(), detailInfo());

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
        LOG_WARNING(log, "Structure has been updated during compact");
        return false;
    }
    minor_compaction_version += 1;
    LOG_DEBUG(log, "{}, before commit compaction, persisted column files: {}", info(), detailInfo());
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
    LOG_DEBUG(log, "{}, after commit compaction, persisted column files: {}", info(), detailInfo());

    return true;
}

ColumnFileSetSnapshotPtr ColumnFilePersistedSet::createSnapshot(const IColumnFileSetStorageReaderPtr & storage_reader)
{
    auto snap = std::make_shared<ColumnFileSetSnapshot>(storage_reader);
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
        LOG_ERROR(log, "Rows and deletes check failed. Actual: rows[{}], deletes[{}]. Expected: rows[{}], deletes[{}].", total_rows, total_deletes, rows.load(), deletes.load());
        throw Exception("Rows and deletes check failed.", ErrorCodes::LOGICAL_ERROR);
    }

    return snap;
}

} // namespace DM
} // namespace DB
