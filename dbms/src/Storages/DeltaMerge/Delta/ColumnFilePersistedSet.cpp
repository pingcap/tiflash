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
inline ColumnFilePersisteds flattenColumnFileLevels(const ColumnFilePersistedSet::ColumnFilePersistedLevels & file_levels)
{
    ColumnFilePersisteds column_files;
    // Last level first
    for (auto level_it = file_levels.rbegin(); level_it != file_levels.rend(); ++level_it)
    {
        for (const auto & file : *level_it)
        {
            column_files.emplace_back(file);
        }
    }
    return column_files;
}

inline void serializeColumnFilePersistedLevels(WriteBatches & wbs, PageId id, const ColumnFilePersistedSet::ColumnFilePersistedLevels & file_levels)
{
    MemoryWriteBuffer buf(0, COLUMN_FILE_SERIALIZE_BUFFER_SIZE);
    auto column_files = flattenColumnFileLevels(file_levels);
    serializeSavedColumnFiles(buf, column_files);
    auto data_size = buf.count();
    wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

void ColumnFilePersistedSet::updateColumnFileStats()
{
    size_t new_persisted_files_count = 0;
    size_t new_rows = 0;
    size_t new_bytes = 0;
    size_t new_deletes = 0;
    for (auto & file_level : persisted_files_levels)
    {
        new_persisted_files_count += file_level.size();
        for (auto & file : file_level)
        {
            new_rows += file->getRows();
            new_bytes += file->getBytes();
            new_deletes += file->getDeletes();
        }
    }
    persisted_files_count = new_persisted_files_count;
    persisted_files_level_count = persisted_files_levels.size();
    rows = new_rows;
    bytes = new_bytes;
    deletes = new_deletes;
}

void ColumnFilePersistedSet::checkColumnFiles(const ColumnFilePersistedLevels & new_column_file_levels)
{
    if constexpr (!DM_RUN_CHECK)
        return;
    size_t new_rows = 0;
    size_t new_deletes = 0;
    for (const auto & level : new_column_file_levels)
    {
        for (const auto & file : level)
        {
            new_rows += file->getRows();
            new_deletes += file->isDeleteRange();
        }
    }

    if (unlikely(new_rows != rows || new_deletes != deletes))
    {
        LOG_ERROR(log, "Rows and deletes check failed. Actual: rows[{}], deletes[{}]. Expected: rows[{}], deletes[{}]. Current column files: {}, new column files: {}.", new_rows, new_deletes, rows.load(), deletes.load(), columnFilesToString(flattenColumnFileLevels(persisted_files_levels)), columnFilesToString(flattenColumnFileLevels(new_column_file_levels)));
        throw Exception("Rows and deletes check failed.", ErrorCodes::LOGICAL_ERROR);
    }
}

ColumnFilePersistedSet::ColumnFilePersistedSet( //
    PageId metadata_id_,
    const ColumnFilePersisteds & persisted_column_files)
    : metadata_id(metadata_id_)
    , log(Logger::get())
{
    // TODO: place column file to different levels, but it seems no need to do it currently because we only do minor compaction on really small files?
    persisted_files_levels.push_back(persisted_column_files);

    updateColumnFileStats();
}

ColumnFilePersistedSetPtr ColumnFilePersistedSet::restore( //
    DMContext & context,
    const RowKeyRange & segment_range,
    PageId id)
{
    Page page = context.storage_pool.metaReader()->read(id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    auto column_files = deserializeSavedColumnFiles(context, segment_range, buf);
    return std::make_shared<ColumnFilePersistedSet>(id, column_files);
}

void ColumnFilePersistedSet::saveMeta(WriteBatches & wbs) const
{
    serializeColumnFilePersistedLevels(wbs, metadata_id, persisted_files_levels);
}

void ColumnFilePersistedSet::recordRemoveColumnFilesPages(WriteBatches & wbs) const
{
    for (const auto & level : persisted_files_levels)
    {
        for (const auto & file : level)
            file->removeData(wbs);
    }
}

BlockPtr ColumnFilePersistedSet::getLastSchema()
{
    for (const auto & level : persisted_files_levels)
    {
        for (auto it = level.rbegin(); it != level.rend(); ++it)
        {
            if (auto * t_file = (*it)->tryToTinyFile(); t_file)
                return t_file->getSchema();
        }
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

    // We check in the direction from the last level to the first level.
    // In every level, we check from the begin to the last.
    auto it_1 = previous_column_files.begin();
    auto level_it = persisted_files_levels.rbegin();
    auto it_2 = level_it->begin();
    bool check_success = true;
    if (likely(previous_column_files.size() <= persisted_files_count.load()))
    {
        while (it_1 != previous_column_files.end() && level_it != persisted_files_levels.rend())
        {
            if (it_2 == level_it->end())
            {
                level_it++;
                if (unlikely(level_it == persisted_files_levels.rend()))
                    throw Exception("Delta Check head algorithm broken", ErrorCodes::LOGICAL_ERROR);
                it_2 = level_it->begin();
                continue;
            }
            // We allow passing unflushed memtable files to `previous_column_files`, these heads will be skipped anyway.
            if (!(*it_2)->mayBeFlushedFrom(&**it_1) && !(*it_1)->isSame(&**it_1))
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
        LOG_ERROR(log, "{}, Delta Check head failed, unexpected size. head column files: {}, level details: {}", info(), columnFilesToString(previous_column_files), levelsInfo());
        throw Exception("Check head failed, unexpected size", ErrorCodes::LOGICAL_ERROR);
    }

    ColumnFilePersisteds tail;
    while (level_it != persisted_files_levels.rend())
    {
        if (it_2 == level_it->end())
        {
            level_it++;
            if (level_it == persisted_files_levels.rend())
                break;
            it_2 = level_it->begin();
            continue;
        }
        const auto & column_file = *it_2;
        tail.push_back(column_file);
        it_2++;
    }

    return tail;
}

size_t ColumnFilePersistedSet::getTotalCacheRows() const
{
    size_t cache_rows = 0;
    for (const auto & level : persisted_files_levels)
    {
        for (const auto & file : level)
        {
            if (auto * tf = file->tryToTinyFile(); tf)
            {
                if (auto && c = tf->getCache(); c)
                    cache_rows += c->block.rows();
            }
        }
    }
    return cache_rows;
}

size_t ColumnFilePersistedSet::getTotalCacheBytes() const
{
    size_t cache_bytes = 0;
    for (const auto & level : persisted_files_levels)
    {
        for (const auto & file : level)
        {
            if (auto * tf = file->tryToTinyFile(); tf)
            {
                if (auto && c = tf->getCache(); c)
                    cache_bytes += c->block.allocatedBytes();
            }
        }
    }
    return cache_bytes;
}

size_t ColumnFilePersistedSet::getValidCacheRows() const
{
    size_t cache_rows = 0;
    for (const auto & level : persisted_files_levels)
    {
        for (const auto & file : level)
        {
            if (auto * tf = file->tryToTinyFile(); tf)
            {
                if (auto && c = tf->getCache(); c)
                    cache_rows += tf->getRows();
            }
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

bool ColumnFilePersistedSet::appendPersistedColumnFilesToLevel0(const ColumnFilePersisteds & column_files, WriteBatches & wbs)
{
    ColumnFilePersistedLevels new_persisted_files_levels;
    for (auto & level : persisted_files_levels)
    {
        auto & new_level = new_persisted_files_levels.emplace_back();
        for (auto & file : level)
            new_level.push_back(file);
    }
    if (new_persisted_files_levels.empty())
        new_persisted_files_levels.emplace_back();
    auto & new_level_0 = new_persisted_files_levels[0];

    for (const auto & f : column_files)
        new_level_0.push_back(f);

    /// Save the new metadata of column files to disk.
    serializeColumnFilePersistedLevels(wbs, metadata_id, new_persisted_files_levels);
    wbs.writeMeta();

    /// Commit updates in memory.
    persisted_files_levels.swap(new_persisted_files_levels);
    updateColumnFileStats();
    LOG_DEBUG(log, "{}, after append {} column files, level info: {}", info(), column_files.size(), levelsInfo());

    return true;
}

MinorCompactionPtr ColumnFilePersistedSet::pickUpMinorCompaction(DMContext & context)
{
    // Every time we try to compact all column files in a specific level.
    // For ColumnFileTiny, we will try to combine small `ColumnFileTiny`s to a bigger one.
    // For ColumnFileDeleteRange and ColumnFileBig, we will simply move them to the next level.
    // And only if there exists some small `ColumnFileTiny`s which can be combined together, we will actually do the compaction.
    size_t check_level_num = 0;
    while (check_level_num < persisted_files_levels.size())
    {
        check_level_num += 1;
        if (next_compaction_level >= persisted_files_levels.size())
            next_compaction_level = 0;

        auto compaction = std::make_shared<MinorCompaction>(next_compaction_level, minor_compaction_version);
        auto & level = persisted_files_levels[next_compaction_level];
        next_compaction_level++;
        if (!level.empty())
        {
            bool is_all_trivial_move = true;
            MinorCompaction::Task cur_task;
            for (auto & file : level)
            {
                auto pack_up_cur_task = [&]() {
                    bool is_trivial_move = compaction->packUpTask(std::move(cur_task));
                    is_all_trivial_move = is_all_trivial_move && is_trivial_move;
                    cur_task = {};
                };

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

                    cur_task.addColumnFile(file);
                }
                else
                {
                    pack_up_cur_task();
                    cur_task.addColumnFile(file);
                }
            }
            bool is_trivial_move = compaction->packUpTask(std::move(cur_task));
            is_all_trivial_move = is_all_trivial_move && is_trivial_move;

            if (!is_all_trivial_move)
                return compaction;
        }
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
    LOG_DEBUG(log, "{}, before commit compaction, level info: {}", info(), levelsInfo());
    ColumnFilePersistedLevels new_persisted_files_levels;
    auto compaction_src_level = compaction->getCompactionSourceLevel();
    // Copy column files in level range [0, compaction_src_level)
    for (size_t i = 0; i < compaction_src_level; i++)
    {
        auto & new_level = new_persisted_files_levels.emplace_back();
        for (const auto & f : persisted_files_levels[i])
            new_level.push_back(f);
    }
    // Copy the files in source level that is not in the compaction task.
    // Actually, just level 0 may contain file that is not in the compaction task, because flush and compaction can happen concurrently.
    // For other levels, we always compact all the files in the level.
    // And because compaction is a single threaded process, so there will be no new files compacted to the source level at the same time.
    const auto & old_src_level_files = persisted_files_levels[compaction_src_level];
    auto old_src_level_files_iter = old_src_level_files.begin();
    for (const auto & task : compaction->getTasks())
    {
        for (const auto & file : task.to_compact)
        {
            if (unlikely(old_src_level_files_iter == old_src_level_files.end()
                         || (file->getId() != (*old_src_level_files_iter)->getId())
                         || (file->getRows() != (*old_src_level_files_iter)->getRows())))
            {
                throw Exception("Compaction algorithm broken", ErrorCodes::LOGICAL_ERROR);
            }
            old_src_level_files_iter++;
        }
    }
    auto & src_level_files = new_persisted_files_levels.emplace_back();
    while (old_src_level_files_iter != old_src_level_files.end())
    {
        src_level_files.emplace_back(*old_src_level_files_iter);
        old_src_level_files_iter++;
    }
    // Add new file to the target level
    auto target_level = compaction_src_level + 1;
    auto & target_level_files = new_persisted_files_levels.emplace_back();
    // Copy the old column files in the target level first if exists
    if (persisted_files_levels.size() > target_level)
    {
        for (auto & column_file : persisted_files_levels[target_level])
            target_level_files.emplace_back(column_file);
    }
    // Add the compaction result to new target level
    for (const auto & task : compaction->getTasks())
    {
        if (task.is_trivial_move)
            target_level_files.push_back(task.to_compact[0]);
        else
            target_level_files.push_back(task.result);
    }
    // Copy column files in level range [target_level + 1, +inf) if exists
    for (size_t i = target_level + 1; i < persisted_files_levels.size(); i++)
    {
        auto & new_level = new_persisted_files_levels.emplace_back();
        for (const auto & f : persisted_files_levels[i])
            new_level.push_back(f);
    }

    checkColumnFiles(new_persisted_files_levels);

    /// Save the new metadata of column files to disk.
    serializeColumnFilePersistedLevels(wbs, metadata_id, new_persisted_files_levels);
    wbs.writeMeta();

    /// Commit updates in memory.
    persisted_files_levels.swap(new_persisted_files_levels);
    updateColumnFileStats();
    LOG_DEBUG(log, "{}, after commit compaction, level info: {}", info(), levelsInfo());

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
    // The read direction is from the last level to the first level,
    // and in each level we read from the begin to the end.
    for (auto level_it = persisted_files_levels.rbegin(); level_it != persisted_files_levels.rend(); level_it++)
    {
        for (const auto & file : *level_it)
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
