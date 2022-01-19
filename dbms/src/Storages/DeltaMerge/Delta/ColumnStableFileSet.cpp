#include <Functions/FunctionHelpers.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/ColumnStableFileSet.h>
#include <Storages/DeltaMerge/DeltaIndexManager.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace DM
{

inline void serializeColumnStableFileLevels(WriteBatches & wbs, PageId id, const ColumnStableFileSet::ColumnStableFileLevels & file_levels)
{
    MemoryWriteBuffer buf(0, COLUMN_FILE_SERIALIZE_BUFFER_SIZE);
    ColumnStableFiles column_files;
    for (const auto & level : file_levels)
    {
        for (const auto & file : level)
        {
            column_files.emplace_back(file);
        }
    }
    serializeColumnStableFiles(buf, column_files);
    auto data_size = buf.count();
    wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

void ColumnStableFileSet::updateStats()
{
    for (auto & file_level : stable_files_levels)
    {
        stable_files_count += file_level.size();
        for (auto & file : file_level)
        {
            rows += file->getRows();
            bytes += file->getBytes();
            deletes += file->getDeletes();
        }
    }
}

ColumnStableFileSet::ColumnStableFileSet(PageId metadata_id_, const ColumnStableFiles & column_stable_files)
    : metadata_id(metadata_id_)
    , log(&Poco::Logger::get("ColumnStableFileSet"))
{
    // TODO: place column file to different levels
    stable_files_levels.push_back(column_stable_files);

    updateStats();
}

ColumnStableFileSetPtr ColumnStableFileSet::restore(DMContext & context, const RowKeyRange & segment_range, PageId id)
{
    Page page = context.storage_pool.meta()->read(id, nullptr);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    auto column_files = deserializeColumnStableFiles(context, segment_range, buf);
    return std::make_shared<ColumnStableFileSet>(id, column_files);
}

void ColumnStableFileSet::saveMeta(WriteBatches & wbs) const
{
    serializeColumnStableFileLevels(wbs, metadata_id, stable_files_levels);
}

void ColumnStableFileSet::recordRemoveColumnFilesPages(WriteBatches & wbs) const
{
    for (const auto & level : stable_files_levels)
    {
        for (const auto & file : level)
            file->removeData(wbs);
    }
}


ColumnStableFiles ColumnStableFileSet::checkHeadAndCloneTail(DMContext & context,
                                                             const RowKeyRange & target_range,
                                                             const ColumnFiles & head_column_files,
                                                             WriteBatches & wbs) const
{
    // We check in the direction from the last level to the first level.
    // In every level, we check from the begin to the last.
    auto it_1 = head_column_files.begin();
    auto level_it = stable_files_levels.rbegin();
    auto it_2 = level_it->begin();
    bool check_success = true;
    if (likely(head_column_files.size() <= stable_files_count.load()))
    {
        while (it_1 != head_column_files.end() && level_it != stable_files_levels.rend())
        {
            if (it_2 == level_it->end())
            {
                level_it++;
                if (unlikely(level_it == stable_files_levels.rend()))
                    throw Exception("Delta Check head algorithm broken", ErrorCodes::LOGICAL_ERROR);
                it_2 = level_it->begin();
                continue;
            }
            if ((*it_1)->getId() != (*it_2)->getId() || (*it_1)->getRows() != (*it_2)->getRows())
            {
                check_success = false;
                break;
            }
            it_1++;
            it_2++;
        }
    }

    if (unlikely(!check_success))
    {
        LOG_ERROR(log,
                  info() << ", Delta Check head failed, unexpected size. head column files: " << columnFilesToString(head_column_files)
                         << ", level details: " << levelsInfo());
        throw Exception("Check head failed, unexpected size", ErrorCodes::LOGICAL_ERROR);
    }

    ColumnStableFiles cloned_tail;
    while (level_it != stable_files_levels.rend())
    {
        if (it_2 == level_it->end())
        {
            level_it++;
            if (level_it == stable_files_levels.rend())
                break;
            it_2 = level_it->begin();
        }
        const auto & column_file = *it_2;
        if (auto * cr = column_file->tryToDeleteRange(); cr)
        {
            auto new_dr = cr->getDeleteRange().shrink(target_range);
            if (!new_dr.none())
            {
                // Only use the available delete_range pack.
                cloned_tail.push_back(cr->cloneWith(new_dr));
            }
        }
        else if (auto * tf = column_file->tryToTinyFile(); tf)
        {
            // Use a newly created page_id to reference the data page_id of current column file.
            PageId new_data_page_id = context.storage_pool.newLogPageId();
            wbs.log.putRefPage(new_data_page_id, tf->getDataPageId());
            auto new_column_file = tf->cloneWith(new_data_page_id);
            cloned_tail.push_back(new_column_file);
        }
        else if (auto * f = column_file->tryToBigFile(); f)
        {
            auto delegator = context.path_pool.getStableDiskDelegator();
            auto new_ref_id = context.storage_pool.newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
            auto file_id = f->getFile()->fileId();
            wbs.data.putRefPage(new_ref_id, file_id);
            auto file_parent_path = delegator.getDTFilePath(file_id);
            auto new_file = DMFile::restore(context.db_context.getFileProvider(), file_id, /* ref_id= */ new_ref_id, file_parent_path, DMFile::ReadMetaMode::all());

            auto new_big_file = f->cloneWith(context, new_file, target_range);
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

size_t ColumnStableFileSet::getTotalCacheRows() const
{
    size_t cache_rows = 0;
    for (const auto & level : stable_files_levels)
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

size_t ColumnStableFileSet::getTotalCacheBytes() const
{
    size_t cache_bytes = 0;
    for (const auto & level : stable_files_levels)
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

size_t ColumnStableFileSet::getValidCacheRows() const
{
    size_t cache_rows = 0;
    for (const auto & level : stable_files_levels)
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

bool ColumnStableFileSet::appendColumnStableFilesToLevel0(size_t prev_flush_version, const ColumnStableFiles & column_files, WriteBatches & wbs)
{
    if (prev_flush_version != flush_version)
    {
        LOG_DEBUG(log, simpleInfo() << " Stop flush because structure got updated");
        return false;
    }
    flush_version += 1;
    ColumnStableFileLevels new_stable_files_levels;
    for (auto & level : stable_files_levels)
    {
        auto & new_level = new_stable_files_levels.emplace_back();
        for (auto & file : level)
            new_level.push_back(file);
    }
    if (new_stable_files_levels.empty())
        new_stable_files_levels.emplace_back();
    auto & new_level_0 = new_stable_files_levels[0];
    for (const auto & f : column_files)
        new_level_0.push_back(f);

    /// Save the new metadata of column files to disk.
    serializeColumnStableFileLevels(wbs, metadata_id, new_stable_files_levels);
    wbs.writeMeta();

    /// Commit updates in memory.
    stable_files_levels.swap(new_stable_files_levels);
    updateStats();

    return true;
}

MinorCompactionPtr ColumnStableFileSet::pickUpMinorCompaction(DMContext & context)
{
    // Every time we try to compact all column files in a specific level.
    // For ColumnTinyFile, we will try to combine small `ColumnTinyFile`s to a bigger one.
    // For ColumnDeleteRangeFile and ColumnBigFile, we will simply move them to the next level.
    // And only if there some small `ColumnTinyFile`s which can be combined together we will actually do the compaction.
    size_t check_level_num = 0;
    while (check_level_num < stable_files_levels.size())
    {
        if (next_compaction_level >= stable_files_levels.size())
            next_compaction_level = 0;

        auto compaction = std::make_shared<MinorCompaction>(next_compaction_level);
        auto & level = stable_files_levels[next_compaction_level];
        if (!level.empty())
        {
            bool is_all_trivial_move = true;
            MinorCompaction::Task cur_task;
            for (auto & file : level)
            {
                auto packup_cur_task = [&]() {
                    bool is_trivial_move = compaction->packUpTask(std::move(cur_task));
                    is_all_trivial_move = is_all_trivial_move && is_trivial_move;
                    cur_task = {};
                };

                if (auto * t_file = file->tryToTinyFile(); t_file)
                {
                    bool cur_task_full = cur_task.total_rows >= context.delta_small_pack_rows;
                    bool small_column_file = t_file->getRows() < context.delta_small_pack_rows;
                    bool schema_ok
                        = cur_task.to_compact.empty();
                    if (!schema_ok)
                    {
                        if (auto * last_t_file = cur_task.to_compact.back()->tryToTinyFile(); last_t_file)
                            schema_ok = t_file->getSchema() == last_t_file->getSchema();
                    }

                    if (cur_task_full || !small_column_file || !schema_ok)
                        packup_cur_task();

                    cur_task.addColumnFile(file);
                }
                else
                {
                    packup_cur_task();
                    cur_task.addColumnFile(file);
                }
            }
            bool is_trivial_move = compaction->packUpTask(std::move(cur_task));
            is_all_trivial_move = is_all_trivial_move && is_trivial_move;

            if (!is_all_trivial_move)
                return compaction;
        }
        next_compaction_level++;
    }
    return nullptr;
}

bool ColumnStableFileSet::installCompactionResults(const MinorCompactionPtr & compaction, WriteBatches & wbs)
{
    if (compaction->current_compaction_version != minor_compaction_version)
    {
        LOG_WARNING(log, "Structure has been updated during compact");
        return false;
    }
    ColumnStableFileLevels new_stable_files_levels;
    for (size_t i = 0; i < compaction->compaction_src_level; i++)
    {
        auto & new_level = new_stable_files_levels.emplace_back();
        for (const auto & f : stable_files_levels[i])
            new_level.push_back(f);
    }
    // Create a new empty level for `compaction_src_level` because all the column files is compacted to next level
    new_stable_files_levels.emplace_back();

    // Add new file to the target level
    auto target_level = compaction->compaction_src_level + 1;
    auto & target_level_files = new_stable_files_levels.emplace_back();
    if (stable_files_levels.size() > target_level)
    {
        for (auto & column_file : stable_files_levels[target_level])
            target_level_files.emplace_back(column_file);
    }
    for (auto & task : compaction->tasks)
    {
        if (task.is_trivial_move)
            target_level_files.push_back(task.to_compact[0]);
        else
            target_level_files.push_back(task.result);
    }

    // Append remaining levels
    for (size_t i = target_level + 1; i < stable_files_levels.size(); i++)
    {
        auto & new_level = new_stable_files_levels.emplace_back();
        for (const auto & f : stable_files_levels[i])
            new_level.push_back(f);
    }

    /// Save the new metadata of column files to disk.
    serializeColumnStableFileLevels(wbs, metadata_id, new_stable_files_levels);
    wbs.writeMeta();

    /// Commit updates in memory.
    stable_files_levels.swap(new_stable_files_levels);
    updateStats();

    return true;
}

ColumnFileSetSnapshotPtr ColumnStableFileSet::createSnapshot(const DMContext & context)
{
    auto storage_snap = std::make_shared<StorageSnapshot>(context.storage_pool, context.getReadLimiter(), true);
    auto snap = std::make_shared<ColumnFileSetSnapshot>(std::move(storage_snap));
    snap->rows = rows;
    snap->bytes = bytes;
    snap->deletes = deletes;

    size_t total_rows = 0;
    size_t total_deletes = 0;
    for (const auto & level : stable_files_levels)
    {
        for (const auto & file : level)
        {
            snap->column_files.push_back(file);
            total_rows += file->getRows();
            total_deletes += file->getDeletes();
        }
    }

    if (unlikely(total_rows != rows || total_deletes != deletes))
        throw Exception("Rows and deletes check failed!", ErrorCodes::LOGICAL_ERROR);

    return snap;
}
}
}
