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

ColumnStableFileSet::ColumnStableFileSet(PageId metadata_id_, const ColumnStableFiles & column_stable_files)
    : metadata_id(metadata_id_)
    , log(&Poco::Logger::get("ColumnStableFileSet"))
{
    // FIXME: place column file to different levels
    column_stable_file_levels.push_back(column_stable_files);
    for (auto & file_level : column_stable_file_levels)
    {
        for (auto & file : file_level)
        {
            rows += file->getRows();
            bytes += file->getBytes();
            deletes += file->getDeletes();
        }
    }
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
    serializeColumnStableFileLevels(wbs, metadata_id, column_stable_file_levels);
}

ColumnStableFiles
ColumnStableFileSet::checkHeadAndCloneTail(DMContext & context, const RowKeyRange & target_range, const ColumnFiles & head_column_files, WriteBatches & wbs) const
{
    if (head_column_files.size() > getColumnFileCount())
    {
//        LOG_ERROR(log,
//                  info() << ", Delta  Check head packs failed, unexpected size. head_packs: " << packsToString(head_packs)
//                         << ", packs: " << packsToString(packs));
        throw Exception("Check head packs failed, unexpected size", ErrorCodes::LOGICAL_ERROR);
    }

    auto it_1 = head_column_files.begin();
    auto level_it = column_stable_file_levels.rbegin();
    auto it_2 = level_it->begin();
    while (it_1 != head_column_files.end() && level_it != column_stable_file_levels.rend())
    {
        if (it_2 == level_it->end())
        {
            level_it++;
            if (level_it == column_stable_file_levels.rend())
                throw Exception("Check head packs failed", ErrorCodes::LOGICAL_ERROR);
            it_2 = level_it->begin();
            continue;
        }
        if ((*it_1)->getId() != (*it_2)->getId() || (*it_1)->getRows() != (*it_2)->getRows())
        {
//            LOG_ERROR(log,
//                      simpleInfo() << ", Delta  Check head packs failed, unexpected size. head_packs: " << packsToString(head_packs)
//                                   << ", packs: " << packsToString(packs));
            throw Exception("Check head packs failed", ErrorCodes::LOGICAL_ERROR);
        }
    }

    ColumnStableFiles cloned_tail;
    while (level_it != column_stable_file_levels.rend() || it_2 != level_it->end())
    {
        if (it_2 == level_it->end())
        {
            level_it++;
            if (level_it == column_stable_file_levels.rend())
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
            PageId new_data_page_id = 0;
            if (tf->getDataPageId())
            {
                // Use a newly created page_id to reference the data page_id of current pack.
                new_data_page_id = context.storage_pool.newLogPageId();
                wbs.log.putRefPage(new_data_page_id, tf->getDataPageId());
            }

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
    }

    return cloned_tail;
}

void ColumnStableFileSet::recordRemoveColumnFilesPages(WriteBatches & wbs) const
{
    for (const auto & level : column_stable_file_levels)
    {
        for (const auto & file : level)
        {
            file->removeData(wbs);
        }
    }
}

size_t ColumnStableFileSet::getTotalCacheRows() const
{
    size_t cache_rows = 0;
    for (auto & pack : packs)
    {
        if (auto p = pack->tryToBlock(); p)
        {
            if (auto && c = p->getCache(); c)
                cache_rows += c->block.rows();
        }
    }
    return cache_rows;
}

size_t ColumnStableFileSet::getTotalCacheBytes() const
{

}

size_t ColumnStableFileSet::getValidCacheRows() const
{

}

bool ColumnStableFileSet::appendColumnStableFilesToLevel0(size_t prev_flush_version, const ColumnStableFiles & column_files, WriteBatches & wbs)
{
    if (prev_flush_version != flush_version)
    {
        LOG_DEBUG(log, simpleInfo() << " Stop flush because structure got updated");
        return false;
    }
    flush_version += 1;
    // FIXME: copy a new column_stable_file_levels
    if (column_stable_file_levels.empty())
        column_stable_file_levels.emplace_back();
    auto & level_0 = column_stable_file_levels[0];
    for (const auto & f : column_files)
    {
        level_0.push_back(f);
    }
    MemoryWriteBuffer buf(0, COLUMN_FILE_SERIALIZE_BUFFER_SIZE);
    serializeColumnStableFileLevels(wbs, metadata_id, column_stable_file_levels);
    return true;
}

MinorCompactionPtr ColumnStableFileSet::pickUpMinorCompaction(DMContext & context)
{
    size_t check_level_num = 0;
    while (check_level_num < column_stable_file_levels.size())
    {
        auto compaction = std::make_shared<MinorCompaction>(next_compaction_level);
        auto & level = column_stable_file_levels[next_compaction_level];
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
                    bool schema_ok = cur_task.to_compact.empty();
                    if (schema_ok)
                    {
                        if (auto * last_t_file = cur_task.to_compact.back()->tryToTinyFile(); last_t_file)
                        {
                            schema_ok = t_file->getSchema() == last_t_file->getSchema();
                        }
                        else
                        {
                            schema_ok = false;
                        }
                    }

                    if (cur_task_full || !schema_ok)
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
    ColumnStableFileLevels new_column_stable_file_levels;
    for (size_t i = 0; i < compaction->compaction_src_level; i++)
    {
        new_column_stable_file_levels.push_back(column_stable_file_levels[i]);
    }
    // the next level is empty because all the column file is compacted to next level
    new_column_stable_file_levels.push_back(ColumnStableFileLevel{});
    ColumnStableFileLevel new_level_after_compact;
    if (column_stable_file_levels.size() > compaction->compaction_src_level + 1)
    {
        for (auto & column_file : column_stable_file_levels[compaction->compaction_src_level + 1])
        {
            new_level_after_compact.emplace_back(column_file);
        }
    }
    for (auto & task : compaction->tasks)
    {
        if (task.is_trivial_move)
        {
            new_level_after_compact.push_back(task.to_compact[0]);
        }
        else
        {
            new_level_after_compact.push_back(task.result);
        }
    }
    new_column_stable_file_levels.push_back(new_level_after_compact);
    for (size_t i = compaction->compaction_src_level + 2; i < column_stable_file_levels.size(); i++)
    {
        new_column_stable_file_levels.push_back(column_stable_file_levels[i]);
    }

    /// Save the new metadata of packs to disk.
    serializeColumnStableFileLevels(wbs, metadata_id, new_column_stable_file_levels);
    wbs.writeMeta();

    /// Update packs in memory.
    column_stable_file_levels.swap(new_column_stable_file_levels);
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
    for (const auto & level : column_stable_file_levels)
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
