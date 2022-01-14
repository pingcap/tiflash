#include <Functions/FunctionHelpers.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/ColumnStableFileSet.h>
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

bool ColumnStableFileSet::installCompactionResults(const MinorCompactionPtr & compaction)
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
    serializeColumnStableFileLevels(compaction->wbs, metadata_id, new_column_stable_file_levels);
    compaction->wbs.writeMeta();

    /// Update packs in memory.
    column_stable_file_levels.swap(new_column_stable_file_levels);
}

ColumnStableFileSetSnapshotPtr ColumnStableFileSet::createSnapshot(const DMContext & context)
{
    auto snap = std::make_shared<ColumnStableFileSetSnapshot>();
    snap->storage_snap = std::make_shared<StorageSnapshot>(context.storage_pool, context.getReadLimiter(), true);

    snap->column_file_set_snapshot = std::make_shared<ColumnFileSetSnapshot>(snap->storage_snap);
    snap->column_file_set_snapshot->rows = rows;
    snap->column_file_set_snapshot->bytes = bytes;
    snap->column_file_set_snapshot->deletes = deletes;

    size_t total_rows = 0;
    size_t total_deletes = 0;
    for (const auto & level : column_stable_file_levels)
    {
        for (const auto & file : level)
        {
            snap->column_file_set_snapshot->column_files.push_back(file);
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
