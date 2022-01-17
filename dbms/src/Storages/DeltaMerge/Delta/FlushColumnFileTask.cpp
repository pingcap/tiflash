#include "FlushColumnFileTask.h"

#include <Storages/DeltaMerge/ColumnFile/ColumnInMemoryFile.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/MemTableSet.h>


namespace DB
{
namespace DM
{
FlushColumnFileTask::FlushColumnFileTask(DMContext & context_, const MemTableSetPtr & mem_table_set_)
    : context{context_},
      mem_table_set{mem_table_set_},
      log(&Poco::Logger::get("FlushColumnFileTask"))
{}

DeltaIndex::Updates FlushColumnFileTask::prepare(WriteBatches & wbs)
{
    DeltaIndex::Updates delta_index_updates;
    /// Write prepared data to disk.
    for (auto & task : tasks)
    {
        if (!task.block_data)
        {
            results.push_back(std::static_pointer_cast<ColumnStableFile>(task.column_file));
        }
        else
        {
            IColumn::Permutation perm;
            task.sorted = sortBlockByPk(getExtraHandleColumnDefine(context.is_common_handle), task.block_data, perm);
            if (task.sorted)
                delta_index_updates.emplace_back(task.deletes_offset, task.rows_offset, perm);

            auto * mem_file = task.column_file->tryToInMemoryFile();
            ColumnStableFilePtr tiny_file;
            // check whether to keep the cache from `mem_file`
            if (mem_file->getCache() && (mem_file->getRows() < context.delta_small_pack_rows || mem_file->getBytes() < context.delta_small_pack_bytes))
            {
                tiny_file = ColumnTinyFile::writeColumnFile(context, task.block_data, 0, task.block_data.rows(), wbs, mem_file->getSchema(), mem_file->getCache());
            }
            else
            {
                tiny_file = ColumnTinyFile::writeColumnFile(context, task.block_data, 0, task.block_data.rows(), wbs, mem_file->getSchema());
            }
            results.push_back(tiny_file);
        }
    }

    wbs.writeLogAndData();
    return delta_index_updates;
}

bool FlushColumnFileTask::commit(ColumnStableFileSetPtr & stable_file_set, WriteBatches & wbs)
{
    // update metadata
    if (!stable_file_set->appendColumnStableFilesToLevel0(current_flush_version, results, wbs))
        return false;

    mem_table_set->removeColumnFilesInFlushTask(*this);
    return true;
}
}
}
