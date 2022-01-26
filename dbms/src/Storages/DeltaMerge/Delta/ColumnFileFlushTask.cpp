#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/ColumnFileFlushTask.h>
#include <Storages/DeltaMerge/Delta/ColumnFilePersistedSet.h>
#include <Storages/DeltaMerge/Delta/MemTableSet.h>

namespace DB
{
namespace DM
{
ColumnFileFlushTask::ColumnFileFlushTask(DMContext & context_, const MemTableSetPtr & mem_table_set_, size_t flush_version_)
    : context{context_}
    , mem_table_set{mem_table_set_}
    , flush_version{flush_version_}
{}

DeltaIndex::Updates ColumnFileFlushTask::prepare(WriteBatches & wbs)
{
    DeltaIndex::Updates delta_index_updates;
    /// Write prepared data to disk.
    for (auto & task : tasks)
    {
        if (!task.block_data)
        {
            results.push_back(std::static_pointer_cast<ColumnFilePersisted>(task.column_file));
        }
        else
        {
            IColumn::Permutation perm;
            task.sorted = sortBlockByPk(getExtraHandleColumnDefine(context.is_common_handle), task.block_data, perm);
            if (task.sorted)
                delta_index_updates.emplace_back(task.deletes_offset, task.rows_offset, perm);

            auto * mem_file = task.column_file->tryToInMemoryFile();
            ColumnFilePersistedPtr tiny_file;
            bool is_small_file = mem_file->getRows() < context.delta_small_pack_rows || mem_file->getBytes() < context.delta_small_pack_bytes;
            if (is_small_file)
            {
                tiny_file = std::make_shared<ColumnFileTiny>(mem_file->getSchema(),
                                                             mem_file->getRows(),
                                                             mem_file->getBytes(),
                                                             task.data_page,
                                                             !task.sorted ? mem_file->getCache() : std::make_shared<ColumnFile::Cache>(std::move(task.block_data)));
            }
            else
            {
                tiny_file = std::make_shared<ColumnFileTiny>(mem_file->getSchema(),
                                                             mem_file->getRows(),
                                                             mem_file->getBytes(),
                                                             task.data_page,
                                                             nullptr);
            }
            results.push_back(tiny_file);
        }
    }

    wbs.writeLogAndData();
    return delta_index_updates;
}

bool ColumnFileFlushTask::commit(ColumnFilePersistedSetPtr & persisted_file_set, WriteBatches & wbs)
{
    // update metadata
    if (!persisted_file_set->appendColumnStableFilesToLevel0(flush_version, results, wbs))
        return false;

    mem_table_set->removeColumnFilesInFlushTask(*this);
    return true;
}
} // namespace DM
} // namespace DB
