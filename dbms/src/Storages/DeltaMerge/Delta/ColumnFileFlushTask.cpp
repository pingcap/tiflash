#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/ColumnFileFlushTask.h>
#include <Storages/DeltaMerge/Delta/ColumnFilePersistedSet.h>
#include <Storages/DeltaMerge/Delta/MemTableSet.h>

namespace ProfileEvents
{
extern const Event DMWriteBytes;
extern const Event PSMWriteBytes;
extern const Event WriteBufferFromFileDescriptorWriteBytes;
extern const Event WriteBufferAIOWriteBytes;
} // namespace ProfileEvents

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
            continue;

        IColumn::Permutation perm;
        task.sorted = sortBlockByPk(getExtraHandleColumnDefine(context.is_common_handle), task.block_data, perm);
        if (task.sorted)
            delta_index_updates.emplace_back(task.deletes_offset, task.rows_offset, perm);

        task.data_page = ColumnFileTiny::writeColumnFileData(context, task.block_data, 0, task.block_data.rows(), wbs);
    }

    wbs.writeLogAndData();
    return delta_index_updates;
}

bool ColumnFileFlushTask::commit(ColumnFilePersistedSetPtr & persisted_file_set, WriteBatches & wbs)
{
    if (!persisted_file_set->checkAndIncreaseFlushVersion(flush_version))
        return false;

    /// Create new column file instance for ColumnFilePersistedSet
    ColumnFilePersisteds new_column_files;
    for (auto & task : tasks)
    {
        ColumnFilePersistedPtr new_column_file;
        if (auto * m_file = task.column_file->tryToInMemoryFile(); m_file)
        {
            // Just keep cache for really small column file
            ColumnFile::CachePtr column_file_cache = nullptr;
            if (m_file->getRows() < context.delta_small_column_file_rows || m_file->getBytes() < context.delta_small_column_file_bytes)
            {
                column_file_cache = !task.sorted ? m_file->getCache() : std::make_shared<ColumnFile::Cache>(std::move(task.block_data));
            }
            new_column_file = std::make_shared<ColumnFileTiny>(m_file->getSchema(),
                                                               m_file->getRows(),
                                                               m_file->getBytes(),
                                                               task.data_page,
                                                               column_file_cache);
        }
        else if (auto * t_file = task.column_file->tryToTinyFile(); t_file)
        {
            new_column_file = std::make_shared<ColumnFileTiny>(*t_file);
        }
        else if (auto * b_file = task.column_file->tryToBigFile(); b_file)
        {
            new_column_file = std::make_shared<ColumnFileBig>(*b_file);
        }
        else if (auto * d_file = task.column_file->tryToDeleteRange(); d_file)
        {
            new_column_file = std::make_shared<ColumnFileDeleteRange>(*d_file);
        }
        else
        {
            throw Exception("Unexpected column file type", ErrorCodes::LOGICAL_ERROR);
        }
        new_column_files.push_back(new_column_file);
    }

    // serialize metadata and update persisted_file_set
    if (!persisted_file_set->appendPersistedColumnFilesToLevel0(new_column_files, wbs))
        return false;

    mem_table_set->removeColumnFilesInFlushTask(*this);

    // Also update the write amplification
    auto total_write = ProfileEvents::counters[ProfileEvents::DMWriteBytes].load(std::memory_order_relaxed);
    auto actual_write = ProfileEvents::counters[ProfileEvents::PSMWriteBytes].load(std::memory_order_relaxed)
        + ProfileEvents::counters[ProfileEvents::WriteBufferFromFileDescriptorWriteBytes].load(std::memory_order_relaxed)
        + ProfileEvents::counters[ProfileEvents::WriteBufferAIOWriteBytes].load(std::memory_order_relaxed);
    GET_METRIC(tiflash_storage_write_amplification)
        .Set((static_cast<double>(actual_write) / 1024 / 1024) / (static_cast<double>(total_write) / 1024 / 1024));
    return true;
}
} // namespace DM
} // namespace DB
