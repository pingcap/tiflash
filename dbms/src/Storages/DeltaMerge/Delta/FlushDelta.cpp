#include <Common/TiFlashMetrics.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/WriteBatches.h>

namespace ProfileEvents
{
extern const Event DMWriteBytes;
extern const Event PSMWriteBytes;
extern const Event WriteBufferFromFileDescriptorWriteBytes;
extern const Event WriteBufferAIOWriteBytes;
} // namespace ProfileEvents

namespace DB::DM
{
struct FlushPackTask
{
    FlushPackTask(const ColumnFilePtr & pack_)
        : pack(pack_)
    {}

    ColumnFilePtr pack;

    Block block_data;
    PageId data_page = 0;

    bool sorted = false;
    size_t rows_offset = 0;
    size_t deletes_offset = 0;
};
using FlushPackTasks = std::vector<FlushPackTask>;

bool DeltaValueSpace::flush(DMContext & context)
{
    LOG_DEBUG(log, info() << ", Flush start");

    /// We have two types of data needed to flush to disk:
    ///  1. The cache data in DeltaPackBlock
    ///  2. The serialized metadata of packs in DeltaValueSpace

    FlushPackTasks tasks;
    WriteBatches wbs(context.storage_pool, context.getWriteLimiter());

    size_t flush_rows = 0;
    size_t flush_bytes = 0;
    size_t flush_deletes = 0;

    DeltaIndexPtr cur_delta_index;
    {
        /// Prepare data which will be written to disk.
        std::scoped_lock lock(mutex);
        if (abandoned.load(std::memory_order_relaxed))
        {
            LOG_DEBUG(log, simpleInfo() << "Flush stop because abandoned");
            return false;
        }

        size_t total_rows = 0;
        size_t total_deletes = 0;
        for (auto & pack : column_files)
        {
            if (unlikely(!tasks.empty() && pack->isSaved()))
            {
                String msg = "Pack should not already saved, because previous packs are not saved.";

                LOG_ERROR(log, simpleInfo() << msg << " Packs: " << columnFilesToString(column_files));
                throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
            }


            if (!pack->isSaved())
            {
                auto & task = tasks.emplace_back(pack);

                if (auto * dpb = pack->tryToInMemoryFile(); dpb)
                {
                    // Stop other threads appending to this pack.
                    dpb->disableAppend();
                    task.rows_offset = total_rows;
                    task.deletes_offset = total_deletes;
                    task.block_data = dpb->readDataForFlush();
                }

                flush_rows += pack->getRows();
                flush_bytes += pack->getBytes();
                flush_deletes += pack->isDeleteRange();
            }

            total_rows += pack->getRows();
            total_deletes += pack->isDeleteRange();
        }

        if (unlikely(flush_rows != unsaved_rows || flush_deletes != unsaved_deletes || total_rows != rows || total_deletes != deletes))
            throw Exception("Rows and deletes check failed", ErrorCodes::LOGICAL_ERROR);

        cur_delta_index = delta_index;
    }

    // No update, return successfully.
    if (tasks.empty())
    {
        LOG_DEBUG(log, simpleInfo() << " Nothing to flush");
        return true;
    }

    DeltaIndex::Updates delta_index_updates;
    DeltaIndexPtr new_delta_index;
    {
        /// Write prepared data to disk.
        for (auto & task : tasks)
        {
            if (!task.block_data)
                continue;
            IColumn::Permutation perm;
            task.sorted = sortBlockByPk(getExtraHandleColumnDefine(context.is_common_handle), task.block_data, perm);
            if (task.sorted)
                delta_index_updates.emplace_back(task.deletes_offset, task.rows_offset, perm);

            task.data_page = ColumnTinyFile::writeColumnFileData(context, task.block_data, 0, task.block_data.rows(), wbs);
        }

        wbs.writeLogAndData();
    }

    if (!delta_index_updates.empty())
    {
        LOG_DEBUG(log, simpleInfo() << " Update index start");
        new_delta_index = cur_delta_index->cloneWithUpdates(delta_index_updates);
        LOG_DEBUG(log, simpleInfo() << " Update index done");
    }

    {
        /// If this instance is still valid, then commit.
        std::scoped_lock lock(mutex);

        if (abandoned.load(std::memory_order_relaxed))
        {
            // Delete written data.
            wbs.setRollback();
            LOG_DEBUG(log, simpleInfo() << " Flush stop because abandoned");
            return false;
        }

        ColumnFiles::iterator flush_start_point;
        ColumnFiles::iterator flush_end_point;

        {
            /// Do some checks before continue, in case other threads do some modifications during current operation,
            /// as we didn't always hold the lock.

            auto p_it = column_files.begin();
            auto t_it = tasks.begin();
            for (; p_it != column_files.end(); ++p_it)
            {
                if (*p_it == t_it->pack)
                    break;
            }

            flush_start_point = p_it;

            for (; t_it != tasks.end(); ++t_it, ++p_it)
            {
                if (p_it == column_files.end() || *p_it != t_it->pack || (*p_it)->isSaved())
                {
                    // The packs have been modified, or this pack already saved by another thread.
                    // Let's rollback and break up.
                    wbs.rollbackWrittenLogAndData();
                    LOG_DEBUG(log, simpleInfo() << " Stop flush because structure got updated");
                    return false;
                }
            }

            flush_end_point = p_it;
        }

        /// Things look good, let's continue.

        // Create a temporary packs copy, used to generate serialized data.
        // Save the previous saved packs, and the packs we are saving, and the later packs appended during the period we did not held the lock.
        ColumnFiles packs_copy(column_files.begin(), flush_start_point);
        for (auto & task : tasks)
        {
            // Use a new pack instance to do the serializing.
            ColumnFilePtr new_pack;
            if (auto * dp_block = task.pack->tryToInMemoryFile(); dp_block)
            {
                bool is_small_file = dp_block->getRows() < context.delta_small_pack_rows || dp_block->getBytes() < context.delta_small_pack_bytes;
                if (is_small_file)
                {
                    new_pack = std::make_shared<ColumnTinyFile>(dp_block->getSchema(),
                                                                dp_block->getRows(),
                                                                dp_block->getBytes(),
                                                                task.data_page,
                                                                !task.sorted ? dp_block->getCache() : std::make_shared<ColumnFile::Cache>(std::move(task.block_data)));
                }
                else
                {
                    new_pack = std::make_shared<ColumnTinyFile>(dp_block->getSchema(),
                                                                dp_block->getRows(),
                                                                dp_block->getBytes(),
                                                                task.data_page,
                                                                nullptr);
                }
            }
            else if (auto * t_file = task.pack->tryToTinyFile(); t_file)
            {
                new_pack = std::make_shared<ColumnTinyFile>(*t_file);
            }
            else if (auto * dp_file = task.pack->tryToBigFile(); dp_file)
            {
                new_pack = std::make_shared<ColumnBigFile>(*dp_file);
            }
            else if (auto * dp_delete = task.pack->tryToDeleteRange(); dp_delete)
            {
                new_pack = std::make_shared<ColumnDeleteRangeFile>(*dp_delete);
            }
            else
            {
                throw Exception("Unexpected column file type", ErrorCodes::LOGICAL_ERROR);
            }

            new_pack->setSaved();

            packs_copy.push_back(new_pack);
        }
        packs_copy.insert(packs_copy.end(), flush_end_point, column_files.end());

        if constexpr (DM_RUN_CHECK)
        {
            size_t check_unsaved_rows = 0;
            size_t check_unsaved_deletes = 0;
            size_t total_rows = 0;
            size_t total_deletes = 0;
            for (auto & pack : packs_copy)
            {
                if (!pack->isSaved())
                {
                    check_unsaved_rows += pack->getRows();
                    check_unsaved_deletes += pack->isDeleteRange();
                }
                total_rows += pack->getRows();
                total_deletes += pack->isDeleteRange();
            }
            if (unlikely(check_unsaved_rows + flush_rows != unsaved_rows //
                         || check_unsaved_deletes + flush_deletes != unsaved_deletes //
                         || total_rows != rows //
                         || total_deletes != deletes))
                throw Exception("Rows and deletes check failed", ErrorCodes::LOGICAL_ERROR);
        }

        /// Save the new metadata of packs to disk.
        MemoryWriteBuffer buf(0, COLUMN_FILE_SERIALIZE_BUFFER_SIZE);
        serializeColumnStableFiles(buf, packs_copy);
        const auto data_size = buf.count();

        wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
        wbs.writeMeta();

        /// Commit updates in memory.
        column_files.swap(packs_copy);

        /// Update delta tree
        if (new_delta_index)
            delta_index = new_delta_index;

        unsaved_rows -= flush_rows;
        unsaved_bytes -= flush_bytes;
        unsaved_deletes -= flush_deletes;

        LOG_DEBUG(log,
                  simpleInfo() << " Flush end. Flushed " << tasks.size() << " packs, " << flush_rows << " rows and " << flush_deletes
                               << " deletes.");
    }


    ProfileEvents::increment(ProfileEvents::DMWriteBytes, flush_bytes);

    // Also update the write amplification
    auto total_write = ProfileEvents::counters[ProfileEvents::DMWriteBytes].load(std::memory_order_relaxed);
    auto actual_write = ProfileEvents::counters[ProfileEvents::PSMWriteBytes].load(std::memory_order_relaxed)
        + ProfileEvents::counters[ProfileEvents::WriteBufferFromFileDescriptorWriteBytes].load(std::memory_order_relaxed)
        + ProfileEvents::counters[ProfileEvents::WriteBufferAIOWriteBytes].load(std::memory_order_relaxed);
    GET_METRIC(tiflash_storage_write_amplification)
        .Set((double)(actual_write / 1024 / 1024) / (total_write / 1024 / 1024));

    return true;
}

} // namespace DB::DM
