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
    FlushPackTask(const DeltaPackPtr & pack_) : pack(pack_) {}

    DeltaPackPtr pack;

    Block  block_data;
    PageId data_page = 0;

    bool   sorted         = false;
    size_t rows_offset    = 0;
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
    WriteBatches   wbs(context.storage_pool);

    size_t flush_rows    = 0;
    size_t flush_bytes   = 0;
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

        size_t total_rows    = 0;
        size_t total_deletes = 0;
        for (auto & pack : packs)
        {
            if (unlikely(!tasks.empty() && pack->isSaved()))
            {
                String msg = "Pack should not already saved, because previous packs are not saved.";

                LOG_ERROR(log, simpleInfo() << msg << " Packs: " << packsToString(packs));
                throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
            }


            if (!pack->isSaved())
            {
                auto & task = tasks.emplace_back(pack);

                if (auto dpb = pack->tryToBlock(); dpb)
                {
                    // Stop other threads appending to this pack.
                    dpb->disableAppend();

                    if (!dpb->getDataPageId())
                    {
                        if (unlikely(!dpb->getCache()))
                            throw Exception("Mutable pack does not have cache", ErrorCodes::LOGICAL_ERROR);


                        task.rows_offset    = total_rows;
                        task.deletes_offset = total_deletes;
                        task.block_data     = dpb->readFromCache();
                    }
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
    DeltaIndexPtr       new_delta_index;
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

            task.data_page = DeltaPackBlock::writePackData(context, task.block_data, 0, task.block_data.rows(), wbs);
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

        DeltaPacks::iterator flush_start_point;
        DeltaPacks::iterator flush_end_point;

        {
            /// Do some checks before continue, in case other threads do some modifications during current operation,
            /// as we didn't always hold the lock.

            auto p_it = packs.begin();
            auto t_it = tasks.begin();
            for (; p_it != packs.end(); ++p_it)
            {
                if (*p_it == t_it->pack)
                    break;
            }

            flush_start_point = p_it;

            for (; t_it != tasks.end(); ++t_it, ++p_it)
            {
                if (p_it == packs.end() || *p_it != t_it->pack || (*p_it)->isSaved())
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
        DeltaPacks packs_copy(packs.begin(), flush_start_point);
        for (auto & task : tasks)
        {
            // Use a new pack instance to do the serializing.
            DeltaPackPtr new_pack;
            if (auto dp_block = task.pack->tryToBlock(); dp_block)
            {
                auto new_dpb = std::make_shared<DeltaPackBlock>(*dp_block);
                // If it's data have been updated, use the new pages info.
                if (task.data_page != 0)
                    new_dpb->setDataPageId(task.data_page);
                if (task.sorted)
                    new_dpb->setCache(std::make_shared<DeltaPackBlock::Cache>(std::move(task.block_data)));

                new_pack = new_dpb;
            }
            else if (auto dp_file = task.pack->tryToFile(); dp_file)
            {
                new_pack = std::make_shared<DeltaPackFile>(*dp_file);
            }
            else if (auto dp_delete = task.pack->tryToDeleteRange(); dp_delete)
            {
                new_pack = std::make_shared<DeltaPackDeleteRange>(*dp_delete);
            }
            else
            {
                throw Exception("Unexpected delta pack type", ErrorCodes::LOGICAL_ERROR);
            }

            new_pack->setSaved();

            packs_copy.push_back(new_pack);
        }
        packs_copy.insert(packs_copy.end(), flush_end_point, packs.end());

        if constexpr (DM_RUN_CHECK)
        {
            size_t check_unsaved_rows    = 0;
            size_t check_unsaved_deletes = 0;
            size_t total_rows            = 0;
            size_t total_deletes         = 0;
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
            if (unlikely(check_unsaved_rows + flush_rows != unsaved_rows             //
                         || check_unsaved_deletes + flush_deletes != unsaved_deletes //
                         || total_rows != rows                                       //
                         || total_deletes != deletes))
                throw Exception("Rows and deletes check failed", ErrorCodes::LOGICAL_ERROR);
        }

        /// Save the new metadata of packs to disk.
        MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
        serializeSavedPacks(buf, packs_copy);
        const auto data_size = buf.count();

        wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
        wbs.writeMeta();

        /// Commit updates in memory.
        packs.swap(packs_copy);

        /// Update delta tree
        if (new_delta_index)
            delta_index = new_delta_index;

        for (auto & pack : packs)
        {
            if (auto dp_block = pack->tryToBlock(); dp_block && dp_block->getCache() && dp_block->getDataPageId() != 0
                && (pack->getRows() >= context.delta_small_pack_rows || pack->getBytes() >= context.delta_small_pack_bytes))
            {
                // This pack is too large to use cache.
                dp_block->clearCache();
            }
        }

        unsaved_rows -= flush_rows;
        unsaved_bytes -= flush_bytes;
        unsaved_deletes -= flush_deletes;

        LOG_DEBUG(log,
                  simpleInfo() << " Flush end. Flushed " << tasks.size() << " packs, " << flush_rows << " rows and " << flush_deletes
                               << " deletes.");
    }


    ProfileEvents::increment(ProfileEvents::DMWriteBytes, flush_bytes);

    // Also update the write amplification
    auto total_write  = ProfileEvents::counters[ProfileEvents::DMWriteBytes].load(std::memory_order_relaxed);
    auto actual_write = ProfileEvents::counters[ProfileEvents::PSMWriteBytes].load(std::memory_order_relaxed)
        + ProfileEvents::counters[ProfileEvents::WriteBufferFromFileDescriptorWriteBytes].load(std::memory_order_relaxed)
        + ProfileEvents::counters[ProfileEvents::WriteBufferAIOWriteBytes].load(std::memory_order_relaxed);
    GET_METRIC(context.metrics, tiflash_storage_write_amplification)
        .Set((double)(actual_write / 1024 / 1024) / (total_write / 1024 / 1024));

    return true;
}

} // namespace DB::DM
