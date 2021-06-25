#include <Common/CurrentMetrics.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/Page/PageStorage.h>

#include <ext/scope_guard.h>
#include <vector>

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfDeltaCompact;
} // namespace CurrentMetrics

namespace DB::DM
{

struct CompackTask
{
    CompackTask() {}

    DeltaPacks to_compact;
    size_t     total_rows  = 0;
    size_t     total_bytes = 0;

    DeltaPackPtr result;

    void addPack(const DeltaPackPtr & pack)
    {
        total_rows += pack->getRows();
        total_bytes += pack->getBytes();
        to_compact.push_back(pack);
    }
};
using CompackTasks = std::vector<CompackTask>;

bool DeltaValueSpace::compact(DMContext & context)
{
    LOG_DEBUG(log, info() << " Compact start");

    bool v = false;
    // Other thread is doing structure update, just return.
    if (!is_updating.compare_exchange_strong(v, true))
    {
        LOG_DEBUG(log, simpleInfo() << " Compact stop because updating");

        return true;
    }
    SCOPE_EXIT({
        bool v = true;
        if (!is_updating.compare_exchange_strong(v, false))
            throw Exception(simpleInfo() + " is expected to be updating", ErrorCodes::LOGICAL_ERROR);
    });

    CompackTasks              tasks;
    PageStorage::SnapshotPtr  log_storage_snap;
    CurrentMetrics::Increment snapshot_metrics{CurrentMetrics::DT_SnapshotOfDeltaCompact, 0};

    {
        /// Prepare compact tasks.

        std::scoped_lock lock(mutex);
        if (abandoned.load(std::memory_order_relaxed))
        {
            LOG_DEBUG(log, simpleInfo() << " Compact stop because abandoned");
            return false;
        }

        CompackTask cur_task;
        for (auto & pack : packs)
        {
            if (!pack->isSaved())
                break;

            auto packup_cur_task = [&]() {
                if (cur_task.to_compact.size() >= 2)
                {
                    tasks.push_back(std::move(cur_task));
                }
                else
                {
                    // Maybe this pack is small, but it cannot be merged with other packs, so also remove it's cache.
                    for (auto & p : cur_task.to_compact)
                    {
                        p->tryToBlock()->clearCache();
                    }
                }

                cur_task = {};
            };

            if (auto dp_block = pack->tryToBlock(); dp_block)
            {
                if (unlikely(!dp_block->getDataPageId()))
                    throw Exception("Saved DeltaPackBlock does not have data_page_id", ErrorCodes::LOGICAL_ERROR);

                bool cur_task_full = cur_task.total_rows >= context.delta_limit_rows || cur_task.total_bytes >= context.delta_limit_bytes;
                bool small_pack    = pack->getRows() < context.delta_small_pack_rows && pack->getBytes() < context.delta_small_pack_bytes;
                bool schema_ok
                    = cur_task.to_compact.empty() || dp_block->getSchema() == cur_task.to_compact.back()->tryToBlock()->getSchema();

                if (cur_task_full || !small_pack || !schema_ok)
                    packup_cur_task();

                if (small_pack)
                    cur_task.addPack(pack);
                else
                    // Then this pack's cache should not exist.
                    dp_block->clearCache();
            }
            else
            {
                packup_cur_task();
            }
        }
        if (cur_task.to_compact.size() >= 2)
            tasks.push_back(std::move(cur_task));

        if (tasks.empty())
        {
            LOG_DEBUG(log, simpleInfo() << " Nothing to compact");
            return true;
        }

        log_storage_snap = context.storage_pool.log().getSnapshot();
        snapshot_metrics.changeTo(1); // add metrics for snapshot
    }

    /// Write generated compact packs' data.

    size_t total_compact_packs = 0;
    size_t total_compact_rows  = 0;

    WriteBatches wbs(context.storage_pool);
    PageReader   reader(context.storage_pool.log(), std::move(log_storage_snap));
    for (auto & task : tasks)
    {
        auto & schema          = *(task.to_compact[0]->tryToBlock()->getSchema());
        auto   compact_columns = schema.cloneEmptyColumns();

        // Read data from old packs
        for (auto & pack : task.to_compact)
        {
            auto dp_block = pack->tryToBlock();
            if (unlikely(!dp_block))
                throw Exception("The compact candidate is not a DeltaPackBlock", ErrorCodes::LOGICAL_ERROR);

            // We ensure schema of all packs are the same
            Block  block      = dp_block->isCached() ? dp_block->readFromCache() : dp_block->readFromDisk(reader);
            size_t block_rows = block.rows();
            for (size_t i = 0; i < schema.columns(); ++i)
            {
                compact_columns[i]->insertRangeFrom(*block.getByPosition(i).column, 0, block_rows);
            }

            wbs.removed_log.delPage(dp_block->getDataPageId());
        }

        Block compact_block = schema.cloneWithColumns(std::move(compact_columns));
        auto  compact_rows  = compact_block.rows();

        // Note that after compact, caches are no longer exist.

        auto compact_pack = DeltaPackBlock::writePack(context, compact_block, 0, compact_rows, wbs);
        // Use the original schema instance, so that we can avoid serialize the new schema instance.
        compact_pack->tryToBlock()->setSchema(task.to_compact.front()->tryToBlock()->getSchema());
        compact_pack->tryToBlock()->setSaved();

        wbs.writeLogAndData();
        task.result = compact_pack;

        total_compact_packs += task.to_compact.size();
        total_compact_rows += compact_rows;
    }

    {
        std::scoped_lock lock(mutex);

        /// Check before commit.
        if (abandoned.load(std::memory_order_relaxed))
        {
            wbs.rollbackWrittenLogAndData();
            LOG_DEBUG(log, simpleInfo() << " Stop compact because abandoned");
            return false;
        }

        DeltaPacks new_packs;
        auto       old_packs_offset = packs.begin();
        for (auto & task : tasks)
        {
            auto old_it    = old_packs_offset;
            auto locate_it = [&](const DeltaPackPtr & pack) {
                for (; old_it != packs.end(); ++old_it)
                {
                    if (*old_it == pack)
                        return old_it;
                }
                return old_it;
            };

            auto start_it = locate_it(task.to_compact.front());
            auto end_it   = locate_it(task.to_compact.back());

            if (unlikely(start_it == packs.end() || end_it == packs.end()))
            {
                LOG_WARNING(log, "Structure has been updated during compact");
                wbs.rollbackWrittenLogAndData();
                LOG_DEBUG(log, simpleInfo() << " Compact stop because structure got updated");
                return false;
            }

            new_packs.insert(new_packs.end(), old_packs_offset, start_it);
            new_packs.push_back(task.result);

            old_packs_offset = end_it + 1;
        }
        new_packs.insert(new_packs.end(), old_packs_offset, packs.end());

        checkPacks(new_packs);

        /// Save the new metadata of packs to disk.
        MemoryWriteBuffer buf(0, PACK_SERIALIZE_BUFFER_SIZE);
        serializeSavedPacks(buf, new_packs);
        const auto data_size = buf.count();

        wbs.meta.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
        wbs.writeMeta();

        /// Update packs in memory.
        packs.swap(new_packs);

        last_try_compact_packs = std::min(packs.size(), last_try_compact_packs.load());

        LOG_DEBUG(log,
                  simpleInfo() << " Successfully compacted " << total_compact_packs << " packs into " << tasks.size() << " packs, total "
                               << total_compact_rows << " rows.");
    }

    wbs.writeRemoves();

    return true;
}

} // namespace DB::DM
