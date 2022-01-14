#include "SegmentV2.h"
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/DMContext.h>

namespace DB
{
namespace DM
{
bool SegmentV2::flush(DB::DM::DMContext & context)
{
    size_t current_flush_version = 0;
    FlushColumnFileTasks flush_tasks;
    DeltaIndexPtr cur_delta_index;
    {
        std::scoped_lock lock(mutex);
        current_flush_version = flush_version;

        // FIXME: calculate right rows_offset and deletes_offset
        size_t rows_offset = 0;
        size_t deletes_offset = 0;
        flush_tasks = mem_table_set->buildFlushTasks(rows_offset, deletes_offset);
        cur_delta_index = delta_index;
    }

    // No update, return successfully.
    if (flush_tasks.empty())
    {
        // TODO: log
        return true;
    }

    DeltaIndex::Updates delta_index_updates;
    DeltaIndexPtr new_delta_index;
    {
        /// Write prepared data to disk.
        for (auto & task : flush_tasks)
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


    return false;
}
}
}