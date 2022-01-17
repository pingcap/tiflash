#include "MinorCompaction.h"

#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnTinyFile.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/ColumnStableFileSet.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/Page/PageStorage.h>

namespace DB
{
namespace DM
{
MinorCompaction::MinorCompaction(size_t compaction_src_level_)
    : compaction_src_level{compaction_src_level_}
{}

inline bool MinorCompaction::packUpTask(Task && task)
{
    if (unlikely(task.to_compact.empty()))
        throw Exception("task shouldn't be empty", ErrorCodes::LOGICAL_ERROR);

    bool is_trivial_move = false;
    if (task.to_compact.size() == 1)
    {
        // Maybe this column file is small, but it cannot be merged with other packs, so also remove it's cache.
        for (auto & f : task.to_compact)
        {
            if (auto * t_file = f->tryToTinyFile(); t_file)
            {
                t_file->clearCache();
            }
        }
        is_trivial_move = true;
    }
    task.is_trivial_move = is_trivial_move;
    tasks.push_back(std::move(task));
    return is_trivial_move;
}

void MinorCompaction::prepare(DMContext & context, WriteBatches & wbs, const PageReader & reader)
{
    for (auto & task : tasks)
    {
        if (task.is_trivial_move)
            continue;

        auto & schema = *(task.to_compact[0]->tryToTinyFile()->getSchema());
        auto compact_columns = schema.cloneEmptyColumns();
        for (auto & file : task.to_compact)
        {
            auto * t_file = file->tryToTinyFile();
            if (unlikely(!t_file))
                throw Exception("The compact candidate is not a ColumnTinyFile", ErrorCodes::LOGICAL_ERROR);

            // We ensure schema of all packs are the same
            Block block = t_file->readBlockForMinorCompaction(reader);
            size_t block_rows = block.rows();
            for (size_t i = 0; i < schema.columns(); ++i)
            {
                compact_columns[i]->insertRangeFrom(*block.getByPosition(i).column, 0, block_rows);
            }

            wbs.removed_log.delPage(t_file->getDataPageId());
        }
        Block compact_block = schema.cloneWithColumns(std::move(compact_columns));
        auto compact_rows = compact_block.rows();
        auto compact_column_file = ColumnTinyFile::writeColumnFile(context, compact_block, 0, compact_rows, wbs, task.to_compact.front()->tryToTinyFile()->getSchema());
        wbs.writeLogAndData();
        task.result = compact_column_file;

        total_compact_files += task.to_compact.size();
        total_compact_rows += compact_rows;
    }
}

bool MinorCompaction::commit()
{
    return column_stable_file_set->installCompactionResults(shared_from_this());
}

}
}