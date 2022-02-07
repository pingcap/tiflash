#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/ColumnFilePersistedSet.h>
#include <Storages/DeltaMerge/Delta/MinorCompaction.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/Page/PageStorage.h>

namespace DB
{
namespace DM
{
MinorCompaction::MinorCompaction(size_t compaction_src_level_, size_t current_compaction_version_)
    : compaction_src_level{compaction_src_level_}
    , current_compaction_version{current_compaction_version_}
{}

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

            // We ensure schema of all column files are the same
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
        auto compact_column_file = ColumnFileTiny::writeColumnFile(context, compact_block, 0, compact_rows, wbs, task.to_compact.front()->tryToTinyFile()->getSchema());
        wbs.writeLogAndData();
        task.result = compact_column_file;

        total_compact_files += task.to_compact.size();
        total_compact_rows += compact_rows;
    }
}

bool MinorCompaction::commit(ColumnFilePersistedSetPtr & persisted_file_set, WriteBatches & wbs)
{
    return persisted_file_set->installCompactionResults(shared_from_this(), wbs);
}

String MinorCompaction::info() const
{
    return fmt::format("Compacted {} column files into {} column files, total {} rows.", total_compact_files, tasks.size(), total_compact_rows);
}
} // namespace DM
} // namespace DB
