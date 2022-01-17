#pragma once

#include "Storages/DeltaMerge/ColumnFile/ColumnStableFile.h"
#include "Storages/DeltaMerge/ColumnFile/ColumnTinyFile.h"

namespace DB
{
namespace DM
{
class ColumnStableFileSet;
using ColumnStableFileSetPtr = std::shared_ptr<ColumnStableFileSet>;

class MinorCompaction : public std::enable_shared_from_this<MinorCompaction>
{
    friend class ColumnStableFileSet;
public:
    struct Task
    {
        Task() {}

        ColumnStableFiles to_compact;
        size_t total_rows = 0;
        size_t total_bytes = 0;

        bool is_trivial_move = false;
        ColumnStableFilePtr result;

        void addColumnFile(const ColumnStableFilePtr & column_file)
        {
            total_rows += column_file->getRows();
            total_bytes += column_file->getBytes();
            to_compact.push_back(column_file);
        }
    };
    using Tasks = std::vector<Task>;

private:
    Tasks tasks;

    size_t compaction_src_level;
    size_t current_compaction_version;
    ColumnStableFileSetPtr column_stable_file_set;

    size_t total_compact_files = 0;
    size_t total_compact_rows = 0;

public:
    MinorCompaction(size_t compaction_src_level_);

    // return whether this task is a trivial move
    inline bool packUpTask(Task && task)
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

    void prepare(DMContext & context, WriteBatches & wbs, const PageReader & reader);

    bool commit(WriteBatches & wbs);
};

using MinorCompactionPtr = std::shared_ptr<MinorCompaction>;
}
}