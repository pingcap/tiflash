#pragma once

#include "Storages/DeltaMerge/ColumnFile/ColumnStableFile.h"

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
    inline bool packUpTask(Task && task);

    void prepare(DMContext & context, WriteBatches & wbs, const PageReader & reader);

    bool commit();
};

using MinorCompactionPtr = std::shared_ptr<MinorCompaction>;
}
}