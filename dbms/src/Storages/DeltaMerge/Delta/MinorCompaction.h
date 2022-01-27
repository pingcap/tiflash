#pragma once

#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>

namespace DB
{
namespace DM
{
class ColumnFilePersistedSet;
using ColumnFilePersistedSetPtr = std::shared_ptr<ColumnFilePersistedSet>;
class MinorCompaction;
using MinorCompactionPtr = std::shared_ptr<MinorCompaction>;

class MinorCompaction : public std::enable_shared_from_this<MinorCompaction>
{
    friend class ColumnFilePersistedSet;

public:
    struct Task
    {
        Task() = default;

        ColumnFilePersisteds to_compact;
        size_t total_rows = 0;
        size_t total_bytes = 0;

        bool is_trivial_move = false;
        ColumnFilePersistedPtr result;

        void addColumnFile(const ColumnFilePersistedPtr & column_file)
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

    size_t total_compact_files = 0;
    size_t total_compact_rows = 0;

public:
    explicit MinorCompaction(size_t compaction_src_level_);

    // return whether this task is a trivial move
    inline bool packUpTask(Task && task)
    {
        if (task.to_compact.empty())
            return true;

        bool is_trivial_move = false;
        if (task.to_compact.size() == 1)
        {
            // Maybe this column file is small, but it cannot be merged with other packs, so also remove it's cache if possible.
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

    bool commit(ColumnFilePersistedSetPtr & persisted_file_set, WriteBatches & wbs);

    String info() const;
};
} // namespace DM
} // namespace DB
