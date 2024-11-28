// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

/// Combine small `ColumnFileTiny`s to a bigger one and move it to the next level.
/// For `ColumnFileBig` and `ColumnFileDeleteRange`, it just moves it to the next level.
class MinorCompaction : public std::enable_shared_from_this<MinorCompaction>
{
public:
    struct Task
    {
        Task() = default;

        ColumnFilePersisteds to_compact;
        size_t first_file_index = std::numeric_limits<size_t>::max();
        size_t total_rows = 0;
        size_t total_bytes = 0;

        bool is_trivial_move = false;
        ColumnFilePersistedPtr result;

        void addColumnFile(const ColumnFilePersistedPtr & column_file, size_t index)
        {
            to_compact.push_back(column_file);
            first_file_index = std::min(first_file_index, index);
            total_rows += column_file->getRows();
            total_bytes += column_file->getBytes();
        }
    };
    using Tasks = std::vector<Task>;

private:
    Tasks tasks;

    // The index of the first cftiny which can be compacted
    size_t first_compact_index = std::numeric_limits<size_t>::max();

    size_t current_compaction_version;

    size_t total_compact_files = 0;
    size_t total_compact_rows = 0;
    size_t total_compact_bytes = 0;
    size_t result_compact_files = 0;

public:
    explicit MinorCompaction(size_t current_compaction_version_);

    // Add new task and return whether this task is a trivial move
    inline bool packUpTask(Task && task)
    {
        if (task.to_compact.empty())
            return true;

        bool is_trivial_move = false;
        if (task.to_compact.size() == 1)
        {
            is_trivial_move = true;
        }

        task.is_trivial_move = is_trivial_move;
        tasks.push_back(std::move(task));
        if (!is_trivial_move)
            first_compact_index = std::min(first_compact_index, task.first_file_index);
        return is_trivial_move;
    }

    const Tasks & getTasks() const { return tasks; }

    size_t getFirsCompactIndex() const { return first_compact_index; }

    size_t getCompactionVersion() const { return current_compaction_version; }

    // The stats about compaction. Only effective after `prepare` is called.
    size_t getTotalCompactRows() const { return total_compact_rows; }
    size_t getTotalCompactBytes() const { return total_compact_bytes; }

    /// Create new column file by combining several small `ColumnFileTiny`s
    void prepare(DMContext & context, WriteBatches & wbs, const PageReader & reader);

    /// Add new column files and remove old column files in `ColumnFilePersistedSet`
    bool commit(ColumnFilePersistedSetPtr & persisted_file_set, WriteBatches & wbs);

    String info() const;
};
} // namespace DM
} // namespace DB
