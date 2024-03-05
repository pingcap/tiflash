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

#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/Page/PageDefinesBase.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{
struct WriteBatches;
class MemTableSet;
using MemTableSetPtr = std::shared_ptr<MemTableSet>;
class ColumnFilePersistedSet;
using ColumnFilePersistedSetPtr = std::shared_ptr<ColumnFilePersistedSet>;
class ColumnFileFlushTask;
using ColumnFileFlushTaskPtr = std::shared_ptr<ColumnFileFlushTask>;

class ColumnFileFlushTask
{
public:
    struct Task
    {
        explicit Task(const ColumnFilePtr & column_file_)
            : column_file(column_file_)
        {}

        ColumnFilePtr column_file;

        Block block_data;
        PageIdU64 data_page = 0;

        bool sorted = false;
        size_t rows_offset = 0;
        size_t deletes_offset = 0;
    };
    using Tasks = std::vector<Task>;

private:
    Tasks tasks;
    DMContext & context;
    MemTableSetPtr mem_table_set;
    size_t flush_version;

    size_t flush_rows = 0;
    size_t flush_bytes = 0;
    size_t flush_deletes = 0;

public:
    ColumnFileFlushTask(DMContext & context_, const MemTableSetPtr & mem_table_set_, size_t flush_version_);

    inline Task & addColumnFile(ColumnFilePtr column_file)
    {
        flush_rows += column_file->getRows();
        flush_bytes += column_file->getBytes();
        flush_deletes += column_file->getDeletes();
        return tasks.emplace_back(column_file);
    }

    const Tasks & getAllTasks() const { return tasks; }

    size_t getTaskNum() const { return tasks.size(); }
    size_t getFlushRows() const { return flush_rows; }
    size_t getFlushBytes() const { return flush_bytes; }
    size_t getFlushDeletes() const { return flush_deletes; }

    // Persist data in ColumnFileInMemory
    DeltaIndex::Updates prepare(WriteBatches & wbs);

    // Add the flushed column file to ColumnFilePersistedSet and remove the corresponding column file from MemTableSet
    // Needs extra synchronization on the DeltaValueSpace
    bool commit(ColumnFilePersistedSetPtr & persisted_file_set, WriteBatches & wbs);
};
} // namespace DM
} // namespace DB
