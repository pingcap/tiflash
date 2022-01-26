#pragma once

#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/DeltaIndex.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/Page/PageDefines.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{
class MemTableSet;
using MemTableSetPtr = std::shared_ptr<MemTableSet>;
class ColumnFilePersistedSet;
using ColumnFilePersistedSetPtr = std::shared_ptr<ColumnFilePersistedSet>;
class ColumnFileFlushTask;
using ColumnFileFlushTaskPtr = std::shared_ptr<ColumnFileFlushTask>;

class ColumnFileFlushTask
{
    friend class MemTableSet;
    friend class ColumnFilePersistedSet;

public:
    struct Task
    {
        explicit Task(const ColumnFilePtr & column_file_)
            : column_file(column_file_)
        {}

        ColumnFilePtr column_file;

        Block block_data;
        PageId data_page = 0;

        bool sorted = false;
        size_t rows_offset = 0;
        size_t deletes_offset = 0;
    };
    using Tasks = std::vector<Task>;

private:
    Tasks tasks;
    ColumnFilePersisteds results;
    DMContext & context;
    MemTableSetPtr mem_table_set;
    size_t flush_version;

public:
    ColumnFileFlushTask(DMContext & context_, const MemTableSetPtr & mem_table_set_, size_t flush_version_);

    DeltaIndex::Updates prepare(WriteBatches & wbs);

    bool commit(ColumnFilePersistedSetPtr & persisted_file_set, WriteBatches & wbs);
};
} // namespace DM
} // namespace DB
