#pragma once

#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/Page/PageDefines.h>
#include <common/logger_useful.h>

#include "Storages/DeltaMerge/ColumnFile/ColumnFile.h"
#include "Storages/DeltaMerge/Delta/ColumnStableFileSet.h"

namespace DB
{
namespace DM
{
class MemTableSet;
using MemTableSetPtr = std::shared_ptr<MemTableSet>;

class FlushColumnFileTask
{
    friend class MemTableSet;
    friend class ColumnStableFileSet;
public:
    struct Task
    {
        Task(const ColumnFilePtr & column_file_)
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
    ColumnStableFiles results;
    DMContext & context;
    MemTableSetPtr mem_table_set;
    size_t current_flush_version = 0;

    Poco::Logger * log;

public:
    FlushColumnFileTask(DMContext & context_, const MemTableSetPtr & mem_table_set_);

    void setCurrentFlushVersion(size_t flush_version) { current_flush_version = flush_version; }

    DeltaIndex::Updates prepare(WriteBatches & wbs);

    bool commit(ColumnStableFileSetPtr & stable_file_set, WriteBatches & wbs);
};

using FlushColumnFileTaskPtr = std::shared_ptr<FlushColumnFileTask>;
}
}
