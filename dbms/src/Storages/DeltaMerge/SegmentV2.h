#pragma once

#include "ColumnStableFileSet.h"
#include "MemTableSet.h"

namespace DB
{
namespace DM
{
class SegmentV2
{
private:
    std::mutex mutex;

    const MemTableSetPtr mem_table_set;

    const ColumnStableFileSetPtr column_stable_files;

    DeltaIndexPtr delta_index;

    UInt64 flush_version = 0;

private:
    bool flush(DMContext & context);
};
}
}

