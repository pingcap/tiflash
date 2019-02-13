#pragma once

#include <Interpreters/Context.h>

#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/StorageMergeTree.h>

namespace DB
{

std::pair<Field, Field> getRegionRangeField(const TiKVKey & start_key, const TiKVKey & end_key, TableID table_id);

/// Remove range from this partition.
/// Note that [begin, excluded_end) is not necessarily to locate in the range of this partition (or table).
void deleteRangeInPartition(const Context & context, StorageMergeTree * storage,
    UInt64 partition_id, const Field & begin, const Field & excluded_end);

/// Move data in [begin, excluded_end) from src_partition_id to dest_partition_id.
/// FIXME/TODO: currently this function is not atomic and need to fix.
void moveRangeBetweenPartitions(const Context & context, StorageMergeTree * storage,
    UInt64 src_partition_id, UInt64 dest_partition_id, const Field & begin, const Field & excluded_end);

}
