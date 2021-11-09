#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <common/types.h>

#include <unordered_set>

struct TableScanStatistics
{
    const String & executor_id;

    size_t inbound_rows = 0;
    size_t inbound_blocks = 0;
    size_t inbound_bytes = 0;

    size_t outbound_rows = 0;
    size_t outbound_blocks = 0;
    size_t outbound_bytes = 0;

    size_t hash_table_bytes = 0;

    explicit TableScanStatistics(const String & executor_id_): executor_id(executor_id_)
    {}

    static bool isHit(const String & executor_id)
    {
        return startsWith(executor_id, "TableFullScan_");
    }

    static void buildStatistics(TableScanStatistics & statistics, const ProfileStreamsInfo & profile_streams_info, DAGContext & dag_context)
    {
    }
};