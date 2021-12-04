#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGInterpreterBase.h>
#include <Storages/TableLockHolder.h>
#include <Storages/Transaction/TiDB.h>
#include <kvproto/coprocessor.pb.h>
#include <pingcap/coprocessor/Client.h>

namespace DB
{
class TableScanInterpreter : public DAGInterpreterBase
{
public:
    TableScanInterpreter(
        Context & context_,
        const DAGQueryBlock & query_block_,
        size_t max_streams_,
        bool keep_session_timezone_info_,
        const DAGQuerySource & dag_,
        const LogWithPrefixPtr & log_);

private:
    void executeImpl(DAGPipelinePtr & pipeline) override;

    void executeTS(const tipb::TableScan & ts, DAGPipeline & pipeline);
    void executeRemoteQuery(DAGPipeline & pipeline);
    void executeRemoteQueryImpl(
        DAGPipeline & pipeline,
        const std::vector<pingcap::coprocessor::KeyRange> & cop_key_ranges,
        ::tipb::DAGRequest & dag_req,
        const DAGSchema & schema);

    TableLockHolder table_drop_lock;
    std::vector<ExtraCastAfterTSMode> need_add_cast_column_flag_for_tablescan;
    BoolVec is_remote_table_scan;
};
} // namespace DB
