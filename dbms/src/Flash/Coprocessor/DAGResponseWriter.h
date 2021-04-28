#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

/// do not need be thread safe since it is only used in single thread env
struct ExecutionSummary
{
    UInt64 time_processed_ns;
    UInt64 num_produced_rows;
    UInt64 num_iterations;
    UInt64 concurrency;
    ExecutionSummary() : time_processed_ns(0), num_produced_rows(0), num_iterations(0), concurrency(0) {}

    void merge(const ExecutionSummary & other, bool streaming_call)
    {
        if (streaming_call)
        {
            time_processed_ns = std::max(time_processed_ns, other.time_processed_ns);
            num_produced_rows = std::max(num_produced_rows, other.num_produced_rows);
            num_iterations = std::max(num_iterations, other.num_iterations);
            concurrency = std::max(concurrency, other.concurrency);
        }
        else
        {
            time_processed_ns = std::max(time_processed_ns, other.time_processed_ns);
            num_produced_rows += other.num_produced_rows;
            num_iterations += other.num_iterations;
            concurrency += other.concurrency;
        }
    }
};

class DAGResponseWriter
{
public:
    DAGResponseWriter(Int64 records_per_chunk_, tipb::EncodeType encode_type_, std::vector<tipb::FieldType> result_field_types_,
        DAGContext & dag_context_);
    void fillTiExecutionSummary(
        tipb::ExecutorExecutionSummary * execution_summary, ExecutionSummary & current, const String & executor_id, bool delta_mode);
    void addExecuteSummaries(tipb::SelectResponse & response, bool delta_mode);
    virtual void write(const Block & block) = 0;
    virtual void finishWrite() = 0;
    virtual ~DAGResponseWriter() = default;

protected:
    Int64 records_per_chunk;
    tipb::EncodeType encode_type;
    std::vector<tipb::FieldType> result_field_types;
    DAGContext & dag_context;
    std::unordered_map<String, ExecutionSummary> previous_execution_stats;
    std::unordered_set<String> local_executors;
};

} // namespace DB
