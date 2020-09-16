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

class DAGResponseWriter
{
public:
    DAGResponseWriter(Int64 records_per_chunk_, tipb::EncodeType encode_type_, std::vector<tipb::FieldType> result_field_types_,
        DAGContext & dag_context_, bool collect_execute_summary_, bool return_executor_id_);
    void addExecuteSummaries(tipb::SelectResponse & response);
    virtual void write(const Block & block) = 0;
    virtual void finishWrite() = 0;
    virtual ~DAGResponseWriter() = default;

protected:
    Int64 records_per_chunk;
    tipb::EncodeType encode_type;
    std::vector<tipb::FieldType> result_field_types;
    DAGContext & dag_context;
    bool collect_execute_summary;
    bool return_executor_id;
    std::unordered_map<String, std::tuple<UInt64, UInt64, UInt64>> previous_execute_stats;
};

} // namespace DB
