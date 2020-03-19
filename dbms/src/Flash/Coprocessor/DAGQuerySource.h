#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/IQuerySource.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>

#include "DAGDriver.h"
#include "DAGQueryBlock.h"

namespace DB
{

class Context;
class TiFlashMetrics;
using TiFlashMetricsPtr = std::shared_ptr<TiFlashMetrics>;

struct StreamWriter
{
    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;
    std::mutex write_mutex;

    StreamWriter(::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_) : writer(writer_) {}

    void write(const ::coprocessor::BatchResponse & data)
    {
        std::lock_guard<std::mutex> lk(write_mutex);
        writer->Write(data);
    }
};

using StreamWriterPtr = std::shared_ptr<StreamWriter>;

/// Query source of a DAG request via gRPC.
/// This is also an IR of a DAG.
class DAGQuerySource : public IQuerySource
{
public:
    //static const String TS_NAME;
    //static const String SEL_NAME;
    //static const String AGG_NAME;
    //static const String TOPN_NAME;
    //static const String LIMIT_NAME;

    DAGQuerySource(Context & context_, DAGContext & dag_context_, const std::vector<RegionInfo> & regions,
        const tipb::DAGRequest & dag_request_, ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_ = nullptr);

    std::tuple<std::string, ASTPtr> parse(size_t max_query_size) override;
    String str(size_t max_query_size) override;
    std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) override;

    //DAGContext & getDAGContext() const { return dag_context; };

    //bool hasSelection() const { return sel_index != -1; };
    //bool hasAggregation() const { return agg_index != -1; };
    //bool hasTopN() const { return order_index != -1; };
    //bool hasLimit() const { return order_index == -1 && limit_index != -1; };

    //Int32 getTSIndex() const { return ts_index; };
    //Int32 getSelectionIndex() const { return sel_index; };
    //Int32 getAggregationIndex() const { return agg_index; };
    //Int32 getTopNIndex() const { return order_index; };
    //Int32 getLimitIndex() const { return limit_index; };

    //const tipb::TableScan & getTS() const
    //{
    //    assertValid(ts_index, TS_NAME);
    //    return dag_request.executors(ts_index).tbl_scan();
    //};
    //const tipb::Selection & getSelection() const
    //{
    //    assertValid(sel_index, SEL_NAME);
    //    return dag_request.executors(sel_index).selection();
    //};
    //const tipb::Aggregation & getAggregation() const
    //{
    //    assertValid(agg_index, AGG_NAME);
    //    return dag_request.executors(agg_index).aggregation();
    //};
    //const tipb::TopN & getTopN() const
    //{
    //    assertValid(order_index, TOPN_NAME);
    //    return dag_request.executors(order_index).topn();
    //};
    //const tipb::Limit & getLimit() const
    //{
    //    assertValid(limit_index, LIMIT_NAME);
    //    return dag_request.executors(limit_index).limit();
    //};
    const tipb::DAGRequest & getDAGRequest() const { return dag_request; };

    std::vector<tipb::FieldType> getResultFieldTypes() const { return result_field_types; };

    ASTPtr getAST() const { return ast; };

    tipb::EncodeType getEncodeType() const { return encode_type; }

    std::shared_ptr<DAGQueryBlock> getQueryBlock() const { return root_query_block; }
    const std::vector<RegionInfo> & getRegions() const { return regions; }

    StreamWriterPtr writer;

protected:
    //void assertValid(Int32 index, const String & name) const
    //{
    //    if (index < 0 || index > dag_request.executors_size())
    //    {
    //        throw Exception("Access invalid executor: " + name);
    //    }
    //}

    //void analyzeResultFieldTypes();
    void analyzeDAGEncodeType();

protected:
    Context & context;
    DAGContext & dag_context;

    const std::vector<RegionInfo> & regions;

    const tipb::DAGRequest & dag_request;

    TiFlashMetricsPtr metrics;

    //Int32 ts_index = -1;
    //Int32 sel_index = -1;
    //Int32 agg_index = -1;
    //Int32 order_index = -1;
    //Int32 limit_index = -1;

    std::vector<tipb::FieldType> result_field_types;
    tipb::EncodeType encode_type;
    std::shared_ptr<DAGQueryBlock> root_query_block;
    ASTPtr ast;
};

} // namespace DB
