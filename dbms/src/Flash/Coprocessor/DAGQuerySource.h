#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
#include <Flash/Mpp/MPPHandler.h>
#include <Interpreters/Context.h>
#include <Interpreters/IQuerySource.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>


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

    void write(tipb::SelectResponse & response, [[maybe_unused]] uint16_t id = 0)
    {
        std::string dag_data;
        response.SerializeToString(&dag_data);
        ::coprocessor::BatchResponse resp;
        resp.set_data(dag_data);
        std::lock_guard<std::mutex> lk(write_mutex);
        writer->Write(resp);
    }
    // a helper function
    uint16_t getPartitionNum() { return 0; }
};

using StreamWriterPtr = std::shared_ptr<StreamWriter>;

/// Query source of a DAG request via gRPC.
/// This is also an IR of a DAG.
class DAGQuerySource : public IQuerySource
{
public:
    DAGQuerySource(Context & context_, const std::unordered_map<RegionID, RegionInfo> & regions_, const tipb::DAGRequest & dag_request_,
        const bool is_batch_cop_ = false);

    std::tuple<std::string, ASTPtr> parse(size_t max_query_size) override;
    String str(size_t max_query_size) override;
    std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) override;

    const tipb::DAGRequest & getDAGRequest() const { return dag_request; };

    std::vector<tipb::FieldType> getResultFieldTypes() const { return result_field_types; };

    ASTPtr getAST() const { return ast; };

    tipb::EncodeType getEncodeType() const { return encode_type; }

    std::shared_ptr<DAGQueryBlock> getQueryBlock() const { return root_query_block; }
    const std::unordered_map<RegionID, RegionInfo> & getRegions() const { return regions; }

    bool isBatchCop() const { return is_batch_cop; }

    DAGContext & getDAGContext() const { return *context.getDAGContext(); }

protected:
    void analyzeDAGEncodeType();

protected:
    Context & context;

    const std::unordered_map<RegionID, RegionInfo> & regions;

    const tipb::DAGRequest & dag_request;

    std::vector<tipb::FieldType> result_field_types;
    tipb::EncodeType encode_type;
    std::shared_ptr<DAGQueryBlock> root_query_block;
    ASTPtr ast;

    const bool is_batch_cop;
};

} // namespace DB
