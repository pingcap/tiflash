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

struct StreamWriter
{
    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;
    std::mutex write_mutex;

    StreamWriter(::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_)
        : writer(writer_)
    {}

    void write(tipb::SelectResponse & response, [[maybe_unused]] uint16_t id = 0)
    {
        ::coprocessor::BatchResponse resp;
        if (!response.SerializeToString(resp.mutable_data()))
            throw Exception("Fail to serialize response, response size: " + std::to_string(response.ByteSizeLong()));
        std::lock_guard<std::mutex> lk(write_mutex);
        if (!writer->Write(resp))
            throw Exception("Failed to write resp");
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
    DAGQuerySource(Context & context_, const RegionInfoMap & regions_, const RegionInfoList & regions_needs_remote_read_, const tipb::DAGRequest & dag_request_, const std::shared_ptr<LogWithPrefix> & log_, const bool is_batch_cop_ = false);

    std::tuple<std::string, ASTPtr> parse(size_t) override;
    String str(size_t max_query_size) override;
    std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) override;

    const tipb::DAGRequest & getDAGRequest() const { return dag_request; };

    std::vector<tipb::FieldType> getResultFieldTypes() const { return result_field_types; };

    ASTPtr getAST() const { return ast; };

    tipb::EncodeType getEncodeType() const { return encode_type; }

    std::shared_ptr<DAGQueryBlock> getQueryBlock() const { return root_query_block; }
    const RegionInfoMap & getRegions() const { return regions; }
    const RegionInfoList & getRegionsForRemoteRead() const { return regions_for_remote_read; }

    bool isBatchCop() const { return is_batch_cop; }

    DAGContext & getDAGContext() const { return *context.getDAGContext(); }

    std::string getExecutorNames() const;

protected:
    void analyzeDAGEncodeType();

protected:
    Context & context;

    const RegionInfoMap & regions;
    const RegionInfoList & regions_for_remote_read;

    const tipb::DAGRequest & dag_request;

    std::vector<tipb::FieldType> result_field_types;
    tipb::EncodeType encode_type;
    std::shared_ptr<DAGQueryBlock> root_query_block;
    ASTPtr ast;

    const bool is_batch_cop;

    std::shared_ptr<LogWithPrefix> log;
};

} // namespace DB
