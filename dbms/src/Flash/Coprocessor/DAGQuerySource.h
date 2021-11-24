#pragma once

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
#include <Flash/Coprocessor/RegionInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/IQuerySource.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>
#include <tipb/select.pb.h>


namespace DB
{
/// Query source of a DAG request via gRPC.
/// This is also an IR of a DAG.
class DAGQuerySource : public IQuerySource
{
public:
    DAGQuerySource(
        Context & context_,
        const RegionInfoMap & regions_,
        const RegionInfoList & regions_for_remote_read_,
        const tipb::DAGRequest & dag_request_,
        const LogWithPrefixPtr & log_,
        const bool is_batch_cop_or_mpp_ = false);

    std::tuple<std::string, ASTPtr> parse(size_t) override;
    String str(size_t max_query_size) override;
    std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) override;

    const tipb::DAGRequest & getDAGRequest() const { return dag_request; };

    const std::vector<tipb::FieldType> & getResultFieldTypes() const { return result_field_types; }

    ASTPtr getAST() const { return ast; };

    tipb::EncodeType getEncodeType() const { return encode_type; }

    std::shared_ptr<DAGQueryBlock> getRootQueryBlock() const { return root_query_block; }
    const RegionInfoMap & getRegions() const { return regions; }
    const RegionInfoList & getRegionsForRemoteRead() const { return regions_for_remote_read; }

    bool isBatchCopOrMpp() const { return is_batch_cop_or_mpp; }

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

    const bool is_batch_cop_or_mpp;

    LogWithPrefixPtr log;
};

} // namespace DB
