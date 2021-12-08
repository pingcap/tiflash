#pragma once

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
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
    explicit DAGQuerySource(Context & context_);

    std::tuple<std::string, ASTPtr> parse(size_t) override;
    String str(size_t max_query_size) override;
    std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) override;

    const std::vector<tipb::FieldType> & getResultFieldTypes() const { return result_field_types; }

    ASTPtr getAST() const { return ast; };

    tipb::EncodeType getEncodeType() const { return encode_type; }

    std::shared_ptr<DAGQueryBlock> getRootQueryBlock() const { return root_query_block; }

    DAGContext & getDAGContext() const { return *context.getDAGContext(); }

    std::string getExecutorNames() const;

private:
    void analyzeDAGEncodeType();

    Context & context;
    std::vector<tipb::FieldType> result_field_types;
    tipb::EncodeType encode_type;
    std::shared_ptr<DAGQueryBlock> root_query_block;
    ASTPtr ast;

    LogWithPrefixPtr log;
};

} // namespace DB
