#pragma once

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQueryBlock.h>
#include <Interpreters/Context.h>
#include <Interpreters/IQuerySource.h>

namespace DB
{
/// DAGQuerySource is an adaptor between DAG and CH's executeQuery.
/// TODO: consider to directly use DAGContext instead.
class DAGQuerySource : public IQuerySource
{
public:
    explicit DAGQuerySource(Context & context_);

    std::tuple<std::string, ASTPtr> parse(size_t) override;
    String str(size_t max_query_size) override;
    std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) override;

    std::shared_ptr<DAGQueryBlock> getRootQueryBlock() const { return root_query_block; }

    DAGContext & getDAGContext() const { return *context.getDAGContext(); }

private:
    Context & context;
    std::shared_ptr<DAGQueryBlock> root_query_block;
};

} // namespace DB
