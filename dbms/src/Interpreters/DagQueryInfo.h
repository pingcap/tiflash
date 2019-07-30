#pragma once

#include <Interpreters/IQueryInfo.h>
#include <tipb/select.pb.h>
#include <Parsers/IAST.h>
#include <Core/QueryProcessingStage.h>
#include <Coprocessor/CoprocessorHandler.h>


namespace DB
{

/** IQueryInfo interface for different source of queries.
  */
class DagQueryInfo : public IQueryInfo
{
public:

    DagQueryInfo(const tipb::DAGRequest & dag_request, CoprocessorContext & coprocessorContext_);
    bool isInternalQuery() { return false;};
    virtual std::tuple<std::string, ASTPtr> parse(size_t max_query_size);
    virtual String get_query_ignore_error(size_t max_query_size);
    virtual std::unique_ptr<IInterpreter> getInterpreter(Context & context, QueryProcessingStage::Enum stage);

private:
    const tipb::DAGRequest & dag_request;
    CoprocessorContext & coprocessorContext;
    String query;
    ASTPtr ast;
};

}
