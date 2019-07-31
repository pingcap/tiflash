
#include <Parsers/ASTSelectQuery.h>
#include <Interpreters/DAGQueryInfo.h>
#include <Interpreters/InterpreterDAGRequest.h>


namespace DB
{

    DAGQueryInfo::DAGQueryInfo(const tipb::DAGRequest & dag_request_, CoprocessorContext & coprocessorContext_)
    : dag_request(dag_request_), coprocessorContext(coprocessorContext_) {}

    std::tuple<std::string, ASTPtr> DAGQueryInfo::parse(size_t ) {
        query = String("cop query");
        ast = std::make_shared<ASTSelectQuery>();
        ((ASTSelectQuery*)ast.get())->is_fake_sel = true;
        return std::make_tuple(query, ast);
    }

    String DAGQueryInfo::get_query_ignore_error(size_t ) {
        return query;
    }

    std::unique_ptr<IInterpreter> DAGQueryInfo::getInterpreter(Context & , QueryProcessingStage::Enum ) {
        return std::make_unique<InterpreterDAGRequest>(coprocessorContext, dag_request);
    }
}
