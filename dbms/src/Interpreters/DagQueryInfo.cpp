
#include <Parsers/ASTSelectQuery.h>
#include <Interpreters/DagQueryInfo.h>
#include <Interpreters/InterpreterDagRequest.h>


namespace DB
{

    DagQueryInfo::DagQueryInfo(const tipb::DAGRequest & dag_request_, CoprocessorContext & coprocessorContext_)
    : dag_request(dag_request_), coprocessorContext(coprocessorContext_) {}

    std::tuple<std::string, ASTPtr> DagQueryInfo::parse(size_t ) {
        query = String("cop query");
        ast = std::make_shared<ASTSelectQuery>();
        ((ASTSelectQuery*)ast.get())->is_fake_sel = true;
        return std::make_tuple(query, ast);
    }

    String DagQueryInfo::get_query_ignore_error(size_t ) {
        return query;
    }

    std::unique_ptr<IInterpreter> DagQueryInfo::getInterpreter(Context & , QueryProcessingStage::Enum ) {
        return std::make_unique<InterpreterDagRequest>(coprocessorContext, dag_request);
    }
}
