#include <Parsers/ASTDBGInvokeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDBGInvokeQuery.h>
#include <Common/typeid_cast.h>
#include <Debug/DBGInvoker.h>

namespace DB
{

BlockIO InterpreterDBGInvokeQuery::execute()
{
    const ASTDBGInvokeQuery & ast = typeid_cast<const ASTDBGInvokeQuery &>(*query_ptr);
    BlockIO res;
    res.in = context.getDBGInvoker().invoke(context, ast.func.name, ast.func.args);
    return res;
}

}
