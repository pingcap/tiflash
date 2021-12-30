#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowProcesslistQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
BlockIO InterpreterShowProcesslistQuery::execute()
{
    return executeQuery("SELECT * FROM system.processes", context, true);
}

} // namespace DB
