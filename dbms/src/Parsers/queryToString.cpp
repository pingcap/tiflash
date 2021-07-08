#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>
#include <sstream>

namespace DB
{
    String queryToString(const ASTPtr & query)
    {
        return queryToString(*query);
    }

    String queryToString(const IAST & query)
    {
        WriteBufferFromOwnString out;
        formatAST(query, out, false, true);
        return out.str();
    }
}
