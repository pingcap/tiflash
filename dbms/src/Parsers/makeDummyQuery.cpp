#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/makeDummyQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

ASTPtr makeDummyQuery()
{
    static const String tmp = "select 1";
    ParserQuery parser(tmp.data() + tmp.size());
    ASTPtr parent = parseQuery(parser, tmp.data(), tmp.data() + tmp.size(), "", 0);
    return ((ASTSelectWithUnionQuery *)parent.get())->list_of_selects->children.at(0);
}

} // namespace DB

