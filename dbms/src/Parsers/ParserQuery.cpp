#include <Parsers/ParserQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserDeleteQuery.h>
#include <Parsers/ParserDBGInvokeQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserUseQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ParserTruncateQuery.h>
#include <Parsers/ParserManageQuery.h>

namespace DB
{


bool ParserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserQueryWithOutput query_with_output_p;
    ParserInsertQuery insert_p(end);
    ParserUseQuery use_p;
    ParserSetQuery set_p;
    ParserDeleteQuery delete_p;
    ParserDBGInvokeQuery dbginvoke_p;
    ParserOptimizeQuery optimize_p;
    ParserSystemQuery system_p;
    ParserTruncateQuery truncate_p;
    ParserManageQuery manage_p;

    bool res = query_with_output_p.parse(pos, node, expected)
        || insert_p.parse(pos, node, expected)
        || use_p.parse(pos, node, expected)
        || set_p.parse(pos, node, expected)
        || delete_p.parse(pos, node, expected)
        || dbginvoke_p.parse(pos, node, expected)
        || optimize_p.parse(pos, node, expected)
        || system_p.parse(pos, node, expected)
        || truncate_p.parse(pos, node, expected)
        || manage_p.parse(pos, node, expected);

    return res;
}

}
