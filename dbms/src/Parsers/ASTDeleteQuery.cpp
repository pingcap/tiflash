#include <iomanip>
#include <Parsers/ASTDeleteQuery.h>


namespace DB
{

void ASTDeleteQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    settings.ostr
        << (settings.hilite ? hilite_keyword : "")
        << "DELETE FROM "
        << (settings.hilite ? hilite_none : "")
        << (!database.empty() ? backQuoteIfNeed(database) + "." : "")
        << backQuoteIfNeed(table);

    if (partition_expression_list)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws <<
            indent_str << "PARTITION " << (settings.hilite ? hilite_none : "");
        partition_expression_list->formatImpl(settings, state, frame);
    }

    if (where)
    {
        settings.ostr << " WHERE ";
        where->formatImpl(settings, state, frame);
    }
}

}
