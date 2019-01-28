#include <iomanip>
#include <Parsers/ASTDeleteQuery.h>


namespace DB
{

void ASTDeleteQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    settings.ostr
        << (settings.hilite ? hilite_keyword : "")
        << "DELETE FROM "
        << (settings.hilite ? hilite_none : "")
        << (!database.empty() ? backQuoteIfNeed(database) + "." : "")
        << backQuoteIfNeed(table);

    if (where)
    {
        settings.ostr << " WHERE ";
        where->formatImpl(settings, state, frame);
    }
}

}
