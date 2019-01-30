#pragma once

#include <Parsers/IAST.h>


namespace DB
{
namespace ManageOperation
{
    enum Enum
    {
        Flush,
        Status,
        Check,
    };

    inline const char * toString(UInt64 op)
    {
        static const char * data[] = {"Flush", "Status", "Check"};
        return op < 3 ? data[op] : "Unknown operation";
    }
}

/** Manage query
  */
class ASTManageQuery : public IAST
{
public:
    String database;
    String table;

    ManageOperation::Enum operation;

    /** Get the text that identifies this element. */
    String getID() const override
    {
        return "ManageQuery_" + database + "_" + table + "_" + ManageOperation::toString(operation);
    };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTManageQuery>(*this);
        res->children.clear();
        return res;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MANAGE TABLE " << (settings.hilite ? hilite_none : "")
                      << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table) << " "
                      << (settings.hilite ? hilite_keyword : "") << ManageOperation::toString(operation)
                      << (settings.hilite ? hilite_none : "");
    }
};
}
