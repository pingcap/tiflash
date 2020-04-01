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
        DeleteRows,
        MergeDelta,
    };

    inline const char * toString(UInt64 op)
    {
        static const char * data[] = {"Flush", "Status", "Check", "Delete Rows", "Merge Delta"};
        return op < 5 ? data[op] : "Unknown operation";
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

    size_t rows = 0;

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
        if(operation == ManageOperation::Enum::DeleteRows)
            settings.ostr << " " << rows;
    }
};
}
