#pragma once

#include <Parsers/IAST.h>


namespace DB
{


/** DELETE query
  */
class ASTDeleteQuery : public IAST
{
public:
    String database;
    String table;
    ASTPtr partition_expression_list;
    ASTPtr where;

    // Just for execute.
    ASTPtr select;

    /** Get the text that identifies this element. */
    String getID() const override { return "DeleteQuery_" + database + "_" + table; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTDeleteQuery>(*this);
        res->children.clear();

        if (where)
        {
            res->where = where->clone();
            res->children.push_back(res->where);
        }

        return res;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
