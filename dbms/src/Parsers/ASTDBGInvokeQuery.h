#pragma once

#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <Parsers/IAST.h>


namespace DB
{


/** DBGInvoke query
  */
class ASTDBGInvokeQuery : public IAST
{
public:
    struct DBGFunc
    {
        String name;
        ASTs args;
    };

    DBGFunc func;

    /** Get the text that identifies this element. */
    String getID() const override { return "DBGInvoke"; };

    ASTPtr clone() const override { return std::make_shared<ASTDBGInvokeQuery>(*this); }

    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "DBGInvoke "
            << (settings.hilite ? hilite_none : "") << func.name << "(";

        for (auto it = func.args.begin(); it != func.args.end(); ++it)
        {
            if (it != func.args.begin())
            settings.ostr << ", ";
            settings.ostr << (*it)->getColumnName();
        }

        settings.ostr << ")";
    }
};

}
