#include <Core/ColumnsWithTypeAndName.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

ColumnWithTypeAndName ColumnWithTypeAndName::cloneEmpty() const
{
    ColumnWithTypeAndName res;

    res.name = name;
    res.type = type;
    res.column_id = column_id;
    if (column)
        res.column = column->cloneEmpty();

    return res;
}


bool ColumnWithTypeAndName::operator==(const ColumnWithTypeAndName & other) const
{
    // TODO should we check column_id here?
    return name == other.name
        && ((!type && !other.type) || (type && other.type && type->equals(*other.type)))
        && ((!column && !other.column) || (column && other.column && column->getName() == other.column->getName()));
}


void ColumnWithTypeAndName::dumpStructure(WriteBuffer & out) const
{
    out << name << ' ' << column_id;

    if (type)
        out << ' ' << type->getName();
    else
        out << " nullptr";

    if (column)
        out << ' ' << column->dumpStructure();
    else
        out << " nullptr";
}

String ColumnWithTypeAndName::dumpStructure() const
{
    WriteBufferFromOwnString out;
    dumpStructure(out);
    return out.str();
}

}
