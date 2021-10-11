#include <Columns/IColumn.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{
String IColumn::dumpStructure() const
{
    WriteBufferFromOwnString res;
    res << getFamilyName() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr & sub_column) {
        res << ", " << sub_column->dumpStructure();
    };

    const_cast<IColumn *>(this)->forEachSubcolumn(callback);

    res << ")";
    return res.str();
}

} // namespace DB
