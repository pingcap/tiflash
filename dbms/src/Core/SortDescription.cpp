#include <Columns/Collator.h>
#include <Core/SortDescription.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Core/Block.h>

#include <sstream>


namespace DB
{
void dumpSortDescription(const SortDescription & description, const Block & header, WriteBuffer & out)
{
    bool first = true;

    for (const auto & desc : description)
    {
        if (!first)
            out << ", ";
        first = false;

        if (!desc.column_name.empty())
            out << desc.column_name;
        else
        {
            if (desc.column_number < header.columns())
                out << header.getByPosition(desc.column_number).name;
            else
                out << "?";

            out << " (pos " << desc.column_number << ")";
        }

        if (desc.direction > 0)
            out << " ASC";
        else
            out << " DESC";

    }
}

std::string SortColumnDescription::getID() const
{
    WriteBufferFromOwnString out;
    out << column_name << ", " << column_number << ", " << direction << ", " << nulls_direction;
    if (collator)
        out << ", collation locale: " << collator->getLocale();
    return out.str();
}

std::string dumpSortDescription(const SortDescription & description)
{
    WriteBufferFromOwnString wb;
    dumpSortDescription(description, Block{}, wb);
    return wb.str();
}

} // namespace DB
