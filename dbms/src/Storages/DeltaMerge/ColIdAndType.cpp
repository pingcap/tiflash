#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/ColIdAndType.h>


namespace DB
{
namespace DM
{

void readText(ColIdAndTypeSet & colid_and_types, ReadBuffer & buf);
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    size_t count;
    assertString("Columns: ", buf);
    DB::readText(count, buf);
    assertString("\n\n", buf);

    UInt64 id;
    String type_name;
    for (size_t i = 0; i < count; ++i)
    {
        DB::readText(id, buf);
        assertChar(' ', buf);
        readString(type_name, buf);
        auto type = data_type_factory.get(type_name);
        colid_and_types.emplace(id, type);
        assertChar('\n', buf);
    }
}

void writeText(const ColIdAndTypeSet & colid_and_types, WriteBuffer & buf)
{
    writeString("Columns: ", buf);
    DB::writeText(colid_and_types.size(), buf);
    writeString("\n\n", buf);

    for (auto & c : colid_and_types)
    {
        DB::writeText(c.id, buf);
        writeChar(' ', buf);
        writeString(c.type->getName(), buf);
        writeChar('\n', buf);
    }
}

} // namespace DM
} // namespace DB