#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFileDefines.h>

namespace DB
{
namespace DM
{

struct ColumnStat
{
    ColId       col_id;
    DataTypePtr type;
    // A hint size for speeding up deserialize.
    double avg_size;
    // Serialize size in disk.
    size_t serialized_bytes = 0;
};

using ColumnStats = std::unordered_map<ColId, ColumnStat>;

inline void readText(ColumnStats & column_sats, DMFileVersion ver, ReadBuffer & buf)
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    size_t count;
    DB::assertString("Columns: ", buf);
    DB::readText(count, buf);
    DB::assertString("\n\n", buf);

    ColId  id = 0;
    String type_name;
    double avg_size         = 0.0;
    size_t serialized_bytes = 0;
    for (size_t i = 0; i < count; ++i)
    {
        DB::readText(id, buf);
        DB::assertChar(' ', buf);
        DB::readText(avg_size, buf);
        DB::assertChar(' ', buf);
        DB::readString(type_name, buf);
        if (ver >= DMFileVersion::VERSION_WITH_COLUMN_SIZE)
        {
            DB::assertChar(' ', buf);
            DB::readText(serialized_bytes, buf);
        }
        DB::assertChar('\n', buf);

        auto type = data_type_factory.get(type_name);
        column_sats.emplace(id, ColumnStat{id, type, avg_size, serialized_bytes});
    }
}

inline void writeText(const ColumnStats & column_sats, DMFileVersion ver, WriteBuffer & buf)
{
    DB::writeString("Columns: ", buf);
    DB::writeText(column_sats.size(), buf);
    DB::writeString("\n\n", buf);

    for (auto & [id, stat] : column_sats)
    {
        DB::writeText(id, buf);
        DB::writeChar(' ', buf);
        DB::writeText(stat.avg_size, buf);
        DB::writeChar(' ', buf);
        DB::writeString(stat.type->getName(), buf);
        if (ver >= DMFileVersion::VERSION_WITH_COLUMN_SIZE)
        {
            DB::writeChar(' ', buf);
            DB::writeText(stat.serialized_bytes, buf);
        }
        DB::writeChar('\n', buf);
    }
}

} // namespace DM
} // namespace DB
