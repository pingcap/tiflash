#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/Checksum/Checksum.h>

namespace DB
{
namespace DM
{

struct ColumnStat
{
    ColId       col_id;
    DataTypePtr type;
    // The average size of values. A hint for speeding up deserialize.
    double avg_size;
    // The serialized size of the column data on disk.
    size_t serialized_bytes = 0;
};

using ColumnStats = std::unordered_map<ColId, ColumnStat>;

inline void readText(ColumnStats & column_sats, DMFileFormat::Version ver, ReadBuffer & buf, UnifiedDigestBase * digest = nullptr)
{
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    size_t count;
    DB::assertString("Columns: ", buf);
    DB::readText(count, buf);
    if (digest)
    {
        digest->update(count);
    }
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
        if (ver >= DMFileFormat::V1)
        {
            DB::assertChar(' ', buf);
            DB::readText(serialized_bytes, buf);
        }
        DB::assertChar(' ', buf);
        DB::readString(type_name, buf);
        DB::assertChar('\n', buf);

        auto type = data_type_factory.get(type_name);
        column_sats.emplace(id, ColumnStat{id, type, avg_size, serialized_bytes});
        if (digest)
        {
            digest->update(avg_size);
            if (ver >= DMFileFormat::V1)
            {
                digest->update(serialized_bytes);
            }
            digest->update(type_name.data(), type_name.length());
        }
    }
}

inline void writeText(const ColumnStats & column_sats, DMFileFormat::Version ver, WriteBuffer & buf, UnifiedDigestBase * digest = nullptr)
{
    auto size = column_sats.size();
    DB::writeString("Columns: ", buf);
    DB::writeText(size, buf);
    if (digest)
    {
        digest->update(size);
    }
    DB::writeString("\n\n", buf);

    for (auto & [id, stat] : column_sats)
    {
        DB::writeText(id, buf);
        DB::writeChar(' ', buf);
        DB::writeText(stat.avg_size, buf);
        if (ver >= DMFileFormat::V1)
        {
            DB::writeChar(' ', buf);
            DB::writeText(stat.serialized_bytes, buf);
        }
        DB::writeChar(' ', buf);
        // Note that name of DataType may contains ' '
        auto name = stat.type->getName();
        DB::writeString(name, buf);
        DB::writeChar('\n', buf);
        if (digest)
        {
            digest->update(stat.avg_size);
            if (ver >= DMFileFormat::V1)
            {
                digest->update(stat.serialized_bytes);
            }
            digest->update(name.data(), name.length());
        }
    }
}

} // namespace DM
} // namespace DB
