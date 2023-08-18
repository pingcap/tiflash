// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{
namespace DM
{
struct ColumnStat
{
    ColId col_id;
    DataTypePtr type;
    // The average size of values. A hint for speeding up deserialize.
    double avg_size;
    // The serialized size of the column data on disk.
    size_t serialized_bytes = 0;

    // These members are only useful when using metav2
    size_t data_bytes = 0;
    size_t mark_bytes = 0;
    size_t nullmap_data_bytes = 0;
    size_t nullmap_mark_bytes = 0;
    size_t index_bytes = 0;
    void serializeToBuffer(WriteBuffer & buf) const
    {
        writeIntBinary(col_id, buf);
        writeStringBinary(type->getName(), buf);
        writeFloatBinary(avg_size, buf);
        writeIntBinary(serialized_bytes, buf);
        writeIntBinary(data_bytes, buf);
        writeIntBinary(mark_bytes, buf);
        writeIntBinary(nullmap_data_bytes, buf);
        writeIntBinary(nullmap_mark_bytes, buf);
        writeIntBinary(index_bytes, buf);
    }

    void parseFromBuffer(ReadBuffer & buf)
    {
        readIntBinary(col_id, buf);
        String type_name;
        readStringBinary(type_name, buf);
        type = DataTypeFactory::instance().getOrSet(type_name);
        readFloatBinary(avg_size, buf);
        readIntBinary(serialized_bytes, buf);
        readIntBinary(data_bytes, buf);
        readIntBinary(mark_bytes, buf);
        readIntBinary(nullmap_data_bytes, buf);
        readIntBinary(nullmap_mark_bytes, buf);
        readIntBinary(index_bytes, buf);
    }
};

using ColumnStats = std::unordered_map<ColId, ColumnStat>;

inline void readText(ColumnStats & column_sats, DMFileFormat::Version ver, ReadBuffer & buf)
{
    DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    size_t count;
    DB::assertString("Columns: ", buf);
    DB::readText(count, buf);
    DB::assertString("\n\n", buf);

    ColId id = 0;
    String type_name;
    double avg_size = 0.0;
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

        auto type = data_type_factory.getOrSet(type_name);
        column_sats.emplace(id, ColumnStat{id, type, avg_size, serialized_bytes});
    }
}

inline void writeText(const ColumnStats & column_sats, DMFileFormat::Version ver, WriteBuffer & buf)
{
    DB::writeString("Columns: ", buf);
    DB::writeText(column_sats.size(), buf);
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
        DB::writeString(stat.type->getName(), buf);
        DB::writeChar('\n', buf);
    }
}

} // namespace DM
} // namespace DB
