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
#include <Storages/DeltaMerge/dtpb/dmfile.pb.h>
#include <Storages/KVStore/Types.h>

namespace DB::DM
{
struct ColumnStat
{
    ColId col_id;
    DataTypePtr type;
    // The average size of values. A hint for speeding up deserialize.
    double avg_size;
    // The serialized size of the column data on disk. (including column data and nullmap)
    size_t serialized_bytes = 0;

    // These members are only useful when using metav2
    size_t data_bytes = 0;
    size_t mark_bytes = 0;
    size_t nullmap_data_bytes = 0;
    size_t nullmap_mark_bytes = 0;
    size_t index_bytes = 0;
    size_t sizes_bytes = 0; // Array sizes or String sizes, depends on the data type of this column
    size_t sizes_mark_bytes = 0;

    std::vector<dtpb::VectorIndexFileProps> vector_index;

#ifndef NDEBUG
    // This field is only used for testing
    String additional_data_for_test{};
#endif

    dtpb::ColumnStat toProto() const
    {
        dtpb::ColumnStat stat;
        stat.set_col_id(col_id);
        stat.set_type_name(type->getName());
        stat.set_avg_size(avg_size);
        stat.set_serialized_bytes(serialized_bytes);
        stat.set_data_bytes(data_bytes);
        stat.set_mark_bytes(mark_bytes);
        stat.set_nullmap_data_bytes(nullmap_data_bytes);
        stat.set_nullmap_mark_bytes(nullmap_mark_bytes);
        stat.set_index_bytes(index_bytes);
        stat.set_sizes_bytes(sizes_bytes);
        stat.set_sizes_mark_bytes(sizes_mark_bytes);

        for (const auto & vec_idx : vector_index)
        {
            auto * pb_idx = stat.add_vector_indexes();
            pb_idx->CopyFrom(vec_idx);
        }

#ifndef NDEBUG
        stat.set_additional_data_for_test(additional_data_for_test);
#endif

        return stat;
    }

    void mergeFromProto(const dtpb::ColumnStat & proto)
    {
        col_id = proto.col_id();
        type = DataTypeFactory::instance().getOrSet(proto.type_name());
        avg_size = proto.avg_size();
        serialized_bytes = proto.serialized_bytes();
        data_bytes = proto.data_bytes();
        mark_bytes = proto.mark_bytes();
        nullmap_data_bytes = proto.nullmap_data_bytes();
        nullmap_mark_bytes = proto.nullmap_mark_bytes();
        index_bytes = proto.index_bytes();
        sizes_bytes = proto.sizes_bytes();
        sizes_mark_bytes = proto.sizes_mark_bytes();

        if (proto.has_vector_index())
        {
            // For backward compatibility, loaded `vector_index` into `vector_indexes`
            // with index_id == EmptyIndexID
            vector_index.emplace_back(proto.vector_index());
            auto & idx = vector_index.back();
            idx.set_index_id(EmptyIndexID);
            idx.set_index_bytes(index_bytes);
        }
        vector_index.reserve(vector_index.size() + proto.vector_indexes_size());
        for (const auto & pb_idx : proto.vector_indexes())
        {
            vector_index.emplace_back(pb_idx);
        }

#ifndef NDEBUG
        additional_data_for_test = proto.additional_data_for_test();
#endif
    }

    // New fields should be added via protobuf. Use `toProto` instead
    [[deprecated("Use ColumnStat::toProto instead")]] //
    void
    serializeToBuffer(WriteBuffer & buf) const
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

    // This only presents for reading with old data. Use `mergeFromProto` instead
    [[deprecated("Use ColumnStat::mergeFromProto instead")]] //
    void
    parseFromBuffer(ReadBuffer & buf)
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

[[deprecated("Used by DMFileMeta v1. Use ColumnStat::mergeFromProto instead")]] //
inline void
readText(ColumnStats & column_sats, DMFileFormat::Version ver, ReadBuffer & buf)
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
        column_sats.emplace(
            id,
            ColumnStat{
                .col_id = id,
                .type = type,
                .avg_size = avg_size,
                .serialized_bytes = serialized_bytes,
                // ... here ignore some fields with default initializers
                .vector_index = {},
#ifndef NDEBUG
                .additional_data_for_test = {},
#endif
            });
    }
}

[[deprecated("Used by DMFileMeta v1. Use ColumnStat::toProto instead")]] //
inline void
writeText(const ColumnStats & column_sats, DMFileFormat::Version ver, WriteBuffer & buf)
{
    DB::writeString("Columns: ", buf);
    DB::writeText(column_sats.size(), buf);
    DB::writeString("\n\n", buf);

    for (const auto & [id, stat] : column_sats)
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

} // namespace DB::DM
