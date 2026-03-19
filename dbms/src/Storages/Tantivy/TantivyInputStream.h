// Copyright 2025 PingCAP, Ltd.
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

#include <Common/Logger.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/ShardInfo.h>
#include <Flash/Coprocessor/TiCIScan.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/Schema/TiDBTypes.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <Common/typeid_cast.h>
#include <fcntl.h>
#include <fmt/os.h>
#include <tici-search-lib/src/lib.rs.h>

namespace DB::TS
{

// convert literal value from timezone specified in cop request to UTC in-place
inline UInt64 convertPackedU64WithTimezone(UInt64 from_time, const TimezoneInfo & timezone_info)
{
    static const auto & time_zone_utc = DateLUT::instance("UTC");
    UInt64 result_time = from_time;
    if (timezone_info.is_name_based)
        convertTimeZone(from_time, result_time, *timezone_info.timezone, time_zone_utc);
    else if (timezone_info.timezone_offset != 0)
        convertTimeZoneByOffset(from_time, result_time, false, timezone_info.timezone_offset);
    return result_time;
}

/// Holds vector query parameters extracted from TiCIVectorQueryInfo proto.
struct VectorQueryState
{
    Int64 col_id;
    Int32 distance_metric;
    UInt32 top_k;
    std::vector<float> query_vector;
    bool has_filter;
    ::Expr filter_expr; // converted from tipb filter_expr, empty if no filter
};

class TantivyInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "TantivyInputStream";

    static constexpr auto version_column_name = "column_-1024";

public:
    // FTS constructor
    TantivyInputStream(
        LoggerPtr log_,
        UInt32 keyspace_id_,
        Int64 table_id_,
        Int64 index_id_,
        ShardInfo query_shard_info_,
        NamesAndTypes return_columns_,
        UInt64 limit_,
        std::vector<Int64> sort_column_ids_,
        std::vector<bool> sort_column_asc_,
        UInt64 read_ts_,
        ::Expr match_expr_,
        bool is_count,
        std::shared_ptr<rust::Box<ShardsSnapshot>> shards_snapshot_)
        : log(log_)
        , keyspace_id(keyspace_id_)
        , table_id(table_id_)
        , index_id(index_id_)
        , query_shard_info(query_shard_info_)
        , return_columns(return_columns_)
        , limit(limit_)
        , sort_column_ids(sort_column_ids_)
        , sort_column_asc(sort_column_asc_)
        , read_ts(read_ts_)
        , query_mode(TiCIQueryMode::FTS)
        , match_expr(match_expr_)
        , is_count(is_count)
        , shards_snapshot(std::move(shards_snapshot_))
    {}

    // Vector constructor
    TantivyInputStream(
        LoggerPtr log_,
        UInt32 keyspace_id_,
        Int64 table_id_,
        Int64 index_id_,
        ShardInfo query_shard_info_,
        NamesAndTypes return_columns_,
        UInt64 read_ts_,
        VectorQueryState vector_state_,
        std::shared_ptr<rust::Box<ShardsSnapshot>> shards_snapshot_)
        : log(log_)
        , keyspace_id(keyspace_id_)
        , table_id(table_id_)
        , index_id(index_id_)
        , query_shard_info(query_shard_info_)
        , return_columns(return_columns_)
        , limit(vector_state_.top_k)
        , read_ts(read_ts_)
        , query_mode(TiCIQueryMode::Vector)
        , vector_state(std::move(vector_state_))
        , is_count(false)
        , shards_snapshot(std::move(shards_snapshot_))
    {}

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

    Block readImpl() override
    {
        if (done)
        {
            return {};
        }
        Block ret = (query_mode == TiCIQueryMode::Vector) ? readVector() : readFromS3(is_count);
        done = true;
        return ret;
    }

protected:
    Block readVector()
    {
        auto return_fields = getFields(return_columns);
        auto shard_info = query_shard_info;
        LOG_DEBUG(log, "vector shard info: {}", shard_info.toString());
        auto key_ranges = getKeyRanges(shard_info.key_ranges);

        rust::Vec<float> query_vec;
        for (auto v : vector_state.query_vector)
            query_vec.push_back(v);

        VectorSearchParam vsp{
            .limit = static_cast<size_t>(vector_state.top_k),
            .col_id = vector_state.col_id,
            .distance_metric = vector_state.distance_metric,
            .query_vector = std::move(query_vec),
            .has_filter = vector_state.has_filter,
        };

        RUNTIME_CHECK(shards_snapshot != nullptr);
        SearchResult search_result = search_vector(
            **shards_snapshot,
            {
                .keyspace_id = keyspace_id,
                .table_id = table_id,
                .index_id = index_id,
                .shard_id = shard_info.shard_id,
                .shard_epoch = shard_info.shard_epoch,
            },
            key_ranges,
            return_fields,
            vector_state.filter_expr,
            vsp,
            read_ts);

        return buildBlockFromResult(search_result);
    }

    Block readFromS3(bool is_count)
    {
        auto return_fields = getFields(return_columns);
        auto shard_info = query_shard_info;
        LOG_DEBUG(log, "shard info: {}", shard_info.toString());
        auto key_ranges = getKeyRanges(shard_info.key_ranges);

        rust::Vec<rust::String> tici_sort_column_names;
        for (const auto & sort_column_id : sort_column_ids)
        {
            tici_sort_column_names.push_back(rust::String("column_" + std::to_string(sort_column_id)));
        }
        rust::Vec<bool> tici_sort_column_asc;
        for (const auto & asc : sort_column_asc)
        {
            tici_sort_column_asc.push_back(asc);
        }

        SearchParam search_param{
            .limit = static_cast<size_t>(limit),
            .sort_field_names = std::move(tici_sort_column_names),
            .is_asc = std::move(tici_sort_column_asc),
        };
        if (is_count)
            return_fields = {};

        RUNTIME_CHECK(shards_snapshot != nullptr);
        SearchResult search_result = search(
            **shards_snapshot,
            {
                .keyspace_id = keyspace_id,
                .table_id = table_id,
                .index_id = index_id,
                .shard_id = shard_info.shard_id,
                .shard_epoch = shard_info.shard_epoch,
            },
            key_ranges,
            return_fields,
            match_expr,
            search_param,
            read_ts);

        Block res(return_columns);
        if (is_count)
        {
            RUNTIME_CHECK_MSG(return_columns.size() == 1, "count search should return one column");
            auto & column = res.getByPosition(0).column->assumeMutableRef();
            column.insert(Int64(search_result.count));
            return res;
        }

        return buildBlockFromResult(search_result);
    }

private:
    static bool isVectorFloat32Type(const DataTypePtr & type)
    {
        const auto * type_array = typeid_cast<const DataTypeArray *>(type.get());
        return type_array != nullptr && type_array->getNestedType()->isFloatingPoint()
            && type_array->getNestedType()->getSizeOfValueInMemory() == sizeof(Float32);
    }

    static Field decodeVectorFloat32Field(const String & raw_value, const String & column_name)
    {
        RUNTIME_CHECK_MSG(
            raw_value.size() >= sizeof(UInt32),
            "Malformed TiCI vector payload for column {}: payload is too short ({} bytes)",
            column_name,
            raw_value.size());
        const auto element_count = readLittleEndian<UInt32>(raw_value.data());
        const auto expected_size = sizeof(UInt32) + static_cast<size_t>(element_count) * sizeof(Float32);
        RUNTIME_CHECK_MSG(
            raw_value.size() == expected_size,
            "Malformed TiCI vector payload for column {}: expected {} bytes for {} elements, got {} bytes",
            column_name,
            expected_size,
            element_count,
            raw_value.size());

        size_t cursor = 0;
        auto field = DecodeVectorFloat32(cursor, raw_value);
        RUNTIME_CHECK_MSG(
            cursor == raw_value.size(),
            "Malformed TiCI vector payload for column {}: {} trailing bytes remain",
            column_name,
            raw_value.size() - cursor);
        return field;
    }

    Block buildBlockFromResult(SearchResult & search_result)
    {
        Block res(return_columns);
        auto documents = search_result.rows;
        if (documents.empty())
        {
            return res;
        }
        for (auto & name_and_type : return_columns)
        {
            const auto nested_type = removeNullable(name_and_type.type);
            int idx = -1;
            for (size_t j = 0; j < documents[0].fieldValues.size(); j++)
            {
                if (documents[0].fieldValues[j].field_name == name_and_type.name)
                {
                    idx = j;
                    break;
                }
            }
            if (idx == -1)
            {
                if (name_and_type.name == version_column_name)
                {
                    auto col = res.getByName(name_and_type.name).column->assumeMutable();
                    for (auto & doc : documents)
                    {
                        col->insert(Field(doc.version));
                    }
                    continue;
                }
                if (query_mode == TiCIQueryMode::Vector && isVectorFloat32Type(nested_type))
                {
                    RUNTIME_CHECK_MSG(
                        false,
                        "TiCI vector query did not materialize requested vector column {}",
                        name_and_type.name);
                }
                for (size_t j = 0; j < documents.size(); j++)
                {
                    // Insert default value for missing fields
                    res.getByName(name_and_type.name).column->assumeMutable()->insertDefault();
                }
                continue;
            }

            const auto * result_type = nested_type.get();
            auto col = res.getByName(name_and_type.name).column->assumeMutable();
            bool has_null = false;
            if (result_type->isStringOrFixedString())
            {
                for (auto & doc : documents)
                {
                    const auto & field_value = doc.fieldValues[idx];
                    if (field_value.is_null)
                    {
                        if (query_mode == TiCIQueryMode::Vector)
                        {
                            RUNTIME_CHECK_MSG(
                                false,
                                "TiCI vector query returned null payload for vector column {}",
                                name_and_type.name);
                        }
                        has_null = true;
                        col->insert(Field());
                    }
                    else
                    {
                        const auto & v = field_value.string_value;
                        col->insert(Field(String(v.begin(), v.end())));
                    }
                }
            }
            else if (result_type->isInteger())
            {
                for (auto & doc : documents)
                {
                    const auto & field_value = doc.fieldValues[idx];
                    if (field_value.is_null)
                    {
                        has_null = true;
                        col->insert(Field());
                    }
                    else
                    {
                        col->insert(Field(field_value.int_value));
                    }
                }
            }
            else if (result_type->isFloatingPoint())
            {
                for (auto & doc : documents)
                {
                    const auto & field_value = doc.fieldValues[idx];
                    if (field_value.is_null)
                    {
                        has_null = true;
                        col->insert(Field());
                    }
                    else
                    {
                        col->insert(Field(field_value.float_value));
                    }
                }
            }
            else if (isVectorFloat32Type(nested_type))
            {
                for (auto & doc : documents)
                {
                    const auto & field_value = doc.fieldValues[idx];
                    if (field_value.is_null)
                    {
                        has_null = true;
                        col->insert(Field());
                    }
                    else
                    {
                        const String raw_value(field_value.string_value.begin(), field_value.string_value.end());
                        col->insert(decodeVectorFloat32Field(raw_value, name_and_type.name));
                    }
                }
            }
            else if (result_type->isDateOrDateTime())
            {
                for (auto & doc : documents)
                {
                    const auto & field_value = doc.fieldValues[idx];
                    if (field_value.is_null)
                    {
                        has_null = true;
                        col->insert(Field());
                    }
                    else
                    {
                        auto t = static_cast<UInt64>(field_value.int_value);
                        col->insert(Field(t));
                    }
                }
            }
            else
            {
                RUNTIME_CHECK_MSG(
                    false,
                    "Unsupported TiCI result column type {} for column {}",
                    nested_type->getName(),
                    name_and_type.name);
            }
            if (has_null)
            {
                RUNTIME_CHECK_MSG(
                    col->isColumnNullable(),
                    "column {} is not nullable, but got null value from TiCI",
                    name_and_type.name);
            }
        }
        return res;
    }

    Block header;
    bool done = false;
    LoggerPtr log;
    UInt32 keyspace_id;
    Int64 table_id;
    Int64 index_id;
    ShardInfo query_shard_info;
    NamesAndTypes return_columns;
    UInt64 limit;
    std::vector<Int64> sort_column_ids;
    std::vector<bool> sort_column_asc;
    UInt64 read_ts;
    TiCIQueryMode query_mode;
    ::Expr match_expr; // FTS mode
    VectorQueryState vector_state; // Vector mode
    bool is_count;
    std::shared_ptr<rust::Box<ShardsSnapshot>> shards_snapshot;

    static rust::Vec<rust::String> getFields(NamesAndTypes & columns)
    {
        rust::Vec<rust::String> fields;
        for (auto & name_and_type : columns)
        {
            fields.push_back(name_and_type.name);
        }
        return fields;
    }

    static rust::Vec<::Range> getKeyRanges(ShardInfo::KeyRanges & key_ranges)
    {
        rust::Vec<::Range> res;
        for (const auto & range : key_ranges)
        {
            rust::Slice<::std::uint8_t const> start(
                reinterpret_cast<const unsigned char *>(range.start().c_str()),
                range.start().size());
            rust::Slice<::std::uint8_t const> end(
                reinterpret_cast<const unsigned char *>(range.end().c_str()),
                range.end().size());
            res.push_back({
                .start = std::move(start),
                .end = std::move(end),
            });
        }
        return res;
    }
};
} // namespace DB::TS
