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

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Logger.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/ShardInfo.h>
#include <TiDB/Schema/TiDBTypes.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <fcntl.h>
#include <fmt/os.h>
#include <tici-search-lib/src/lib.rs.h>

#include <cstring>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>

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

class TantivyInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "TantivyInputStream";

public:
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
        , match_expr(match_expr_)
        , is_count(is_count)
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
        Block ret = readFromS3(is_count);
        done = true;
        return ret;
    }

protected:
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

        RUNTIME_CHECK_MSG(
            search_result.result_type == result_type_rows(),
            "expected row search result, got type {}",
            search_result.result_type);

        const auto row_count = static_cast<size_t>(search_result.row_count);
        if (row_count == 0)
            return res;

        auto name_to_pos = buildColumnPositionMap(return_columns);
        std::vector<bool> filled(return_columns.size(), false);

        for (const auto & column_data : search_result.i64_columns)
        {
            installIntegerLikeColumn(
                res,
                name_to_pos,
                filled,
                return_columns,
                column_data.col_name,
                column_data.values,
                column_data.null_map);
        }
        for (const auto & column_data : search_result.u64_columns)
        {
            installIntegerLikeColumn(
                res,
                name_to_pos,
                filled,
                return_columns,
                column_data.col_name,
                column_data.values,
                column_data.null_map);
        }
        for (const auto & column_data : search_result.f64_columns)
        {
            installFloatColumn(res, name_to_pos, filled, return_columns, column_data);
        }
        for (const auto & column_data : search_result.bytes_columns)
        {
            installBytesColumn(res, name_to_pos, filled, return_columns, column_data);
        }

        for (size_t i = 0; i < return_columns.size(); ++i)
        {
            if (!filled[i])
                fillDefaultColumn(res.getByPosition(i), row_count);
        }

        return res;
    }

private:
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
    ::Expr match_expr;
    bool is_count;
    std::shared_ptr<rust::Box<ShardsSnapshot>> shards_snapshot;

    static std::unordered_map<String, size_t> buildColumnPositionMap(const NamesAndTypes & columns)
    {
        std::unordered_map<String, size_t> positions;
        positions.reserve(columns.size());
        for (size_t i = 0; i < columns.size(); ++i)
            positions.emplace(columns[i].name, i);
        return positions;
    }

    static ColumnUInt8::MutablePtr buildNullMapColumn(size_t row_count, const rust::Vec<::std::uint8_t> & null_map)
    {
        auto null_map_column = ColumnUInt8::create(row_count, 0);
        if (null_map.size() == 0)
            return null_map_column;

        RUNTIME_CHECK_MSG(
            null_map.size() == row_count,
            "null map size mismatch, expect {}, got {}",
            row_count,
            null_map.size());
        auto & dst = null_map_column->getData();
        for (size_t i = 0; i < row_count; ++i)
            dst[i] = null_map[i];
        return null_map_column;
    }

    static void fillDefaultColumn(ColumnWithTypeAndName & column, size_t row_count)
    {
        auto mutable_column = column.type->createColumn();
        for (size_t i = 0; i < row_count; ++i)
            mutable_column->insertDefault();
        column.column = std::move(mutable_column);
    }

    template <typename TargetType, typename SourceType>
    static MutableColumnPtr buildNumericColumn(
        const NameAndTypePair & name_and_type,
        const rust::Vec<SourceType> & values,
        const rust::Vec<::std::uint8_t> & null_map)
    {
        const auto row_count = values.size();
        auto nested_column = ColumnVector<TargetType>::create(row_count);
        auto & data = nested_column->getData();
        const bool has_null_map = null_map.size() != 0;

        if (has_null_map)
        {
            RUNTIME_CHECK_MSG(
                null_map.size() == row_count,
                "null map size mismatch for column {}, expect {}, got {}",
                name_and_type.name,
                row_count,
                null_map.size());
        }

        if constexpr (std::is_same_v<TargetType, SourceType>)
        {
            if (row_count != 0)
                std::memcpy(data.data(), values.data(), row_count * sizeof(TargetType));
        }
        else
        {
            for (size_t i = 0; i < row_count; ++i)
                data[i] = static_cast<TargetType>(values[i]);
        }

        if (name_and_type.type->isNullable())
            return ColumnNullable::create(std::move(nested_column), buildNullMapColumn(row_count, null_map));
        return nested_column;
    }

    template <typename SourceType>
    static MutableColumnPtr buildIntegerLikeColumn(
        const NameAndTypePair & name_and_type,
        const rust::Vec<SourceType> & values,
        const rust::Vec<::std::uint8_t> & null_map)
    {
        const auto nested_type = removeNullable(name_and_type.type);
        if (typeid_cast<const DataTypeInt8 *>(nested_type.get()))
            return buildNumericColumn<Int8>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeInt16 *>(nested_type.get()))
            return buildNumericColumn<Int16>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeInt32 *>(nested_type.get()))
            return buildNumericColumn<Int32>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeInt64 *>(nested_type.get()))
            return buildNumericColumn<Int64>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeUInt8 *>(nested_type.get()))
            return buildNumericColumn<UInt8>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeUInt16 *>(nested_type.get()))
            return buildNumericColumn<UInt16>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeUInt32 *>(nested_type.get()))
            return buildNumericColumn<UInt32>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeUInt64 *>(nested_type.get()))
            return buildNumericColumn<UInt64>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeDate *>(nested_type.get()))
            return buildNumericColumn<DataTypeDate::FieldType>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeDateTime *>(nested_type.get()))
            return buildNumericColumn<DataTypeDateTime::FieldType>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeMyDate *>(nested_type.get()))
            return buildNumericColumn<DataTypeMyDate::FieldType>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeMyDateTime *>(nested_type.get()))
            return buildNumericColumn<DataTypeMyDateTime::FieldType>(name_and_type, values, null_map);

        throw std::runtime_error(fmt::format(
            "unsupported integer-like target type {} for column {}",
            nested_type->getName(),
            name_and_type.name));
    }

    static MutableColumnPtr buildFloatColumn(
        const NameAndTypePair & name_and_type,
        const rust::Vec<double> & values,
        const rust::Vec<::std::uint8_t> & null_map)
    {
        const auto nested_type = removeNullable(name_and_type.type);
        if (typeid_cast<const DataTypeFloat32 *>(nested_type.get()))
            return buildNumericColumn<Float32>(name_and_type, values, null_map);
        if (typeid_cast<const DataTypeFloat64 *>(nested_type.get()))
            return buildNumericColumn<Float64>(name_and_type, values, null_map);

        throw std::runtime_error(
            fmt::format("unsupported float target type {} for column {}", nested_type->getName(), name_and_type.name));
    }

    static MutableColumnPtr buildBytesColumn(const NameAndTypePair & name_and_type, const BytesColumnData & column_data)
    {
        const auto row_count = column_data.offsets.size();
        if (column_data.offsets.size() != 0)
        {
            RUNTIME_CHECK_MSG(
                column_data.offsets[column_data.offsets.size() - 1] == column_data.chars.size(),
                "string offsets and chars size mismatch for column {}",
                name_and_type.name);
        }

        if (removeNullable(name_and_type.type)->isString())
        {
            auto nested_column = ColumnString::create();
            auto & chars = nested_column->getChars();
            auto & offsets = nested_column->getOffsets();

            chars.resize(column_data.chars.size());
            if (!column_data.chars.empty())
                std::memcpy(chars.data(), column_data.chars.data(), column_data.chars.size());

            offsets.resize(row_count);
            for (size_t i = 0; i < row_count; ++i)
                offsets[i] = static_cast<ColumnString::Offset>(column_data.offsets[i]);

            if (name_and_type.type->isNullable())
                return ColumnNullable::create(
                    std::move(nested_column),
                    buildNullMapColumn(row_count, column_data.null_map));
            return nested_column;
        }
        throw std::runtime_error(fmt::format(
            "unsupported bytes target type {} for column {}",
            name_and_type.type->getName(),
            name_and_type.name));
    }

    template <typename ValueType>
    static void installIntegerLikeColumn(
        Block & res,
        const std::unordered_map<String, size_t> & name_to_pos,
        std::vector<bool> & filled,
        const NamesAndTypes & columns,
        const rust::String & col_name,
        const rust::Vec<ValueType> & values,
        const rust::Vec<::std::uint8_t> & null_map)
    {
        const String column_name(col_name);
        const auto it = name_to_pos.find(column_name);
        RUNTIME_CHECK_MSG(it != name_to_pos.end(), "unexpected column {} returned from TiCI", column_name);

        const auto pos = it->second;
        RUNTIME_CHECK_MSG(!filled[pos], "duplicate column {} returned from TiCI", column_name);
        auto & result_column = res.getByPosition(pos);
        result_column.column = buildIntegerLikeColumn(columns[pos], values, null_map);
        filled[pos] = true;
    }

    static void installFloatColumn(
        Block & res,
        const std::unordered_map<String, size_t> & name_to_pos,
        std::vector<bool> & filled,
        const NamesAndTypes & columns,
        const F64ColumnData & column_data)
    {
        const String column_name(column_data.col_name);
        const auto it = name_to_pos.find(column_name);
        RUNTIME_CHECK_MSG(it != name_to_pos.end(), "unexpected column {} returned from TiCI", column_name);

        const auto pos = it->second;
        RUNTIME_CHECK_MSG(!filled[pos], "duplicate column {} returned from TiCI", column_name);
        auto & result_column = res.getByPosition(pos);
        result_column.column = buildFloatColumn(columns[pos], column_data.values, column_data.null_map);
        filled[pos] = true;
    }

    static void installBytesColumn(
        Block & res,
        const std::unordered_map<String, size_t> & name_to_pos,
        std::vector<bool> & filled,
        const NamesAndTypes & columns,
        const BytesColumnData & column_data)
    {
        const String column_name(column_data.col_name);
        const auto it = name_to_pos.find(column_name);
        RUNTIME_CHECK_MSG(it != name_to_pos.end(), "unexpected column {} returned from TiCI", column_name);

        const auto pos = it->second;
        RUNTIME_CHECK_MSG(!filled[pos], "duplicate column {} returned from TiCI", column_name);
        auto & result_column = res.getByPosition(pos);
        result_column.column = buildBytesColumn(columns[pos], column_data);
        filled[pos] = true;
    }

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
