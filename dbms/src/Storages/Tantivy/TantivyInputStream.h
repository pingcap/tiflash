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
        UInt64 read_ts_,
        ::Expr match_expr_,
        bool is_count)
        : log(log_)
        , keyspace_id(keyspace_id_)
        , table_id(table_id_)
        , index_id(index_id_)
        , query_shard_info(query_shard_info_)
        , return_columns(return_columns_)
        , limit(limit_)
        , read_ts(read_ts_)
        , match_expr(match_expr_)
        , is_count(is_count)
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

        auto search_param = SearchParam{static_cast<size_t>(limit)};
        if (is_count)
            return_fields = {};

        SearchResult search_result = search(
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

        auto documents = search_result.rows;
        if (documents.empty())
        {
            return res;
        }
        for (auto & name_and_type : return_columns)
        {
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
                for (size_t j = 0; j < documents.size(); j++)
                {
                    // Insert default value for missing fields
                    res.getByName(name_and_type.name).column->assumeMutable()->insertDefault();
                }
                continue;
            }

            auto col = res.getByName(name_and_type.name).column->assumeMutable();
            bool has_null = false;
            if (removeNullable(name_and_type.type)->isStringOrFixedString())
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
                        const auto & v = field_value.string_value;
                        col->insert(Field(String(v.begin(), v.end())));
                    }
                }
            }
            if (removeNullable(name_and_type.type)->isInteger())
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
            if (removeNullable(name_and_type.type)->isDateOrDateTime())
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
            if (has_null)
            {
                RUNTIME_CHECK_MSG(
                    col->isNullable(),
                    "column {} is not nullable, but got null value from TiCI",
                    name_and_type.name);
            }
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
    UInt64 read_ts;
    ::Expr match_expr;
    bool is_count;

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
