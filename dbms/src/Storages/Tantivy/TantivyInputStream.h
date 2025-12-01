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

rust::Vec<rust::String> getFields(NamesAndTypes & columns);
rust::Vec<::Range> getKeyRanges(ShardInfo::KeyRanges & key_ranges);
std::tuple<::Expr, std::vector<ColumnID>> tipbToTiCIExpr(const tipb::Expr & expr, const TimezoneInfo & timezone_info);

std::tuple<::Expr, std::vector<ColumnID>> tipbToTiCIExpr(
    google::protobuf::RepeatedPtrField<tipb::Expr> exprs,
    const TimezoneInfo & tz);

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

    Block readImpl() override;

protected:
    Block readFromS3(bool is_count);

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
};


} // namespace DB::TS
