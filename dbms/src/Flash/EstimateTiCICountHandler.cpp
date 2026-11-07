// Copyright 2026 PingCAP, Inc.
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

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Flash/EstimateTiCICountHandler.h>
#include <Interpreters/TimezoneInfo.h>
#include <Storages/Tantivy/TiCIRequestUtils.h>
#include <common/logger_useful.h>
#include <tici-search-lib/src/lib.rs.h>
#include <tipb/executor.pb.h>

namespace DB
{
namespace
{
TimezoneInfo buildEstimateTimezoneInfo(const coprocessor::TiCIEstimateCountRequest & request)
{
    TimezoneInfo timezone_info;
    if (!request.time_zone_name().empty())
        timezone_info.resetByTimezoneName(request.time_zone_name());
    else
        timezone_info.resetByTimezoneOffset(request.time_zone_offset());
    return timezone_info;
}

tipb::FTSQueryInfo parseEstimateQueryInfo(const coprocessor::TiCIEstimateCountRequest & request)
{
    tipb::FTSQueryInfo query_info;
    if (!query_info.ParseFromString(request.fts_query_info()))
        throw TiFlashException("Failed to parse fts_query_info", Errors::Coprocessor::BadRequest);
    if (query_info.match_expr_size() == 0)
        throw TiFlashException("Empty TiCI estimate query expression", Errors::Coprocessor::BadRequest);
    return query_info;
}

rust::Vec<::ShardWithRange> buildEstimateShardRanges(const coprocessor::TiCIEstimateCountRequest & request)
{
    rust::Vec<::ShardWithRange> shards;
    for (const auto & shard_info : request.shard_infos())
    {
        shards.push_back({
            .shard_id = shard_info.shard_id(),
            .ranges = TS::getKeyRanges(shard_info.ranges()),
        });
    }
    return shards;
}
} // namespace

EstimateTiCICountHandler::EstimateTiCICountHandler(
    const coprocessor::TiCIEstimateCountRequest * request_,
    coprocessor::TiCIEstimateCountResponse * response_,
    const String & identifier)
    : request(request_)
    , response(response_)
    , log(Logger::get(identifier))
{}

grpc::Status EstimateTiCICountHandler::execute()
{
    try
    {
        if (request->shard_infos_size() == 0)
            return grpc::Status::OK;

        const auto keyspace_id = RequestUtils::deriveKeyspaceID(request->context());
        const auto fts_query_info = parseEstimateQueryInfo(*request);
        const auto timezone_info = buildEstimateTimezoneInfo(*request);
        auto [query, column_ids] = TS::tipbToTiCIExpr(fts_query_info.match_expr(), timezone_info);
        (void)column_ids;

        auto shard_ranges = buildEstimateShardRanges(*request);
        const auto estimate_result = estimate_count(keyspace_id, shard_ranges, query);
        response->set_est_count(estimate_result.estimated_total_count);
        LOG_DEBUG(
            log,
            "GetEstimateTiCICount done, est_count={}, input_shards={}, available_shards={}, sampled_shards={}",
            response->est_count(),
            request->shard_infos_size(),
            estimate_result.available_shards,
            estimate_result.sampled_shards);
    }
    catch (const TiFlashException & e)
    {
        LOG_WARNING(
            log,
            "GetEstimateTiCICount failed with TiFlash exception: {}\n{}",
            e.displayText(),
            e.getStackTrace().toString());
        response->set_other_error(e.standardText());
    }
    catch (const Exception & e)
    {
        LOG_WARNING(
            log,
            "GetEstimateTiCICount failed with DB exception: {}\n{}",
            e.message(),
            e.getStackTrace().toString());
        response->set_other_error(e.message());
    }
    catch (const pingcap::Exception & e)
    {
        LOG_WARNING(log, "GetEstimateTiCICount failed with KV exception: {}", e.message());
        response->set_other_error(e.message());
    }
    catch (const std::exception & e)
    {
        LOG_WARNING(log, "GetEstimateTiCICount failed: {}", e.what());
        response->set_other_error(e.what());
    }
    catch (...)
    {
        LOG_WARNING(log, "GetEstimateTiCICount failed with unknown exception");
        response->set_other_error("other exception");
    }

    return grpc::Status::OK;
}

} // namespace DB
