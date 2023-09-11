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

#include <Common/Exception.h>
#include <Flash/DiagnosticsService.h>
#include <Flash/LogSearch.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Path.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <fmt/ranges.h>

#include <ext/scope_guard.h>
#include <memory>
#include <vector>


namespace DB
{
using diagnosticspb::LogLevel;
using diagnosticspb::SearchLogResponse;

::grpc::Status DiagnosticsService::server_info(
    ::grpc::ServerContext *,
    const ::diagnosticspb::ServerInfoRequest * request,
    ::diagnosticspb::ServerInfoResponse * response)
try
{
    if (context.getSharedContextDisagg()->isDisaggregatedComputeMode()
        && context.getSharedContextDisagg()->use_autoscaler)
    {
        String err_msg = "tiflash compute node should be managed by AutoScaler instead of PD, this grpc should not be "
                         "called be AutoScaler for now";
        LOG_ERROR(log, err_msg);
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, err_msg);
    }
    const TiFlashRaftProxyHelper * helper = context.getTMTContext().getKVStore()->getProxyHelper();
    if (helper)
    {
        std::string req = request->SerializeAsString();
        helper->fn_server_info(helper->proxy_ptr, strIntoView(&req), response);
    }
    else
    {
        LOG_ERROR(log, "TiFlashRaftProxyHelper is null, `DiagnosticsService::server_info` is useless");
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, "TiFlashRaftProxyHelper is null");
    }
    return ::grpc::Status::OK;
}
catch (const Exception & e)
{
    LOG_ERROR(log, e.displayText());
    return grpc::Status(grpc::StatusCode::INTERNAL, "internal error");
}
catch (const std::exception & e)
{
    LOG_ERROR(log, e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, "internal error");
}

// get & filter(ts of last record < start-time) all files in same log directory.
std::list<std::string> getFilesToSearch(
    Poco::Util::LayeredConfiguration & config,
    Poco::Logger * log,
    const int64_t start_time)
{
    std::list<std::string> files_to_search;

    std::string log_dir; // log directory
    auto error_log_file_prefix = config.getString("logger.errorlog", "*");
    auto tracing_log_file_prefix = config.getString("logger.tracing_log", "*");
    // ignore tiflash error log and mpp task tracing log
    std::vector<String> ignore_log_file_prefixes = {error_log_file_prefix, tracing_log_file_prefix};

    {
        auto log_file_prefix = config.getString("logger.log");
        if (auto it = log_file_prefix.rfind('/'); it != std::string::npos)
        {
            log_dir = std::string(log_file_prefix.begin(), log_file_prefix.begin() + it);
        }
    }

    LOG_DEBUG(log, "got log directory {}", log_dir);

    if (log_dir.empty())
        return files_to_search;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(log_dir); it != dir_end; ++it)
    {
        const auto & path = it->path();
        if (it->isFile() && it->exists())
        {
            if (!FilterFileByDatetime(path, ignore_log_file_prefixes, start_time))
                files_to_search.emplace_back(path);
        }
    }

    LOG_DEBUG(log, "got log files to search {}", files_to_search);

    return files_to_search;
}

grpc::Status searchLog(
    Poco::Logger * log,
    ::grpc::ServerWriter<::diagnosticspb::SearchLogResponse> * stream,
    LogIterator & log_itr)
{
    static constexpr size_t LOG_BATCH_SIZE = 256;

    for (;;)
    {
        size_t i = 0;
        auto resp = SearchLogResponse::default_instance();
        for (; i < LOG_BATCH_SIZE;)
        {
            if (auto tmp_msg = log_itr.next(); !tmp_msg)
            {
                break;
            }
            else
            {
                i++;
                resp.mutable_messages()->Add(std::move(*tmp_msg));
            }
        }

        if (i == 0)
            break;

        if (!stream->Write(resp))
        {
            LOG_DEBUG(log, "Write response failed for unknown reason.");
            return grpc::Status(grpc::StatusCode::UNKNOWN, "Write response failed for unknown reason.");
        }
    }
    return ::grpc::Status::OK;
}

::grpc::Status DiagnosticsService::search_log(
    ::grpc::ServerContext *,
    const ::diagnosticspb::SearchLogRequest * request,
    ::grpc::ServerWriter<::diagnosticspb::SearchLogResponse> * stream)
{
    int64_t start_time = request->start_time();
    int64_t end_time = request->end_time();
    if (end_time == 0)
    {
        end_time = std::numeric_limits<int64_t>::max();
    }
    std::vector<LogLevel> levels;
    for (auto level : request->levels())
    {
        levels.push_back(static_cast<LogLevel>(level));
    }

    std::vector<std::string> patterns;
    for (auto && pattern : request->patterns())
    {
        patterns.push_back(pattern);
    }

    LOG_DEBUG(log, "Handling SearchLog: {}", request->DebugString());
    SCOPE_EXIT({ LOG_DEBUG(log, "Handling SearchLog done: {}", request->DebugString()); });

    auto files_to_search = getFilesToSearch(config, log, start_time);

    for (const auto & path : files_to_search)
    {
        LOG_DEBUG(log, "start to search file {}", path);
        auto status = grpc::Status::OK;
        ReadLogFile(path, [&](std::istream & istr) {
            LogIterator log_itr(start_time, end_time, levels, patterns, istr);
            status = searchLog(log, stream, log_itr);
        });
        if (!status.ok())
            return status;
    }

    return ::grpc::Status::OK;
}
} // namespace DB
