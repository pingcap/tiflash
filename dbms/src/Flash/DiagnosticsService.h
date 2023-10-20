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

#include <Server/IServer.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#include <kvproto/diagnosticspb.grpc.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
class DiagnosticsService final
    : public ::diagnosticspb::Diagnostics::Service
    , public std::enable_shared_from_this<DiagnosticsService>
    , private boost::noncopyable
{
public:
    explicit DiagnosticsService(Context & context_, Poco::Util::LayeredConfiguration & config_)
        : log(&Poco::Logger::get("DiagnosticsService"))
        , context(context_)
        , config(config_)
    {}
    ~DiagnosticsService() override = default;

public:
    ::grpc::Status search_log(
        ::grpc::ServerContext * grpc_context,
        const ::diagnosticspb::SearchLogRequest * request,
        ::grpc::ServerWriter<::diagnosticspb::SearchLogResponse> * stream) override;

    ::grpc::Status server_info(
        ::grpc::ServerContext * grpc_context,
        const ::diagnosticspb::ServerInfoRequest * request,
        ::diagnosticspb::ServerInfoResponse * response) override;

private:
    Poco::Logger * log;
    Context & context;

    Poco::Util::LayeredConfiguration & config;
};

} // namespace DB
