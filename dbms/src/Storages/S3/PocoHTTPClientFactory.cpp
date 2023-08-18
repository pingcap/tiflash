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

#include <Storages/S3/PocoHTTPClient.h>
#include <Storages/S3/PocoHTTPClientFactory.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpRequest.h>

#include <memory>

namespace DB::S3
{
PocoHTTPClientFactory::PocoHTTPClientFactory(const PocoHTTPClientConfiguration & http_cfg)
    : poco_cfg(http_cfg)
{}

std::shared_ptr<Aws::Http::HttpClient> PocoHTTPClientFactory::CreateHttpClient(
    const Aws::Client::ClientConfiguration & clientConfiguration) const
{
    // TODO: maybe we need different `poco_cfg` for different client sometimes?
    return std::make_shared<PocoHTTPClient>(clientConfiguration, poco_cfg);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHTTPClientFactory::CreateHttpRequest(
    const Aws::String & uri,
    Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory & streamFactory) const
{
    return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHTTPClientFactory::CreateHttpRequest(
    const Aws::Http::URI & uri,
    Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory &) const
{
    auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("PocoHTTPClientFactory", uri, method);

    /// Don't create default response stream. Actual response stream will be set later in PocoHTTPClient.
    request->SetResponseStreamFactory(null_factory);

    return request;
}

} // namespace DB::S3
