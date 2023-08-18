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

#include <Common/DNSPTRResolverProvider.h>
// Note that this include the `CaresPTRResolver` defined under "Flash/" because that class need
// to link with thirdparty that is not included by libcommon_io. Use a register function
// instead of include it could be better.
#include <Flash/CaresPTRResolver.h>

namespace DB
{
std::shared_ptr<DNSPTRResolver> DNSPTRResolverProvider::get()
{
    static auto resolver = std::make_shared<CaresPTRResolver>(CaresPTRResolver::provider_token{});
    return resolver;
}
} // namespace DB
