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


#include <memory>
#include <string>
#include <unordered_set>

namespace DB
{
struct DNSPTRResolver
{
    virtual ~DNSPTRResolver() = default;

    virtual std::unordered_set<std::string> resolve(const std::string & ip) = 0;

    virtual std::unordered_set<std::string> resolve_v6(const std::string & ip)
        = 0; // NOLINT(readability-identifier-naming)
};
/*
 * Provides a ready-to-use DNSPTRResolver instance.
 * It hides 3rd party lib dependencies, handles initialization and lifetime.
 * Since `get` function is static, it can be called from any context. Including cached static functions.
 **/
class DNSPTRResolverProvider
{
public:
    static std::shared_ptr<DNSPTRResolver> get();
};
} // namespace DB
