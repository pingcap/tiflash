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
/// Suppress gcc warning: ‘*((void*)&<anonymous> +4)’ may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <cpptoml.h>
#if !__clang__
#pragma GCC diagnostic pop
#endif
#include <Common/Config/TOMLConfiguration.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace DB
{
namespace tests
{
inline static auto loadConfigFromString(const std::string & s)
{
    std::istringstream ss(s);
    cpptoml::parser p(ss);
    auto table = p.parse();
    Poco::AutoPtr<Poco::Util::LayeredConfiguration> config = new Poco::Util::LayeredConfiguration();
    config->add(new DB::TOMLConfiguration(table), false); // Take ownership of TOMLConfig
    return config;
}
} // namespace tests
} // namespace DB
