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

#include <TestUtils/ConfigTestUtils.h>
#include <TestUtils/FailPointUtils.h>

namespace DB
{
namespace tests
{
void initRandomFailPoint(const std::string & config_str)
{
    fiu_init(0); // init failpoint
    auto config = loadConfigFromString(config_str);
    FailPointHelper::initRandomFailPoints(*config, Logger::get("test"));
}

void disableRandomFailPoint(const std::string & config_str)
{
    auto config = loadConfigFromString(config_str);
    FailPointHelper::disableRandomFailPoints(*config, Logger::get("test"));
}

} // namespace tests
} // namespace DB
