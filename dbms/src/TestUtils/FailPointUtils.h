// Copyright 2022 PingCAP, Ltd.
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

#include <Common/FailPoint.h>
#include <TestUtils/ConfigTestUtils.h>

namespace DB
{
namespace tests
{
void initRandomFailPoint(const String & config_str);

#define FAILPOINT_TEST_BEGIN \
    size_t i = 0;            \
    for (; i < 100; ++i)   \
    {                        \
        try                  \
        {\


#define FAILPOINT_TEST_END                                 \
    }                                                      \
    catch (...)                                            \
    {                                                      \
        ::DB::tryLogCurrentException(__PRETTY_FUNCTION__); \
    }                                                      \
    }                                                      \
    ASSERT_EQ(i, 100);
} // namespace tests
} // namespace DB
