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

#include <Core/TiFlashDisaggregatedMode.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{

class StorageDisaggregatedTest : public ::testing::Test
{
};

TEST_F(StorageDisaggregatedTest, LabelTest)
{
    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::Compute), "tiflash_compute");
    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::Compute), DISAGGREGATED_MODE_COMPUTE_PROXY_LABEL);

    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::Storage), "tiflash");
    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::Storage), DEF_PROXY_LABEL);

    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::None), "tiflash");
    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::None), DEF_PROXY_LABEL);
}

} // namespace tests
} // namespace DB
