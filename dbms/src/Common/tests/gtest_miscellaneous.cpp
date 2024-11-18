// Copyright 2024 PingCAP, Inc.
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

#include <Common/getNumberOfCPUCores.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{

TEST(CommonMiscellaneousTest, cpuRelated)
{
    auto hardware_logical_cpu_cores = std::thread::hardware_concurrency();
    UInt16 number_of_logical_cpu_cores = std::thread::hardware_concurrency();

    computeAndSetNumberOfPhysicalCPUCores(number_of_logical_cpu_cores, hardware_logical_cpu_cores);
    ASSERT_EQ(getNumberOfPhysicalCPUCores(), hardware_logical_cpu_cores);

    computeAndSetNumberOfPhysicalCPUCores(number_of_logical_cpu_cores, 0);
    ASSERT_EQ(1, getNumberOfPhysicalCPUCores());

    UInt16 test_num = 123;
    computeAndSetNumberOfPhysicalCPUCores(test_num, hardware_logical_cpu_cores * 2);
    ASSERT_EQ(test_num, getNumberOfPhysicalCPUCores());

    computeAndSetNumberOfPhysicalCPUCores(1, hardware_logical_cpu_cores * 2);
    ASSERT_EQ(1, getNumberOfPhysicalCPUCores());

    computeAndSetNumberOfPhysicalCPUCores(0, hardware_logical_cpu_cores);
    ASSERT_EQ(1, getNumberOfPhysicalCPUCores());
}

} // namespace tests
} // namespace DB
