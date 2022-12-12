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

#include <Common/getNumberOfCPUCores.h>
#include <common/types.h>

namespace CPUCores
{
UInt16 number_of_logical_cpu_cores = std::thread::hardware_concurrency();
UInt16 number_of_physical_cpu_cores = std::thread::hardware_concurrency() / 2;
} // namespace CPUCores


UInt16 getNumberOfLogicalCPUCores()
{
    return CPUCores::number_of_logical_cpu_cores;
}

UInt16 getNumberOfPhysicalCPUCores()
{
    return CPUCores::number_of_physical_cpu_cores;
}

// We should call this function before Context has been created,
// which will call `getNumberOfLogicalCPUCores`, or we can not
// set cpu cores any more.
void setNumberOfLogicalCPUCores(UInt16 number_of_logical_cpu_cores_)
{
    CPUCores::number_of_logical_cpu_cores = number_of_logical_cpu_cores_;
}

void setNumberOfPhysicalCPUCores(UInt16 number_of_logical_cpu_cores_, UInt16 number_of_hardware_physical_cores)
{
    auto hardware_logical_cpu_cores = std::thread::hardware_concurrency();
    CPUCores::number_of_physical_cpu_cores = number_of_logical_cpu_cores_ / (hardware_logical_cpu_cores / number_of_hardware_physical_cores);
}
