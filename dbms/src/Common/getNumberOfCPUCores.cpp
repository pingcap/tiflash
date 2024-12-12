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

#include <Common/Logger.h>
#include <Common/getNumberOfCPUCores.h>
#include <common/logger_useful.h>
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

void computeAndSetNumberOfPhysicalCPUCores(
    UInt16 number_of_logical_cpu_cores_,
    UInt16 number_of_hardware_physical_cores)
{
    number_of_hardware_physical_cores = std::max<UInt16>(1, number_of_hardware_physical_cores);

    // First of all, we need to take consideration of two situation:
    //   1. tiflash on physical machine.
    //      In old codes, tiflash needs to set max_threads which is equal to
    //      physical cpu cores, so we need to ensure this behavior is not broken.
    //   2. tiflash on virtual environment.
    //      In virtual environment, when setting max_threads, we can't directly
    //      get physical cpu cores to set this variable because only host machine's
    //      physical cpu core can be reached. So, number of physical cpus cores can
    //      only be assigned by calculated with logical cpu cores.
    //
    // - `number_of_logical_cpu_cores_` which represents how many logical cpu cores a tiflash could use(no matter in physical or virtual environment) is assigned from ServerInfo.
    // - `hardware_logical_cpu_cores` represents how many logical cpu cores the host physical machine has.
    // - `number_of_hardware_physical_cores` represents how many physical cpu cores the host physical machine has.
    // - `(hardware_logical_cpu_cores / number_of_hardware_physical_cores)` means how many logical cpu core a physical cpu core has.
    // - `number_of_logical_cpu_cores_ / (hardware_logical_cpu_cores / number_of_hardware_physical_cores)` means how many physical cpu cores the tiflash process could use. (Actually, it's needless to get physical cpu cores in virtual environment, but we must ensure the behavior `1` is not broken)
    auto hardware_logical_cpu_cores = std::thread::hardware_concurrency();

    UInt16 thread_num_per_physical_core = hardware_logical_cpu_cores / number_of_hardware_physical_cores;
    thread_num_per_physical_core = std::max<UInt16>(1, thread_num_per_physical_core);

    UInt16 physical_cpu_cores = number_of_logical_cpu_cores_ / thread_num_per_physical_core;
    CPUCores::number_of_physical_cpu_cores = physical_cpu_cores > 0 ? physical_cpu_cores : 1;
    LOG_INFO(
        DB::Logger::get(),
        "logical cpu cores: {}, hardware logical cpu cores: {}, hardware physical cpu cores: {}, physical cpu cores: "
        "{}, number_of_physical_cpu_cores: {}",
        number_of_logical_cpu_cores_,
        hardware_logical_cpu_cores,
        number_of_hardware_physical_cores,
        physical_cpu_cores,
        CPUCores::number_of_physical_cpu_cores);
}
