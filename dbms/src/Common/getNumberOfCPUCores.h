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

#include <common/types.h>

#include <thread>

UInt16 getNumberOfLogicalCPUCores();
UInt16 getNumberOfPhysicalCPUCores();

// We should call this function before Context has been created,
// which will call `getNumberOfLogicalCPUCores`, or we can not
// set cpu cores any more.
void setNumberOfLogicalCPUCores(UInt16 number_of_logical_cpu_cores_);

void computeAndSetNumberOfPhysicalCPUCores(
    UInt16 number_of_logical_cpu_cores,
    UInt16 number_of_hardware_physical_cores);
