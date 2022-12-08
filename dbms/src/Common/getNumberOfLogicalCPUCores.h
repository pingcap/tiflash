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

<<<<<<< HEAD:dbms/src/Common/getNumberOfPhysicalCPUCores.h
/// Get number of CPU cores without hyper-threading.
unsigned getNumberOfPhysicalCPUCores();
=======
#include <common/types.h>

#include <thread>

UInt16 getNumberOfLogicalCPUCores();

// We should call this function before Context has been created,
// which will call `getNumberOfLogicalCPUCores`, or we can not
// set cpu cores any more.
void setNumberOfLogicalCPUCores(UInt16 max_logical_cpu_cores);
>>>>>>> 966e7e228e (Get correct cpu cores in k8s pod (#6430)):dbms/src/Common/getNumberOfLogicalCPUCores.h
