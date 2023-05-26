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
