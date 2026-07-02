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
#include <Storages/Page/workload/EmptyPages.h>
#include <Storages/Page/workload/HeavyMemoryCostInGC.h>
#include <Storages/Page/workload/HeavyRead.h>
#include <Storages/Page/workload/HeavySkewWriteRead.h>
#include <Storages/Page/workload/HeavyWrite.h>
#include <Storages/Page/workload/HighValidBigFileGC.h>
#include <Storages/Page/workload/HoldSnapshotsLongTime.h>
#include <Storages/Page/workload/Normal.h>
#include <Storages/Page/workload/PSStressEnv.h>
#include <Storages/Page/workload/PSWorkload.h>
#include <Storages/Page/workload/PageStorageInMemoryCapacity.h>
#include <Storages/Page/workload/ThousandsOfOffset.h>

#include <cstdlib>

using namespace DB::PS::tests;

int StressWorkload::mainEntry(int argc, char ** argv)
{
    {
        workload_register<HeavyMemoryCostInGC>();
        workload_register<HeavyRead>();
        workload_register<HeavySkewWriteRead>();
        workload_register<HeavyWrite>();
        workload_register<HighValidBigFileGCWorkload>();
        workload_register<HoldSnapshotsLongTime>();
        workload_register<PageStorageInMemoryCapacity>();
        workload_register<NormalWorkload>();
        workload_register<ThousandsOfOffset>();
        workload_register<EmptyPages>();
    }
    try
    {
        auto env = StressEnv::parse(argc, argv);
        env.setup();

        auto & factory = PageWorkloadFactory::getInstance();
        factory.setEnv(env);
        factory.runWorkload();

        SCOPE_EXIT({ factory.stopWorkload(); });

        Int32 code = StressEnvStatus::getInstance().statCode();
        return code >= 0 ? EXIT_SUCCESS : code;
    }
    catch (...)
    {
        DB::tryLogCurrentException("");
        exit(-1);
    }
}
