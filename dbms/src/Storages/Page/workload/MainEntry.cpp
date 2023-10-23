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

using namespace DB::PS::tests;

int StressWorkload::mainEntry(int argc, char ** argv)
{
    {
        work_load_register<HeavyMemoryCostInGC>();
        work_load_register<HeavyRead>();
        work_load_register<HeavySkewWriteRead>();
        work_load_register<HeavyWrite>();
        work_load_register<HighValidBigFileGCWorkload>();
        work_load_register<HoldSnapshotsLongTime>();
        work_load_register<PageStorageInMemoryCapacity>();
        work_load_register<NormalWorkload>();
        work_load_register<ThousandsOfOffset>();
    }
    try
    {
        auto env = StressEnv::parse(argc, argv);
        env.setup();

        auto & factory = PageWorkloadFactory::getInstance();
        factory.setEnv(env);
        factory.runWorkload();

        SCOPE_EXIT({ factory.stopWorkload(); });

        return StressEnvStatus::getInstance().isSuccess();
    }
    catch (...)
    {
        DB::tryLogCurrentException("");
        exit(-1);
    }
}
