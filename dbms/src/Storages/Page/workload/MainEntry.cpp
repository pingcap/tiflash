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
#include <Storages/Page/workload/PSStressEnv.h>
#include <Storages/Page/workload/PSWorkload.h>

using namespace DB::PS::tests;

int StressWorkload::mainEntry(int argc, char ** argv)
{
    {
        // maybe due to sequence of linking, REGISTER_WORKLOAD is not visible to main function in dbms/src/Server/main.cpp
        // cause that REGISTER_WORKLOAD will not be triggered before mainEntry
        // we do this to trigger REGISTER_WORKLOAD explicitly.
        void _work_load_register_named_HeavyMemoryCostInGC();
        void (*f)() = _work_load_register_named_HeavyMemoryCostInGC;
        (void)f;
        void _work_load_register_named_HeavyRead();
        f = _work_load_register_named_HeavyRead;
        (void)f;
        void _work_load_register_named_HeavySkewWriteRead();
        f = _work_load_register_named_HeavySkewWriteRead;
        (void)f;
        void _work_load_register_named_HeavyWrite();
        f = _work_load_register_named_HeavyWrite;
        (void)f;
        void _work_load_register_named_HighValidBigFileGCWorkload();
        f = _work_load_register_named_HighValidBigFileGCWorkload;
        (void)f;
        void _work_load_register_named_HoldSnapshotsLongTime();
        f = _work_load_register_named_HoldSnapshotsLongTime;
        (void)f;
        void _work_load_register_named_PageStorageInMemoryCapacity();
        f = _work_load_register_named_PageStorageInMemoryCapacity;
        (void)f;
        void _work_load_register_named_NormalWorkload();
        f = _work_load_register_named_NormalWorkload;
        (void)f;
        void _work_load_register_named_ThousandsOfOffset();
        f = _work_load_register_named_ThousandsOfOffset;
        (void)f;
    }
    try
    {
        StressEnv::initGlobalLogger();
        auto env = StressEnv::parse(argc, argv);
        env.setup();

        auto & mamager = StressWorkloadManger::getInstance();
        mamager.setEnv(env);
        mamager.runWorkload();

        return StressEnvStatus::getInstance().isSuccess();
    }
    catch (...)
    {
        DB::tryLogCurrentException("");
        exit(-1);
    }
}