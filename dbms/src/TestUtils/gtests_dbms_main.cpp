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

#include <Common/FailPoint.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::FailPoints
{
extern const char force_set_dtfile_exist_when_acquire_id[];
} // namespace DB::FailPoints

// TODO: Optmize set-up & tear-down process which may cost more than 2s. It's NOT friendly for gtest_parallel.
int main(int argc, char ** argv)
{
    DB::tests::TiFlashTestEnv::setupLogger();
    DB::tests::TiFlashTestEnv::initializeGlobalContext();

#ifdef FIU_ENABLE
    fiu_init(0); // init failpoint

    DB::FailPointHelper::enableFailPoint(DB::FailPoints::force_set_dtfile_exist_when_acquire_id);
#endif

    ::testing::InitGoogleTest(&argc, argv);
    auto ret = RUN_ALL_TESTS();

    DB::tests::TiFlashTestEnv::shutdown();

    return ret;
}
