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
#include <Storages/DeltaMerge/ReadThread/ColumnSharingCache.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>
#include <signal.h>

namespace DB::FailPoints
{
extern const char force_set_dtfile_exist_when_acquire_id[];
} // namespace DB::FailPoints

void fault_signal_handler(int signum)
{
    ::signal(signum, SIG_DFL);
    std::cerr << "Received signal " << strsignal(signum) << std::endl;
    std::cerr << StackTrace().toString() << std::endl;
    ::raise(signum);
}

void install_fault_signal_handlers(std::initializer_list<int> signums)
{
    for (auto signum : signums)
    {
        ::signal(signum, fault_signal_handler);
    }
}

class ThrowListener : public testing::EmptyTestEventListener
{
    void OnTestPartResult(const testing::TestPartResult & result) override
    {
        if (result.type() == testing::TestPartResult::kFatalFailure)
        {
            throw ::testing::AssertionException(result);
        }
    }
};


// TODO: Optmize set-up & tear-down process which may cost more than 2s. It's NOT friendly for gtest_parallel.
int main(int argc, char ** argv)
{
    install_fault_signal_handlers({SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGTERM});

    DB::tests::TiFlashTestEnv::setupLogger();
    DB::tests::TiFlashTestEnv::initializeGlobalContext();
    DB::ServerInfo server_info;
    // `DMFileReaderPool` should be constructed before and destructed after `SegmentReaderPoolManager`.
    DB::DM::DMFileReaderPool::instance();
    DB::DM::SegmentReaderPoolManager::instance().init(server_info);
    DB::DM::SegmentReadTaskScheduler::instance();

#ifdef FIU_ENABLE
    fiu_init(0); // init failpoint

    DB::FailPointHelper::enableFailPoint(DB::FailPoints::force_set_dtfile_exist_when_acquire_id);
#endif

    ::testing::InitGoogleTest(&argc, argv);
    ::testing::UnitTest::GetInstance()->listeners().Append(new ThrowListener);

    auto ret = RUN_ALL_TESTS();

    // `SegmentReader` threads may hold a segment and its delta-index for read.
    // `TiFlashTestEnv::shutdown()` will destroy `DeltaIndexManager`.
    // Stop threads explicitly before `TiFlashTestEnv::shutdown()`.
    DB::DM::SegmentReaderPoolManager::instance().stop();
    DB::tests::TiFlashTestEnv::shutdown();

    return ret;
}
