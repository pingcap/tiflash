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

#include <Common/FailPoint.h>
#include <Common/UniThreadPool.h>
#include <IO/IOThreadPools.h>
#include <Interpreters/Context.h>
#include <Poco/Environment.h>
#include <Server/StorageConfigParser.h>
#include <Storages/DeltaMerge/ReadThread/DMFileReaderPool.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/S3/S3Common.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
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

    bool enable_colors = isatty(STDERR_FILENO) && isatty(STDOUT_FILENO);
    DB::tests::TiFlashTestEnv::setupLogger("trace", std::cerr, enable_colors);
    auto run_mode = DB::PageStorageRunMode::ONLY_V3;
    DB::tests::TiFlashTestEnv::initializeGlobalContext(/*testdata_path*/ {}, run_mode);
    DB::ServerInfo server_info;
    // `DMFileReaderPool` should be constructed before and destructed after `SegmentReaderPoolManager`.
    DB::DM::DMFileReaderPool::instance();
    // Set the number of threads of SegmentReader to 4 to avoid print too many log.
    DB::DM::SegmentReaderPoolManager::instance().init(4, 1.0);
    DB::DM::SegmentReadTaskScheduler::instance();

    DB::GlobalThreadPool::initialize(/*max_threads*/ 100, /*max_free_threds*/ 10, /*queue_size*/ 1000);
    DB::S3FileCachePool::initialize(/*max_threads*/ 20, /*max_free_threds*/ 10, /*queue_size*/ 1000);
    DB::DataStoreS3Pool::initialize(/*max_threads*/ 20, /*max_free_threds*/ 10, /*queue_size*/ 1000);
    DB::BuildReadTaskForWNPool::initialize(/*max_threads*/ 20, /*max_free_threds*/ 10, /*queue_size*/ 1000);
    DB::BuildReadTaskForWNTablePool::initialize(/*max_threads*/ 20, /*max_free_threds*/ 10, /*queue_size*/ 1000);
    DB::BuildReadTaskPool::initialize(/*max_threads*/ 20, /*max_free_threds*/ 10, /*queue_size*/ 1000);
    DB::RNWritePageCachePool::initialize(/*max_threads*/ 20, /*max_free_threds*/ 10, /*queue_size*/ 1000);
    const auto s3_endpoint = Poco::Environment::get("S3_ENDPOINT", "");
    const auto s3_bucket = Poco::Environment::get("S3_BUCKET", "mockbucket");
    const auto s3_root = Poco::Environment::get("S3_ROOT", "tiflash_ut/");
    const auto s3_verbose = Poco::Environment::get("S3_VERBOSE", "false");
    const auto s3_poco_client = Poco::Environment::get("S3_POCO_CLIENT", "true");
    const auto access_key_id = Poco::Environment::get("AWS_ACCESS_KEY_ID", "");
    const auto secret_access_key = Poco::Environment::get("AWS_SECRET_ACCESS_KEY", "");
    const auto mock_s3 = Poco::Environment::get("MOCK_S3", "true"); // In unit-tests, use MockS3Client by default.
    auto s3config = DB::StorageS3Config{
        .verbose = s3_verbose == "true",
        .enable_poco_client = s3_poco_client == "true",
        .endpoint = s3_endpoint,
        .bucket = s3_bucket,
        .access_key_id = access_key_id,
        .secret_access_key = secret_access_key,
        .root = s3_root,
    };
    Poco::Environment::set("AWS_EC2_METADATA_DISABLED", "true"); // disable to speedup testing
    DB::tests::TiFlashTestEnv::setIsMockedS3Client(mock_s3 == "true");
    DB::S3::ClientFactory::instance().init(s3config, mock_s3 == "true");

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
    DB::S3FileCachePool::shutdown();
    DB::tests::TiFlashTestEnv::shutdown();
    DB::S3::ClientFactory::instance().shutdown();

    return ret;
}
