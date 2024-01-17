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

#include <Common/UnifiedLogFormatter.h>
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Server/RaftConfigParser.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/PathPool.h>
#include <Storages/S3/S3Common.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <common/logger_useful.h>

#include <memory>

namespace DB::tests
{
std::vector<ContextPtr> TiFlashTestEnv::global_contexts = {};
bool TiFlashTestEnv::is_mocked_s3_client = true;

String TiFlashTestEnv::getTemporaryPath(const std::string_view test_case, bool get_abs)
{
    String path = ".";
    const char * temp_prefix = getenv("TIFLASH_TEMP_DIR");
    if (temp_prefix != nullptr)
        path = DB::String(temp_prefix);
    path += "/tmp/";
    if (!test_case.empty())
        path += std::string(test_case);

    Poco::Path poco_path(path);
    if (get_abs)
        return poco_path.absolute().toString();
    else
        return poco_path.toString();
}

void TiFlashTestEnv::tryCreatePath(const std::string & path)
{
    try
    {
        Poco::File p(path);
        if (!p.exists())
            p.createDirectories();
    }
    catch (...)
    {
        tryLogCurrentException("gtest", fmt::format("while removing dir `{}`", path));
    }
}

void TiFlashTestEnv::tryRemovePath(const std::string & path, bool recreate)
{
    try
    {
        // drop the data on disk
        Poco::File p(path);
        if (p.exists())
        {
            p.remove(true);
        }

        // re-create empty directory for testing
        if (recreate)
        {
            p.createDirectories();
        }
    }
    catch (...)
    {
        tryLogCurrentException("gtest", fmt::format("while removing dir `{}`", path));
    }
}

void TiFlashTestEnv::initializeGlobalContext(
    Strings testdata_path,
    PageStorageRunMode ps_run_mode,
    uint64_t bg_thread_count)
{
    addGlobalContext(DB::Settings(), testdata_path, ps_run_mode, bg_thread_count);
}

void TiFlashTestEnv::addGlobalContext(
    const DB::Settings & settings_,
    Strings testdata_path,
    PageStorageRunMode ps_run_mode,
    uint64_t bg_thread_count)
{
    // set itself as global context
    auto global_context = std::shared_ptr<Context>(DB::Context::createGlobal());
    global_contexts.push_back(global_context);
    global_context->setApplicationType(DB::Context::ApplicationType::LOCAL);
    global_context->setTemporaryPath(getTemporaryPath());

    global_context->initializeTiFlashMetrics();
    KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(false);
    global_context->initializeFileProvider(key_manager, false);

    // initialize background & blockable background thread pool
    global_context->setSettings(settings_);
    Settings & settings = global_context->getSettingsRef();
    global_context->initializeBackgroundPool(
        bg_thread_count == 0 ? settings.background_pool_size.get() : bg_thread_count);
    global_context->initializeBlockableBackgroundPool(
        bg_thread_count == 0 ? settings.background_pool_size.get() : bg_thread_count);

    // Theses global variables should be initialized by the following order
    // 1. capacity
    // 2. path pool
    // 3. TMTContext

    if (testdata_path.empty())
    {
        testdata_path = {getTemporaryPath()};
    }
    else
    {
        Strings absolute_testdata_path;
        for (const auto & path : testdata_path)
        {
            absolute_testdata_path.push_back(Poco::Path(path).absolute().toString());
        }
        testdata_path.swap(absolute_testdata_path);
    }
    global_context->initializePathCapacityMetric(0, testdata_path, {}, {}, {});

    auto paths = getPathPool(testdata_path);
    global_context->setPathPool(
        paths.first,
        paths.second,
        Strings{},
        global_context->getPathCapacity(),
        global_context->getFileProvider());

    global_context->setPageStorageRunMode(ps_run_mode);
    global_context->initializeGlobalPageIdAllocator();
    global_context->initializeGlobalStoragePoolIfNeed(global_context->getPathPool());
    global_context->initializeWriteNodePageStorageIfNeed(global_context->getPathPool());
    LOG_INFO(Logger::get(), "Storage mode : {}", static_cast<UInt8>(global_context->getPageStorageRunMode()));

    TiFlashRaftConfig raft_config;

    raft_config.ignore_databases = {"system"};
    raft_config.engine = TiDB::StorageEngine::DT;
    raft_config.for_unit_test = true;
    global_context->createTMTContext(raft_config, pingcap::ClusterConfig());

    global_context->setDeltaIndexManager(1024 * 1024 * 100 /*100MB*/);

    auto & path_pool = global_context->getPathPool();
    global_context->getTMTContext().restore(path_pool);

    global_context->initializeSharedBlockSchemas(10000);
}

ContextPtr TiFlashTestEnv::getContext()
{
    return getContext({});
}

ContextPtr TiFlashTestEnv::getContext(const DB::Settings & settings, Strings testdata_path)
{
    Context context = *global_contexts[0];
    context.setGlobalContext(*global_contexts[0]);
    // Load `testdata_path` as path if it is set.
    const String root_path = [&]() {
        const auto root_path = testdata_path.empty() ? getTemporaryPath(fmt::format("{}/", getpid()), /*get_abs*/ false)
                                                     : testdata_path[0];
        return Poco::Path(root_path).absolute().toString();
    }();
    if (testdata_path.empty())
        testdata_path.push_back(root_path);
    context.setPath(root_path);
    auto paths = getPathPool(testdata_path);
    context.setPathPool(paths.first, paths.second, Strings{}, context.getPathCapacity(), context.getFileProvider());
    global_contexts[0]->initializeGlobalPageIdAllocator();
    global_contexts[0]->initializeGlobalStoragePoolIfNeed(context.getPathPool());
    global_contexts[0]->tryReleaseWriteNodePageStorageForTest();
    global_contexts[0]->initializeWriteNodePageStorageIfNeed(context.getPathPool());
    context.getSettingsRef() = settings;
    return std::make_shared<Context>(context);
}

FileProviderPtr TiFlashTestEnv::getDefaultFileProvider()
{
    return global_contexts[0]->getFileProvider();
}

void TiFlashTestEnv::shutdown()
{
    for (auto & context : global_contexts)
    {
        context->getTMTContext().setStatusTerminated();
        context->shutdown();
        context.reset();
    }
}

void TiFlashTestEnv::setupLogger(const String & level, std::ostream & os, bool enable_colors)
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(os);
    Poco::AutoPtr<Poco::Formatter> formatter;
    if (enable_colors)
        formatter = new UnifiedLogFormatter<true>();
    else
        formatter = new UnifiedLogFormatter<false>();
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel(level);
}

void TiFlashTestEnv::setUpTestContext(
    Context & context,
    DAGContext * dag_context,
    MockStorage * mock_storage,
    const TestType & test_type)
{
    switch (test_type)
    {
    case TestType::EXECUTOR_TEST:
        context.setExecutorTest();
        break;
    case TestType::INTERPRETER_TEST:
        context.setInterpreterTest();
        break;
    }
    context.setMockStorage(mock_storage);
    context.setDAGContext(dag_context);
    context.getTimezoneInfo().resetByDAGRequest(*dag_context->dag_request);
    /// by default non-mpp task will do collation insensitive group by, let the test do
    /// collation sensitive group by setting `group_by_collation_sensitive` to true
    context.setSetting("group_by_collation_sensitive", Field(static_cast<UInt64>(1)));
}

std::unique_ptr<PathPool> TiFlashTestEnv::createCleanPathPool(const String & path)
{
    // Drop files on disk
    LOG_INFO(Logger::get("Test"), "Clean path {} for bootstrap", path);
    tryRemovePath(path, /*recreate=*/true);

    auto & global_ctx = TiFlashTestEnv::getGlobalContext();
    auto path_capacity = global_ctx.getPathCapacity();
    auto provider = global_ctx.getFileProvider();
    // Create a PathPool instance on the clean directory
    Strings main_data_paths{path};
    return std::make_unique<PathPool>(main_data_paths, main_data_paths, Strings{}, path_capacity, provider);
}

FileProviderPtr TiFlashTestEnv::getMockFileProvider()
{
    bool encryption_enabled = false;
    return std::make_shared<FileProvider>(std::make_shared<MockKeyManager>(encryption_enabled), encryption_enabled);
}

bool TiFlashTestEnv::createBucketIfNotExist(::DB::S3::TiFlashS3Client & s3_client)
{
    Aws::S3::Model::CreateBucketRequest request;
    request.SetBucket(s3_client.bucket());
    auto outcome = s3_client.CreateBucket(request);
    if (outcome.IsSuccess())
    {
        LOG_DEBUG(s3_client.log, "Created bucket {}", s3_client.bucket());
    }
    else if (
        outcome.GetError().GetExceptionName() == "BucketAlreadyOwnedByYou"
        || outcome.GetError().GetExceptionName() == "BucketAlreadyExists")
    {
        LOG_DEBUG(s3_client.log, "Bucket {} already exist", s3_client.bucket());
    }
    else
    {
        const auto & err = outcome.GetError();
        LOG_ERROR(s3_client.log, "CreateBucket: {}:{}", err.GetExceptionName(), err.GetMessage());
    }
    return outcome.IsSuccess() || outcome.GetError().GetExceptionName() == "BucketAlreadyOwnedByYou"
        || outcome.GetError().GetExceptionName() == "BucketAlreadyExists";
}

void TiFlashTestEnv::deleteBucket(::DB::S3::TiFlashS3Client & s3_client)
{
    TiFlashTestEnv::createBucketIfNotExist(s3_client);
    if (!is_mocked_s3_client)
    {
        // All objects (including all object versions and delete markers)
        // in the bucket must be deleted before the bucket itself can be
        // deleted.
        LOG_INFO(s3_client.log, "DeleteBucket, clean all existing objects begin");
        S3::rawListPrefix(
            s3_client,
            s3_client.bucket(),
            s3_client.root(),
            "",
            [&](const Aws::S3::Model::ListObjectsV2Result & r) -> S3::PageResult {
                for (const auto & obj : r.GetContents())
                {
                    const auto & key = obj.GetKey();
                    LOG_INFO(s3_client.log, "DeleteBucket, clean existing object, key={}", key);
                    S3::rawDeleteObject(s3_client, s3_client.bucket(), key);
                }
                return S3::PageResult{.num_keys = r.GetContents().size(), .more = true};
            });
        LOG_INFO(s3_client.log, "DeleteBucket, clean all existing objects done");
    }
    Aws::S3::Model::DeleteBucketRequest request;
    request.SetBucket(s3_client.bucket());
    auto outcome = s3_client.DeleteBucket(request);
    if (!outcome.IsSuccess())
    {
        const auto & err = outcome.GetError();
        LOG_WARNING(s3_client.log, "DeleteBucket: {}:{}", err.GetExceptionName(), err.GetMessage());
    }
}


void TiFlashTestEnv::disableS3Config()
{
    DB::S3::ClientFactory::instance().disable();
}

void TiFlashTestEnv::enableS3Config()
{
    DB::S3::ClientFactory::instance().enable();
}
} // namespace DB::tests
