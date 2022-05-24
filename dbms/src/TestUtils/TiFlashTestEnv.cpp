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

#include <Common/UnifiedLogPatternFormatter.h>
#include <Encryption/MockKeyManager.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Server/RaftConfigParser.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Transaction/TMTContext.h>
#include <TestUtils/TiFlashTestEnv.h>

namespace DB::tests
{
std::unique_ptr<Context> TiFlashTestEnv::global_context = nullptr;

void TiFlashTestEnv::initializeGlobalContext(Strings testdata_path, bool enable_ps_v3)
{
    // set itself as global context
    global_context = std::make_unique<DB::Context>(DB::Context::createGlobal());
    global_context->setGlobalContext(*global_context);
    global_context->setApplicationType(DB::Context::ApplicationType::SERVER);

    global_context->initializeTiFlashMetrics();
    KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(false);
    global_context->initializeFileProvider(key_manager, false);

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
        true,
        global_context->getPathCapacity(),
        global_context->getFileProvider());

    global_context->setPageStorageRunMode(enable_ps_v3 ? PageStorageRunMode::ONLY_V3 : PageStorageRunMode::ONLY_V2);
    global_context->initializeGlobalStoragePoolIfNeed(global_context->getPathPool());
    LOG_FMT_INFO(Logger::get("TiFlashTestEnv"), "Storage mode : {}", static_cast<UInt8>(global_context->getPageStorageRunMode()));

    TiFlashRaftConfig raft_config;

    raft_config.ignore_databases = {"default", "system"};
    raft_config.engine = TiDB::StorageEngine::DT;
    global_context->createTMTContext(raft_config, pingcap::ClusterConfig());

    global_context->setDeltaIndexManager(1024 * 1024 * 100 /*100MB*/);

    global_context->getTMTContext().restore();
}

Context TiFlashTestEnv::getContext(const DB::Settings & settings, Strings testdata_path)
{
    Context context = *global_context;
    context.setGlobalContext(*global_context);
    // Load `testdata_path` as path if it is set.
    const String root_path = testdata_path.empty() ? getTemporaryPath() : testdata_path[0];
    if (testdata_path.empty())
        testdata_path.push_back(root_path);
    context.setPath(root_path);
    auto paths = getPathPool(testdata_path);
    context.setPathPool(paths.first, paths.second, Strings{}, true, context.getPathCapacity(), context.getFileProvider());
    context.getSettingsRef() = settings;
    return context;
}

void TiFlashTestEnv::shutdown()
{
    global_context->getTMTContext().setStatusTerminated();
    global_context->shutdown();
    global_context.reset();
}

void TiFlashTestEnv::setupLogger(const String & level, std::ostream & os)
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(os);
    Poco::AutoPtr<UnifiedLogPatternFormatter> formatter(new UnifiedLogPatternFormatter());
    formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i [%I] <%p> %s: %t");
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel(level);
}
} // namespace DB::tests
