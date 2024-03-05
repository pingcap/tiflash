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

#pragma once

#include <Common/TiFlashBuildInfo.h>
#include <Common/UnifiedLogFormatter.h>
#include <IO/Encryption/DataKeyManager.h>
#include <IO/Encryption/MockKeyManager.h>
#include <Interpreters/Context.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Server/CLIService.h>
#include <Server/IServer.h>
#include <Server/RaftConfigParser.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/TMTContext.h>
#include <daemon/BaseDaemon.h>
#include <pingcap/Config.h>

#include <ext/scope_guard.h>
#include <string>
#include <vector>
#define _TO_STRING(X) #X
#define TO_STRING(X) _TO_STRING(X)
using RaftStoreFFIFunc = void (*)(int argc, const char * const * argv, const DB::EngineStoreServerHelper *);

namespace DTTool
{
int mainEntryTiFlashDTTool(int argc, char ** argv);
}

namespace DTTool::Bench
{
int benchEntry(const std::vector<std::string> & opts);
}

namespace DTTool::Inspect
{
struct InspectArgs
{
    bool check;
    bool dump_columns;
    bool dump_all_columns;
    size_t file_id;
    std::string workdir;
};
int inspectEntry(const std::vector<std::string> & opts, RaftStoreFFIFunc ffi_function);
} // namespace DTTool::Inspect

namespace DTTool::Migrate
{
struct MigrateArgs
{
    bool no_keep;
    bool dry_mode;
    size_t file_id;
    size_t version;
    size_t frame;
    DB::ChecksumAlgo algorithm;
    std::string workdir;
    DB::CompressionMethod compression_method;
    int compression_level;
};
int migrateEntry(const std::vector<std::string> & opts, RaftStoreFFIFunc ffi_function);
} // namespace DTTool::Migrate

namespace DTTool
{
namespace detail
{
using namespace DB;
class ImitativeEnv
{
    DB::ContextPtr createImitativeContext(const std::string & workdir, bool encryption = false)
    {
        // set itself as global context
        global_context = DB::Context::createGlobal();
        global_context->setApplicationType(DB::Context::ApplicationType::LOCAL);

        global_context->initializeTiFlashMetrics();
        KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(encryption);
        global_context->initializeFileProvider(key_manager, encryption);

        auto & settings = global_context->getSettingsRef();
        global_context->initializeBackgroundPool(settings.background_pool_size.get());
        global_context->initializeBlockableBackgroundPool(settings.background_pool_size.get());

        // Theses global variables should be initialized by the following order
        // 1. capacity
        // 2. path pool
        // 3. TMTContext
        auto path = Poco::Path{workdir}.absolute().toString();
        global_context->initializePathCapacityMetric(0, {path}, {}, {}, {});
        global_context->setPathPool(
            /*main_data_paths*/ {path},
            /*latest_data_paths*/ {path},
            /*kvstore_paths*/ Strings{},
            global_context->getPathCapacity(),
            global_context->getFileProvider());
        TiFlashRaftConfig raft_config;
        global_context->initializeGlobalPageIdAllocator();
        global_context->initializeGlobalStoragePoolIfNeed(global_context->getPathPool());
        raft_config.ignore_databases = {"default", "system"};
        raft_config.engine = TiDB::StorageEngine::DT;
        global_context->createTMTContext(raft_config, pingcap::ClusterConfig());

        global_context->setDeltaIndexManager(1024 * 1024 * 100 /*100MB*/);

        auto & path_pool = global_context->getPathPool();
        global_context->getTMTContext().restore(path_pool);
        return global_context;
    }

    static void setupLogger()
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cout);
        Poco::AutoPtr<Poco::Formatter> formatter(new UnifiedLogFormatter<true>());
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Poco::Logger::root().setChannel(formatting_channel);
        Poco::Logger::root().setLevel("trace");
    }

    ContextPtr global_context{};

public:
    explicit ImitativeEnv(const std::string & workdir, bool encryption = false)
    {
        setupLogger();
        createImitativeContext(workdir, encryption);
    }

    ~ImitativeEnv()
    {
        global_context->getTMTContext().setStatusTerminated();
        global_context->shutdown();
        global_context.reset();
    }

    ContextPtr getContext() { return global_context; }
};
} // namespace detail

} // namespace DTTool
