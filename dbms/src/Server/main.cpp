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

#include <Common/ErrorExporter.h>
#include <Common/TiFlashBuildInfo.h>
#include <Common/config.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <common/config_common.h>
#include <config_tools.h>

#include <iostream>
#include <string>
#include <utility> /// pair
#include <vector>

#if ENABLE_TIFLASH_SERVER
#include "Server.h"
#endif
#if ENABLE_TIFLASH_DTTOOL
#include <Server/DTTool/DTTool.h>
#endif
#if ENABLE_TIFLASH_DTWORKLOAD
#include <Storages/DeltaMerge/workload/DTWorkload.h>
#endif
#if ENABLE_TIFLASH_PAGEWORKLOAD
#include <Storages/Page/workload/PSWorkload.h>
#endif
#if ENABLE_TIFLASH_PAGECTL
#include <Storages/Page/tools/PageCtl/PageStorageCtl.h>
#endif
#if ENABLE_TIFLASH_CHECKPOINTTOOL
#include <Storages/Page/tools/Checkpoint/CheckpointTool.h>
#endif
#include <Common/StringUtils/StringUtils.h>
#include <Server/DTTool/DTTool.h>

/// Universal executable for various clickhouse applications
#if ENABLE_TIFLASH_SERVER
int mainEntryClickHouseServer(int argc, char ** argv);
#endif
#if ENABLE_TIFLASH_CLIENT
int mainEntryClickHouseClient(int argc, char ** argv);
#endif

extern "C" void print_raftstore_proxy_version();

int mainEntryVersion(int, char **)
{
    TiFlashBuildInfo::outputDetail(std::cout);
    std::cout << std::endl;

    std::cout << "Raft Proxy" << std::endl;
    print_raftstore_proxy_version();
    return 0;
}

int mainExportError(int argc, char ** argv)
{
    if (argc < 2)
    {
        std::cerr << "Usage:" << std::endl;
        std::cerr << "\ttiflash errgen [DST]" << std::endl;
        return -1;
    }
    std::string dst_path = argv[1];
    DB::WriteBufferFromFile wb(dst_path);
    auto & registry = DB::TiFlashErrorRegistry::instance();
    auto all_errors = registry.allErrors();

    {
        // RAII
        DB::ErrorExporter exporter(wb);
        for (const auto & error : all_errors)
        {
            exporter.writeError(error);
        }
    }
    return 0;
}

namespace
{
using MainFunc = int (*)(int, char **);


/// Add an item here to register new application
std::pair<const char *, MainFunc> clickhouse_applications[] = {
#if ENABLE_TIFLASH_CLIENT
    {"client", mainEntryClickHouseClient},
#endif
#if ENABLE_TIFLASH_SERVER
    {"server", mainEntryClickHouseServer},
#endif
#if ENABLE_TIFLASH_DTTOOL
    {"dttool", DTTool::mainEntryTiFlashDTTool},
#endif
#if ENABLE_TIFLASH_DTWORKLOAD
    {"dtworkload", DB::DM::tests::DTWorkload::mainEntry},
#endif
#if ENABLE_TIFLASH_PAGEWORKLOAD
    {"pageworkload", DB::PS::tests::StressWorkload::mainEntry},
#endif
#if ENABLE_TIFLASH_PAGECTL
    {"pagectl", DB::PageStorageCtl::mainEntry},
#endif
#if ENABLE_TIFLASH_CHECKPOINTTOOL
    {"pagecheckpoint", DB::PS::CheckpointTool::mainEntry},
#endif
    {"version", mainEntryVersion},
    {"errgen", mainExportError}};


int printHelp(int, char **)
{
    std::cerr << "Use one of the following commands:" << std::endl;
    for (auto & application : clickhouse_applications)
        std::cerr << "tiflash " << application.first << " [args] " << std::endl;
    return -1;
};


bool isClickhouseApp(const std::string & app_suffix, std::vector<char *> & argv)
{
    /// Use app if the first arg 'app' is passed (the arg should be quietly removed)
    if (argv.size() >= 2)
    {
        auto first_arg = argv.begin() + 1;

        /// 'tiflash --client ...' and 'tiflash client ...' are Ok
        if (*first_arg == "--" + app_suffix || *first_arg == app_suffix)
        {
            argv.erase(first_arg);
            return true;
        }
    }

    /// Use app if tiflash binary is run through symbolic link with name tiflash-app
    std::string app_name = "tiflash-" + app_suffix;
    return !argv.empty() && (app_name == argv[0] || endsWith(argv[0], "/" + app_name));
}

} // namespace


int main(int argc_, char ** argv_)
{
    std::vector<char *> argv(argv_, argv_ + argc_);

    /// Print a basic help if nothing was matched
    MainFunc main_func = printHelp;

    for (auto & application : clickhouse_applications)
    {
        if (isClickhouseApp(application.first, argv))
        {
            main_func = application.second;
            break;
        }
    }

    return main_func(static_cast<int>(argv.size()), argv.data());
}
