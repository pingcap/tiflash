// Copyright 2025 PingCAP, Inc.
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

#include <Common/Logger.h>
#include <Server/Setup.h>
#include <common/config_common.h> // Included for `USE_JEMALLOC`/`USE_MIMALLOC`
#include <common/logger_useful.h>
#include <common/simd.h>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#if USE_MIMALLOC
#include <Poco/JSON/Parser.h>
#include <mimalloc.h>

#include <fstream>
#endif

namespace DB
{

#if USE_MIMALLOC
#define TRY_LOAD_CONF(NAME)                          \
    {                                                \
        try                                          \
        {                                            \
            auto value = obj->getValue<long>(#NAME); \
            mi_option_set(NAME, value);              \
        }                                            \
        catch (...)                                  \
        {}                                           \
    }

void loadMiConfig(Logger * log)
{
    auto config = getenv("MIMALLOC_CONF");
    if (config)
    {
        LOG_INFO(log, "Got environment variable MIMALLOC_CONF: {}", config);
        Poco::JSON::Parser parser;
        std::ifstream data{config};
        Poco::Dynamic::Var result = parser.parse(data);
        auto obj = result.extract<Poco::JSON::Object::Ptr>();
        TRY_LOAD_CONF(mi_option_show_errors);
        TRY_LOAD_CONF(mi_option_show_stats);
        TRY_LOAD_CONF(mi_option_verbose);
        TRY_LOAD_CONF(mi_option_eager_commit);
        TRY_LOAD_CONF(mi_option_eager_region_commit);
        TRY_LOAD_CONF(mi_option_large_os_pages);
        TRY_LOAD_CONF(mi_option_reserve_huge_os_pages);
        TRY_LOAD_CONF(mi_option_segment_cache);
        TRY_LOAD_CONF(mi_option_page_reset);
        TRY_LOAD_CONF(mi_option_segment_reset);
        TRY_LOAD_CONF(mi_option_reset_delay);
        TRY_LOAD_CONF(mi_option_use_numa_nodes);
        TRY_LOAD_CONF(mi_option_reset_decommits);
        TRY_LOAD_CONF(mi_option_eager_commit_delay);
        TRY_LOAD_CONF(mi_option_os_tag);
    }
}
#undef TRY_LOAD_CONF
#endif

void setupAllocator([[maybe_unused]] const LoggerPtr & log)
{
#ifdef RUN_FAIL_RETURN
    static_assert(false);
#endif
#define RUN_FAIL_RETURN(f)                                    \
    do                                                        \
    {                                                         \
        if (f)                                                \
        {                                                     \
            LOG_ERROR(log, "Fail to update jemalloc config"); \
            return;                                           \
        }                                                     \
    } while (0)
#if USE_JEMALLOC
    const char * version;
    bool old_b, new_b = true;
    size_t old_max_thd, new_max_thd = 1;
    size_t sz_b = sizeof(bool), sz_st = sizeof(size_t), sz_ver = sizeof(version);

    RUN_FAIL_RETURN(je_mallctl("version", &version, &sz_ver, nullptr, 0));
    LOG_INFO(log, "Got jemalloc version: {}", version);

    auto * malloc_conf = getenv("MALLOC_CONF");
    if (malloc_conf)
    {
        LOG_INFO(log, "Got environment variable MALLOC_CONF: {}", malloc_conf);
    }
    else
    {
        LOG_INFO(log, "Not found environment variable MALLOC_CONF");
    }

    RUN_FAIL_RETURN(je_mallctl("opt.background_thread", (void *)&old_b, &sz_b, nullptr, 0));
    RUN_FAIL_RETURN(je_mallctl("opt.max_background_threads", (void *)&old_max_thd, &sz_st, nullptr, 0));

    LOG_INFO(log, "Got jemalloc config: opt.background_thread {}, opt.max_background_threads {}", old_b, old_max_thd);

    bool not_config_bg = !malloc_conf || strstr(malloc_conf, "background_thread") == nullptr;
    if (not_config_bg && !old_b)
    {
        // If the user doesn't explicitly set the background_thread opt, and it is actually false, then set it to true.
        LOG_INFO(log, "Try to use background_thread of jemalloc to handle purging asynchronously");

        RUN_FAIL_RETURN(je_mallctl("max_background_threads", nullptr, nullptr, (void *)&new_max_thd, sz_st));
        LOG_INFO(log, "Set jemalloc.max_background_threads {}", new_max_thd);

        RUN_FAIL_RETURN(je_mallctl("background_thread", nullptr, nullptr, (void *)&new_b, sz_b));
        LOG_INFO(log, "Set jemalloc.background_thread {}", new_b);
    }
#endif

#if USE_MIMALLOC
#define MI_OPTION_SHOW(OPTION) LOG_INFO(log, "mimalloc." #OPTION ": {}", mi_option_get(OPTION));

    int version = mi_version();
    LOG_INFO(log, "Got mimalloc version: {}.{}.{}", (version / 100), ((version % 100) / 10), (version % 10));
    loadMiConfig(log);
    MI_OPTION_SHOW(mi_option_show_errors);
    MI_OPTION_SHOW(mi_option_show_stats);
    MI_OPTION_SHOW(mi_option_verbose);
    MI_OPTION_SHOW(mi_option_eager_commit);
    MI_OPTION_SHOW(mi_option_eager_region_commit);
    MI_OPTION_SHOW(mi_option_large_os_pages);
    MI_OPTION_SHOW(mi_option_reserve_huge_os_pages);
    MI_OPTION_SHOW(mi_option_segment_cache);
    MI_OPTION_SHOW(mi_option_page_reset);
    MI_OPTION_SHOW(mi_option_segment_reset);
    MI_OPTION_SHOW(mi_option_reset_delay);
    MI_OPTION_SHOW(mi_option_use_numa_nodes);
    MI_OPTION_SHOW(mi_option_reset_decommits);
    MI_OPTION_SHOW(mi_option_eager_commit_delay);
    MI_OPTION_SHOW(mi_option_os_tag);
#undef MI_OPTION_SHOW
#endif
#undef RUN_FAIL_RETURN
}

[[maybe_unused]] static void tryLoadBoolConfigFromEnv(const DB::LoggerPtr & log, bool & target, const char * name)
{
    auto * config = getenv(name);
    if (config)
    {
        LOG_INFO(log, "Got environment variable {} = {}", name, config);
        try
        {
            auto result = std::stoul(config);
            if (result != 0 && result != 1)
            {
                LOG_ERROR(log, "Environment variable{} = {} is not valid", name, result);
                return;
            }
            target = result;
        }
        catch (...)
        {}
    }
}

void setupSIMD(const LoggerPtr & log)
{
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    tryLoadBoolConfigFromEnv(log, simd_option::ENABLE_AVX, "TIFLASH_ENABLE_AVX");
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
    tryLoadBoolConfigFromEnv(log, simd_option::ENABLE_AVX512, "TIFLASH_ENABLE_AVX512");
#endif

#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
    tryLoadBoolConfigFromEnv(log, simd_option::ENABLE_ASIMD, "TIFLASH_ENABLE_ASIMD");
#endif

#ifdef TIFLASH_ENABLE_SVE_SUPPORT
    tryLoadBoolConfigFromEnv(log, simd_option::ENABLE_SVE, "TIFLASH_ENABLE_SVE");
#endif
}
} // namespace DB
