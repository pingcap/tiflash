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

#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/PathPool.h>

namespace DB
{

void SharedContextDisagg::initReadNodePageCache(const PathPool & path_pool, const String & cache_dir, size_t cache_capacity)
{
    RUNTIME_CHECK(rn_cache_ps == nullptr && rn_cache == nullptr);

    try
    {
        PSDiskDelegatorPtr delegator;
        if (!cache_dir.empty())
        {
            delegator = path_pool.getPSDiskDelegatorFixedDirectory(cache_dir);
            LOG_INFO(Logger::get(), "Initialize Read Node page cache in cache directory. path={} capacity={}", cache_dir, cache_capacity);
        }
        else
        {
            delegator = path_pool.getPSDiskDelegatorGlobalMulti(PathPool::read_node_cache_path_prefix);
            LOG_INFO(Logger::get(), "Initialize Read Node page cache in data directory. capacity={}", cache_capacity);
        }

        PageStorageConfig config;
        rn_cache_ps = UniversalPageStorageService::create( //
            global_context,
            "read_cache",
            delegator,
            config);
        rn_cache = DM::Remote::RNLocalPageCache::create({
            .underlying_storage = rn_cache_ps->getUniversalPageStorage(),
            .max_size_bytes = cache_capacity,
        });
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }
}

} // namespace DB
