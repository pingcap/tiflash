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

#include <Common/Exception.h>
#include <Storages/KVStore/Read/RegionLockInfo.h>
#include <Storages/KVStore/TiKVHelpers/TiKVKeyValue.h>
#include <pingcap/kv/RegionCache.h>

namespace DB
{

namespace ErrorCodes
{
extern const int REGION_LOCKED;
} // namespace ErrorCodes

class LockException : public Exception
{
public:
    explicit LockException(std::vector<std::pair<RegionID, LockInfoPtr>> && locks_)
        : Exception("Key is locked", ErrorCodes::REGION_LOCKED)
        , locks(std::move(locks_))
    {
        std::set<RegionID> locked_regions;
#if SERVERLESS_PROXY == 0
        for (const auto & lock : locks)
            locked_regions.insert(lock.first);

        this->message(fmt::format("Key is locked ({} locks in regions {})", locks.size(), locked_regions));
#else
        std::set<std::string> keys;
        std::set<std::string> primary_keys;
        for (const auto & lock : locks)
        {
            locked_regions.insert(lock.first);
            std::string key(lock.second->key());
            std::string primary_key(lock.second->primary_lock());
            keys.insert(TiKVKey(key.data(), key.size()).toDebugString());
            primary_keys.insert(TiKVKey(primary_key.data(), primary_key.size()).toDebugString());
        }
        this->message(fmt::format(
            "Key is locked ({} locks in regions {} key {} primary {})",
            locks.size(),
            locked_regions,
            keys,
            primary_keys));
#endif
    }

    std::vector<std::pair<RegionID, LockInfoPtr>> locks;
};

} // namespace DB
