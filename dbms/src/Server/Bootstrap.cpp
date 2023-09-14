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

#include <Common/Exception.h>
#include <Common/RedactHelpers.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <raft_serverpb.pb.h>

namespace DB
{

std::optional<raft_serverpb::StoreIdent> tryGetStoreIdentFromKey(
    const UniversalPageStoragePtr & wn_ps,
    const String & key)
{
    raft_serverpb::StoreIdent store_ident;
    auto page = wn_ps->read(key, nullptr, {}, /*throw_on_not_exist*/ false);
    if (!page.isValid())
    {
        return std::nullopt;
    }
    bool ok = store_ident.ParseFromString(String(page.data));
    RUNTIME_ASSERT(
        ok,
        "Failed to parse store ident, key={} data={}",
        Redact::keyToHexString(key.data(), key.size()),
        Redact::keyToHexString(page.data.data(), page.data.size()));
    RUNTIME_ASSERT(
        store_ident.store_id() != InvalidStoreID,
        "Get invalid store_id from store ident, key={} store_ident={{{}}}",
        Redact::keyToDebugString(key.data(), key.size()),
        store_ident.ShortDebugString());
    return store_ident;
}

std::optional<raft_serverpb::StoreIdent> tryGetStoreIdent(const UniversalPageStoragePtr & wn_ps)
{
    // First try to get from raft engine
    auto store_ident = tryGetStoreIdentFromKey(wn_ps, UniversalPageIdFormat::getStoreIdentId());
    if (store_ident)
        return store_ident;
    // raft engine not exist, try find in kv engine
    return tryGetStoreIdentFromKey(wn_ps, UniversalPageIdFormat::getStoreIdentIdInKVEngine());
}

} // namespace DB
