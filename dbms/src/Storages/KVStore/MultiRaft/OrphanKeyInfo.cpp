// Copyright 2024 PingCAP, Inc.
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

#include <Storages/KVStore/FFI/ColumnFamily.h>
#include <Storages/KVStore/MultiRaft/RegionData.h>

namespace DB
{
void RegionData::OrphanKeysInfo::observeExtraKey(TiKVKey && key)
{
    remained_keys.insert(std::move(key));
}

bool RegionData::OrphanKeysInfo::observeKeyFromNormalWrite(const TiKVKey & key)
{
    bool res = remained_keys.erase(key);
    if (res)
    {
        // TODO since the check is temporarily disabled, we comment this to avoid extra memory cost.
        // If we erased something, log that.
        // So if we meet this key later due to some unknown replay mechanism, we can know it is a replayed orphan key.
        // removed_remained_keys.insert(TiKVKey::copyFromObj(key));
    }
    return res;
}

bool RegionData::OrphanKeysInfo::containsExtraKey(const TiKVKey & key)
{
    return remained_keys.contains(key);
}

uint64_t RegionData::OrphanKeysInfo::remainedKeyCount() const
{
    return remained_keys.size();
}

void RegionData::OrphanKeysInfo::mergeFrom(const RegionData::OrphanKeysInfo & other)
{
    // TODO support move.
    for (const auto & remained_key : other.remained_keys)
    {
        remained_keys.insert(TiKVKey::copyFrom(remained_key));
    }
}

void RegionData::OrphanKeysInfo::advanceAppliedIndex(uint64_t applied_index)
{
    if (deadline_index && snapshot_index)
    {
        auto count = remainedKeyCount();
        if (applied_index >= deadline_index.value() && count > 0)
        {
            auto one = remained_keys.begin()->toDebugString();
            throw Exception(fmt::format(
                "Orphan keys from snapshot still exists. One of total {} is {}. region_id={} snapshot_index={} "
                "deadline_index={} applied_index={}",
                count,
                one,
                region_id,
                snapshot_index.value(),
                deadline_index.value(),
                applied_index));
        }
    }
}

// Returns true - the orphan key is safe to omit
// Returns false - this is still a hard error
bool RegionData::OrphanKeysInfo::omitOrphanWriteKey(const std::shared_ptr<const TiKVKey> & key)
{
    if (pre_handling)
    {
        RUNTIME_CHECK_MSG(snapshot_index.has_value(), "Snapshot index shall be set when Applying snapshot");
        // While pre-handling snapshot from raftstore v2, we accept and store the orphan keys in memory
        // These keys should be resolved in later raft logs
        observeExtraKey(TiKVKey::copyFrom(*key));
        return true;
    }
    else
    {
        // We can't delete this orphan key here, since it can be triggered from `onSnapshot`.
        if (snapshot_index.has_value())
        {
            if (containsExtraKey(*key))
            {
                return true;
            }
            // We can't throw here, since a PUT write may be replayed while its corresponding default not replayed.
            // TODO Parse some extra data to tell the difference.
            return true;
        }
        else
        {
            // After restart, we will lose all orphan key info. We we can't do orphan key checking for now.
            // So we print out a log here, and neglect the error.
            // TODO We will try to recover the state from cached apply snapshot after restart.
            return true;
        }

        // Otherwise, this is still a hard error.
        // TODO We still need to check if there are remained orphan keys after we have applied after peer's flushed_index.
        // Since the registered orphan write key may come from a raft log smaller than snapshot_index with its default key lost,
        // thus this write key will not be replicated any more, which cause a slient data loss.
    }
    return false;
}
} // namespace DB