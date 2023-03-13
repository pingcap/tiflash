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

#pragma once

#include <Poco/File.h>
#include <Storages/DeltaMerge/RowKeyRange.h>

namespace DB
{
class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;

struct TempUniversalPageStorage
{
    UniversalPageStoragePtr temp_ps;
    std::vector<String> paths = {};

    ~TempUniversalPageStorage()
    {
        for (const auto & path : paths)
        {
            Poco::File(path).remove(true);
        }
    }
};
using TempUniversalPageStoragePtr = std::shared_ptr<TempUniversalPageStorage>;

struct AsyncTasks;

class FastAddPeerContext
{
public:
    explicit FastAddPeerContext(uint64_t thread_count = 0);

    // return a TempUniversalPageStoragePtr which have sequence >= upload_seq
    TempUniversalPageStoragePtr getTempUniversalPageStorage(UInt64 store_id, UInt64 upload_seq);

    void updateTempUniversalPageStorage(UInt64 store_id, UInt64 upload_seq, TempUniversalPageStoragePtr temp_ps);

    void insertSegmentEndKeyInfoToCache(NamespaceId table_id, const DM::RowKeyValue & key, UInt64 segment_id);

    // return the cached id of the segment which contains the target key
    // return 0 means no cache info for the key
    UInt64 getSegmentIdContainingKey(NamespaceId table_id, const DM::RowKeyValue & key);

    // TODO: invalidate cache at segment level
    void invalidateCache(NamespaceId table_id);

public:
    std::shared_ptr<AsyncTasks> tasks_trace;
    std::atomic<UInt64> temp_ps_dir_sequence;

private:
    std::mutex ps_cache_mu;
    // Store the latest manifest data for every store
    // StoreId -> pair<UploadSeq, TempUniversalPageStoragePtr>
    std::unordered_map<UInt64, std::pair<UInt64, TempUniversalPageStoragePtr>> temp_ps_cache;

    std::mutex range_cache_mu;

    struct KeyComparator : public std::binary_function<DM::RowKeyValue, DM::RowKeyValue, bool>
    {
        bool operator()(const DM::RowKeyValue & key1, const DM::RowKeyValue & key2) const
        {
            return compare(key1.toRowKeyValueRef(), key2.toRowKeyValueRef());
        }
    };

    // Store the mapping from end key to segment id for each table
    // TableId -> (Segment Range End -> Segment ID)
    std::unordered_map<NamespaceId, std::map<DM::RowKeyValue, UInt64, KeyComparator>> segment_range_cache;
};

TempUniversalPageStoragePtr createTempPageStorage(Context & context, const String & manifest_key, UInt64 dir_seq);
} // namespace DB
