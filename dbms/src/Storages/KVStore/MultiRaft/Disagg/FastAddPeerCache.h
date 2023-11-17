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

#include <Poco/File.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/Config.h>

namespace DB
{
class UniversalPageStorage;
using UniversalPageStoragePtr = std::shared_ptr<UniversalPageStorage>;

// A mapping from segment end key to segment id,
// The main usage:
// auto lock = lock();
// if (isReady(lock))
//     return getSegmentIdContainingKey(lock, key);
// else
//     build(end_key_and_segment_ids)
class EndToSegmentId
{
public:
    [[nodiscard]] std::unique_lock<std::mutex> lock();

    bool isReady(std::unique_lock<std::mutex> & lock) const;

    // The caller must ensure `end_key_and_segment_id` is ordered
    void build(
        std::unique_lock<std::mutex> & lock,
        std::vector<std::pair<DM::RowKeyValue, UInt64>> && end_key_and_segment_ids);

    // Given a key, return the segment_id that may contain the key
    UInt64 getSegmentIdContainingKey(std::unique_lock<std::mutex> & lock, const DM::RowKeyValue & key);

private:
    std::mutex mu;
    bool is_ready = false;

    // Store the mapping from end key to segment id
    // Segment Range End -> Segment ID
    std::vector<std::pair<DM::RowKeyValue, UInt64>> end_to_segment_id;
};
using EndToSegmentIdPtr = std::shared_ptr<EndToSegmentId>;

class ParsedCheckpointDataHolder
{
public:
    ParsedCheckpointDataHolder(Context & context, const PageStorageConfig & config, UInt64 dir_seq);

    UniversalPageStoragePtr getUniversalPageStorage();

    EndToSegmentIdPtr getEndToSegmentIdCache(const KeyspaceTableID & ks_tb_id);

    ~ParsedCheckpointDataHolder()
    {
        for (const auto & path : paths)
        {
            Poco::File(path).remove(true);
        }
    }

private:
    // Paths of this PS.
    std::vector<String> paths = {};

    UniversalPageStoragePtr temp_ps;

    std::mutex mu; // protect segment_end_key_cache
    using EndToSegmentIds = std::unordered_map<KeyspaceTableID, EndToSegmentIdPtr, boost::hash<KeyspaceTableID>>;
    EndToSegmentIds end_to_segment_ids;
};
using ParsedCheckpointDataHolderPtr = std::shared_ptr<ParsedCheckpointDataHolder>;

ParsedCheckpointDataHolderPtr buildParsedCheckpointData(Context & context, const String & manifest_key, UInt64 dir_seq);
} // namespace DB