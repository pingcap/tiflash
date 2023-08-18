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

#include <Storages/Transaction/FastAddPeerCache.h>

namespace DB
{
struct AsyncTasks;

class FastAddPeerContext
{
public:
    explicit FastAddPeerContext(uint64_t thread_count = 0);

    // Return parsed checkpoint data and its corresponding seq which is newer than `required_seq` if exists, otherwise return pair<required_seq, nullptr>
    std::pair<UInt64, ParsedCheckpointDataHolderPtr> getNewerCheckpointData(Context & context, UInt64 store_id, UInt64 required_seq);

public:
    std::shared_ptr<AsyncTasks> tasks_trace;

private:
    class CheckpointCacheElement
    {
    public:
        explicit CheckpointCacheElement(const String & manifest_key_, UInt64 dir_seq_)
            : manifest_key(manifest_key_)
            , dir_seq(dir_seq_)
        {}

        ParsedCheckpointDataHolderPtr getParsedCheckpointData(Context & context);

    private:
        std::mutex mu;
        String manifest_key;
        UInt64 dir_seq;
        ParsedCheckpointDataHolderPtr parsed_checkpoint_data;
    };
    using CheckpointCacheElementPtr = std::shared_ptr<CheckpointCacheElement>;

    std::atomic<UInt64> temp_ps_dir_sequence = 0;

    std::mutex cache_mu;
    // Store the latest manifest data for every store
    // StoreId -> std::pair<UploadSeq, CheckpointCacheElementPtr>
    std::unordered_map<UInt64, std::pair<UInt64, CheckpointCacheElementPtr>> checkpoint_cache_map;

    LoggerPtr log;
};
} // namespace DB
