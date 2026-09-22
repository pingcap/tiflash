// Copyright 2026 PingCAP, Inc.
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
#include <common/types.h>

#include <memory>
#include <mutex>
#include <optional>

namespace DB
{
enum class HashTableSizeKind : UInt8
{
    DistinctKeyCount,
    BuildRowCount,
};

/// Statistics for one hash table owned by a physical hash-table operator.
struct HashTableStats
{
    /// Join V1 reports distinct hash entries, while Join V2 reports build-side row count because its
    /// pointer table does not maintain a distinct-key count. This is the current by-design behavior.
    UInt64 size = 0;
    HashTableSizeKind size_kind = HashTableSizeKind::DistinctKeyCount;
    UInt64 memory_bytes = 0;

    void merge(const HashTableStats & other)
    {
        RUNTIME_CHECK(size_kind == other.size_kind);
        size += other.size;
        memory_bytes += other.memory_bytes;
    }
};

/// Thread-safe accumulator shared by all runtime fragments of one physical hash-table operator.
class HashTableStatsProfileInfo
{
public:
    void mergeHashTableStats(const HashTableStats & stats)
    {
        std::lock_guard lock(hash_table_stats_mutex);
        if (!hash_table_stats)
            hash_table_stats = stats;
        else
            hash_table_stats->merge(stats);
    }

    std::optional<HashTableStats> getHashTableStats() const
    {
        std::lock_guard lock(hash_table_stats_mutex);
        return hash_table_stats;
    }

private:
    mutable std::mutex hash_table_stats_mutex;
    std::optional<HashTableStats> hash_table_stats;
};
using HashTableStatsProfileInfoPtr = std::shared_ptr<HashTableStatsProfileInfo>;
} // namespace DB
