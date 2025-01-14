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

#include <Storages/KVStore/Decode/RegionDataRead.h>
#include <Storages/KVStore/MultiRaft/RegionCFDataBase.h>
#include <Storages/KVStore/MultiRaft/RegionCFDataTrait.h>

namespace DB
{
using RegionWriteCFData = RegionCFDataBase<RegionWriteCFDataTrait>;
using RegionDefaultCFData = RegionCFDataBase<RegionDefaultCFDataTrait>;
using RegionLockCFData = RegionCFDataBase<RegionLockCFDataTrait>;

using DecodedLockCFValuePtr = std::shared_ptr<const RecordKVFormat::DecodedLockCFValue>;

enum class ColumnFamilyType : uint8_t;

struct RegionLockReadQuery;
class Region;

class RegionData
{
public:
    using WriteCFIter = RegionWriteCFData::Map::iterator;
    using ConstWriteCFIter = RegionWriteCFData::Map::const_iterator;
    using LockInfoPtr = std::unique_ptr<kvrpcpb::LockInfo>;

    static void reportAlloc(size_t delta);
    static void reportDealloc(size_t delta);
    static void reportDelta(size_t prev, size_t current);
    void recordMemChange(const RegionDataMemDiff & delta);

    RegionDataMemDiff insert(ColumnFamilyType cf, TiKVKey && key, TiKVValue && value, DupCheck mode = DupCheck::Deny);
    void remove(ColumnFamilyType cf, const TiKVKey & key);

    WriteCFIter removeDataByWriteIt(const WriteCFIter & write_it);
    std::optional<RegionDataReadInfo> readDataByWriteIt(
        const ConstWriteCFIter & write_it,
        bool need_value,
        RegionID region_id,
        UInt64 applied,
        bool hard_error);

    LockInfoPtr getLockInfo(const RegionLockReadQuery & query) const;
    std::shared_ptr<const TiKVValue> getLockByKey(const TiKVKey & key) const;

    void splitInto(const RegionRange & range, RegionData & new_region_data);
    void mergeFrom(const RegionData & ori_region_data);

    // Payload size in RegionData, show how much data flows in/out of the Region.
    size_t dataSize() const;
    // `dataSize()` plus the decoded data cached.
    size_t totalSize() const;

    size_t serialize(WriteBuffer & buf) const;
    static void deserialize(ReadBuffer & buf, RegionData & region_data);

    friend bool operator==(const RegionData & r1, const RegionData & r2) { return r1.isEqual(r2); }
    bool isEqual(const RegionData & r2) const;

    RegionWriteCFData & writeCF();
    RegionDefaultCFData & defaultCF();

    const RegionWriteCFData & writeCF() const;
    const RegionDefaultCFData & defaultCF() const;
    const RegionLockCFData & lockCF() const;

    RegionData() = default;
    ~RegionData();

    RegionData(RegionData && data);
    RegionData & operator=(RegionData &&);
    void assignRegionData(RegionData && new_region_data);

    String summary() const;
    struct OrphanKeysInfo
    {
        // Protected by region task lock.
        void observeExtraKey(TiKVKey && key);

        bool observeKeyFromNormalWrite(const TiKVKey & key);

        bool containsExtraKey(const TiKVKey & key);

        uint64_t remainedKeyCount() const;

        void mergeFrom(const OrphanKeysInfo & other);

        void advanceAppliedIndex(uint64_t);

        bool omitOrphanWriteKey(const std::shared_ptr<const TiKVKey> & key);

        // Providing a `snapshot_index` indicates we can scanning a snapshot of index `snapshot_index`.
        // `snapshot_index` can be set to null if TiFlash is not in a raftstore v2 cluster.
        std::optional<uint64_t> snapshot_index;
        // When applied raft log to `deadline_index`, `remained_keys` shall be cleared.
        // Otherwise, raise a hard error.
        std::optional<uint64_t> deadline_index;
        // Marks if we are prehandling a snapshot. We only register orphan key when prehandling.
        // See `RegionData::readDataByWriteIt`.
        bool pre_handling = false;
        uint64_t region_id = 0;
        std::unordered_set<TiKVKey> removed_remained_keys;

    private:
        // Stores orphan write cf keys while handling a raftstore v2 snapshot.
        std::unordered_set<TiKVKey> remained_keys;
    };

private:
    friend class Region;

private:
    RegionWriteCFData write_cf;
    RegionDefaultCFData default_cf;
    RegionLockCFData lock_cf;
    OrphanKeysInfo orphan_keys_info;

    // Size of 3 cfs, reflects size of real palyload flows to KVStore.
    std::atomic<size_t> cf_data_size = 0;
    // Size of decoded structures for convenient access, considered as amplification in memory.
    std::atomic<size_t> decoded_data_size = 0;
};

} // namespace DB
