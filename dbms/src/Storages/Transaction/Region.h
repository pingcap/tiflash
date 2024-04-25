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

#include <Storages/Transaction/RegionData.h>
#include <Storages/Transaction/RegionMeta.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <common/logger_useful.h>

#include <shared_mutex>

namespace kvrpcpb
{
class ReadIndexResponse;
class ReadIndexRequest;
} // namespace kvrpcpb

namespace DB
{
namespace tests
{
class RegionKVStoreTest;
}

class Region;
using RegionPtr = std::shared_ptr<Region>;
using Regions = std::vector<RegionPtr>;

struct RaftCommandResult;
class KVStore;
class RegionTable;
class RegionRaftCommandDelegate;
class KVStoreTaskLock;
class Context;
class TMTContext;
struct WriteCmdsView;
enum class EngineStoreApplyRes : uint32_t;
struct SSTViewVec;
struct TiFlashRaftProxyHelper;
class RegionMockTest;
struct ReadIndexResult;
enum class RaftstoreVer : uint8_t;

/// Store all kv data of one region. Including 'write', 'data' and 'lock' column families.
class Region : public std::enable_shared_from_this<Region>
{
public:
    const static UInt32 CURRENT_VERSION;

    static const auto PutFlag = RecordKVFormat::CFModifyFlag::PutFlag;
    static const auto DelFlag = RecordKVFormat::CFModifyFlag::DelFlag;

    class CommittedScanner : private boost::noncopyable
    {
    public:
        explicit CommittedScanner(const RegionPtr & region_, bool use_lock, bool need_value);

        bool hasNext();
        RegionDataReadInfo next();

        DecodedLockCFValuePtr getLockInfo(const RegionLockReadQuery & query) { return region->getLockInfo(query); }

        size_t writeMapSize() const { return write_map_size; }

    private:
        bool tryNext();

    private:
        RegionPtr region;
        std::shared_lock<std::shared_mutex> lock; // A shared_lock so that we can concurrently read committed data.
        std::optional<RegionDataReadInfo> peeked;
        bool need_val;
        // In raftstore v2, snapshot sent to TiFlash may contains extra keys which are from newer raft log entries than the snapshot claimed `snapshot_index`.
        // We treat this as a soft error. Some validations may be performed elsewhere instead of blocking the main process.
        bool hard_error;

        size_t write_map_size = 0;
        RegionData::ConstWriteCFIter write_map_it;
        RegionData::ConstWriteCFIter write_map_it_end;
    };

    class CommittedRemover : private boost::noncopyable
    {
    public:
        explicit CommittedRemover(const RegionPtr & region_, bool use_lock = true)
            : region(region_)
        {
            if (use_lock)
                lock = std::unique_lock<std::shared_mutex>(region_->mutex);
        }

        void remove(const RegionWriteCFData::Key & key)
        {
            auto & write_cf_data = region->data.writeCF().getDataMut();
            if (auto it = write_cf_data.find(key); it != write_cf_data.end())
                region->removeDataByWriteIt(it);
        }

    private:
        RegionPtr region;
        std::unique_lock<std::shared_mutex> lock; // A unique_lock so that we can safely remove committed data.
    };

public:
    explicit Region(RegionMeta && meta_);
    explicit Region(RegionMeta && meta_, const TiFlashRaftProxyHelper *);

    void insert(const std::string & cf, TiKVKey && key, TiKVValue && value, DupCheck mode = DupCheck::Deny);
    void insert(ColumnFamilyType type, TiKVKey && key, TiKVValue && value, DupCheck mode = DupCheck::Deny);
    void remove(const std::string & cf, const TiKVKey & key);

    // Directly drop all data in this Region object.
    void clearAllData();

    CommittedScanner createCommittedScanner(bool use_lock, bool need_value);
    CommittedRemover createCommittedRemover(bool use_lock = true);

    std::tuple<size_t, UInt64> serialize(WriteBuffer & buf) const;
    static RegionPtr deserialize(ReadBuffer & buf, const TiFlashRaftProxyHelper * proxy_helper = nullptr);

    std::string getDebugString() const;
    RegionID id() const;
    ImutRegionRangePtr getRange() const;

    std::string toString(bool dump_status = true) const;

    bool isPendingRemove() const;
    void setPendingRemove();
    raft_serverpb::PeerState peerState() const;

    bool isMerging() const;
    void setStateApplying();

    size_t dataSize() const;
    size_t writeCFCount() const;
    std::string dataInfo() const;

    void markCompactLog() const;
    Timepoint lastCompactLogTime() const;

    friend bool operator==(const Region & region1, const Region & region2)
    {
        std::shared_lock<std::shared_mutex> lock1(region1.mutex);
        std::shared_lock<std::shared_mutex> lock2(region2.mutex);

        return region1.meta == region2.meta && region1.data == region2.data;
    }

    // Check if we can read by this index.
    bool checkIndex(UInt64 index) const;

    // Return <WaitIndexStatus, time cost(seconds)> for wait-index.
    std::tuple<WaitIndexStatus, double> waitIndex(UInt64 index, UInt64 timeout_ms, std::function<bool(void)> && check_running, const LoggerPtr & log);

    // Requires RegionMeta's lock
    UInt64 appliedIndex() const;
    // Requires RegionMeta's lock
    UInt64 appliedIndexTerm() const;

    void notifyApplied() { meta.notifyAll(); }
    // Export for tests.
    void setApplied(UInt64 index, UInt64 term);

    RegionVersion version() const;
    RegionVersion confVer() const;

    RegionMetaSnapshot dumpRegionMetaSnapshot() const;

    // Assign data and meta by moving from `new_region`.
    void assignRegion(Region && new_region);

    void tryCompactionFilter(const Timestamp safe_point);

    RegionRaftCommandDelegate & makeRaftCommandDelegate(const KVStoreTaskLock &);
    metapb::Region cloneMetaRegion() const;
    const metapb::Region & getMetaRegion() const;
    raft_serverpb::MergeState cloneMergeState() const;
    const raft_serverpb::MergeState & getMergeState() const;

    TableID getMappedTableID() const;
    KeyspaceID getKeyspaceID() const;
    EngineStoreApplyRes handleWriteRaftCmd(const WriteCmdsView & cmds, UInt64 index, UInt64 term, TMTContext & tmt);
    void finishIngestSSTByDTFile(RegionPtr && rhs, UInt64 index, UInt64 term);

    UInt64 getSnapshotEventFlag() const { return snapshot_event_flag; }

    /// get approx rows, bytes info about mem cache.
    std::pair<size_t, size_t> getApproxMemCacheInfo() const;
    void cleanApproxMemCacheInfo() const;

    RegionMeta & mutMeta() { return meta; }

    RaftstoreVer getClusterRaftstoreVer();
    void beforePrehandleSnapshot(uint64_t region_id, std::optional<uint64_t> deadline_index);
<<<<<<< HEAD:dbms/src/Storages/Transaction/Region.h
    void afterPrehandleSnapshot();
    RegionData::OrphanKeysInfo & orphanKeysInfo() { return data.orphan_keys_info; }
    const RegionData::OrphanKeysInfo & orphanKeysInfo() const { return data.orphan_keys_info; }
=======
    void afterPrehandleSnapshot(int64_t ongoing);
    void observeLearnerReadEvent(Timestamp read_tso) const;
    Timestamp getLastObservedReadTso() const;
>>>>>>> 89d1d73883 (KVStore: Log when meets any commit_ts < observed max_read_tso (#8991)):dbms/src/Storages/KVStore/Region.h

private:
    Region() = delete;
    friend class RegionRaftCommandDelegate;
    friend class RegionMockTest;
    friend class tests::RegionKVStoreTest;

    // Private methods no need to lock mutex, normally

    void doInsert(ColumnFamilyType type, TiKVKey && key, TiKVValue && value, DupCheck mode = DupCheck::Deny);
    void doRemove(ColumnFamilyType type, const TiKVKey & key);

    std::optional<RegionDataReadInfo> readDataByWriteIt(const RegionData::ConstWriteCFIter & write_it, bool need_value, bool hard_error);
    RegionData::WriteCFIter removeDataByWriteIt(const RegionData::WriteCFIter & write_it);

    DecodedLockCFValuePtr getLockInfo(const RegionLockReadQuery & query) const;

    RegionPtr splitInto(RegionMeta && meta);
    void setPeerState(raft_serverpb::PeerState state);

private:
    RegionData data;
    RegionMeta meta;
    // Modification to data or meta requires this mutex.
    mutable std::shared_mutex mutex;

    LoggerPtr log;

    const TableID mapped_table_id;
    const KeyspaceID keyspace_id;

    std::atomic<UInt64> snapshot_event_flag{1};
    const TiFlashRaftProxyHelper * proxy_helper{nullptr};
    mutable std::atomic<Timepoint> last_compact_log_time{Timepoint::min()};
    mutable std::atomic<size_t> approx_mem_cache_rows{0};
    mutable std::atomic<size_t> approx_mem_cache_bytes{0};
    mutable std::atomic<Timestamp> last_observed_read_tso{0};
};

class RegionRaftCommandDelegate : public Region
    , private boost::noncopyable
{
public:
    /// Only after the task mutex of KVStore is locked, region can apply raft command.
    void handleAdminRaftCmd(const raft_cmdpb::AdminRequest &, const raft_cmdpb::AdminResponse &, UInt64, UInt64, const KVStore &, RegionTable &, RaftCommandResult &);
    const RegionRangeKeys & getRange();
    UInt64 appliedIndex();

    RegionRaftCommandDelegate() = delete;

private:
    friend class tests::RegionKVStoreTest;

    Regions execBatchSplit(
        const raft_cmdpb::AdminRequest & request,
        const raft_cmdpb::AdminResponse & response,
        const UInt64 index,
        const UInt64 term);
    void execChangePeer(
        const raft_cmdpb::AdminRequest & request,
        const raft_cmdpb::AdminResponse & response,
        const UInt64 index,
        const UInt64 term);
    void execPrepareMerge(
        const raft_cmdpb::AdminRequest & request,
        const raft_cmdpb::AdminResponse & response,
        const UInt64 index,
        const UInt64 term);
    RegionID execCommitMerge(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term, const KVStore & kvstore, RegionTable & region_table);
    void execRollbackMerge(
        const raft_cmdpb::AdminRequest & request,
        const raft_cmdpb::AdminResponse & response,
        const UInt64 index,
        const UInt64 term);
};

kvrpcpb::ReadIndexRequest GenRegionReadIndexReq(const Region & region, UInt64 start_ts = 0);

} // namespace DB
