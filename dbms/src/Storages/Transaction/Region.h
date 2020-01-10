#pragma once

#include <shared_mutex>

#include <Storages/Transaction/IndexReaderCreate.h>
#include <Storages/Transaction/RegionData.h>
#include <Storages/Transaction/RegionMeta.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <common/logger_useful.h>

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;
using Regions = std::vector<RegionPtr>;

struct RaftCommandResult;
class KVStore;
class RegionTable;
class RegionRaftCommandDelegate;
class KVStoreTaskLock;
class Context;

/// Store all kv data of one region. Including 'write', 'data' and 'lock' column families.
/// TODO: currently the synchronize mechanism is broken and need to fix.
class Region : public std::enable_shared_from_this<Region>
{
public:
    const static UInt32 CURRENT_VERSION;

    const static std::string lock_cf_name;
    const static std::string default_cf_name;
    const static std::string write_cf_name;
    const static std::string log_name;

    static const auto PutFlag = CFModifyFlag::PutFlag;
    static const auto DelFlag = CFModifyFlag::DelFlag;

    class CommittedScanner : private boost::noncopyable
    {
    public:
        CommittedScanner(const RegionPtr & store_) : store(store_), lock(store_->mutex)
        {
            const auto & data = store->data.writeCF().getData();

            write_map_size = data.size();
            write_map_it = data.begin();
            write_map_it_end = data.end();
        }

        bool hasNext() const { return write_map_size && write_map_it != write_map_it_end; }

        auto next(bool need_value = true) { return store->readDataByWriteIt(write_map_it++, need_value); }

        LockInfoPtr getLockInfo(UInt64 start_ts) { return store->getLockInfo(start_ts); }

        size_t writeMapSize() const { return write_map_size; }

    private:
        RegionPtr store;
        std::shared_lock<std::shared_mutex> lock;

        size_t write_map_size = 0;
        RegionData::ConstWriteCFIter write_map_it;
        RegionData::ConstWriteCFIter write_map_it_end;
    };

    class CommittedRemover : private boost::noncopyable
    {
    public:
        CommittedRemover(const RegionPtr & store_) : store(store_), lock(store_->mutex) {}

        void remove(const RegionWriteCFData::Key & key)
        {
            auto & write_cf_data = store->data.writeCF().getDataMut();
            if (auto it = write_cf_data.find(key); it != write_cf_data.end())
                store->removeDataByWriteIt(it);
        }

    private:
        RegionPtr store;
        std::unique_lock<std::shared_mutex> lock;
    };

public:
    explicit Region(RegionMeta && meta_);
    explicit Region(RegionMeta && meta_, const IndexReaderCreateFunc & index_reader_create);

    void insert(const std::string & cf, TiKVKey && key, TiKVValue && value);
    void remove(const std::string & cf, const TiKVKey & key);

    CommittedScanner createCommittedScanner();
    CommittedRemover createCommittedRemover();

    std::tuple<size_t, UInt64> serialize(WriteBuffer & buf) const;
    static RegionPtr deserialize(ReadBuffer & buf, const IndexReaderCreateFunc * index_reader_create = nullptr);

    RegionID id() const;
    ImutRegionRangePtr getRange() const;

    enginepb::CommandResponse toCommandResponse() const;
    std::string toString(bool dump_status = true) const;

    bool isPendingRemove() const;
    void setPendingRemove();
    bool isPeerRemoved() const;
    raft_serverpb::PeerState peerState() const;

    size_t dataSize() const;
    size_t writeCFCount() const;
    std::string dataInfo() const;

    void markPersisted() const;
    Timepoint lastPersistTime() const;
    size_t dirtyFlag() const;
    void decDirtyFlag(size_t x) const;
    void incDirtyFlag();

    friend bool operator==(const Region & region1, const Region & region2)
    {
        std::shared_lock<std::shared_mutex> lock1(region1.mutex);
        std::shared_lock<std::shared_mutex> lock2(region2.mutex);

        return region1.meta == region2.meta && region1.data == region2.data;
    }

    UInt64 learnerRead();

    void waitIndex(UInt64 index);

    UInt64 appliedIndex() const;

    RegionVersion version() const;
    RegionVersion confVer() const;

    /// version, conf_version, range
    std::tuple<RegionVersion, RegionVersion, ImutRegionRangePtr> dumpVersionRange() const;

    HandleRange<HandleID> getHandleRangeByTable(TableID table_id) const;

    void assignRegion(Region && new_region);

    using HandleMap = std::unordered_map<HandleID, std::tuple<Timestamp, UInt8>>;

    /// Only can be used for applying snapshot. only can be called by single thread.
    /// Try to fill record with delmark if it exists in ch but has been remove by GC in leader.
    void compareAndCompleteSnapshot(HandleMap & handle_map, const Timestamp safe_point);
    /// Traverse all data in source_region and get handle with largest version.
    void compareAndUpdateHandleMaps(const Region & source_region, HandleMap & handle_map);

    static ColumnFamilyType getCf(const std::string & cf);
    RegionRaftCommandDelegate & makeRaftCommandDelegate(const KVStoreTaskLock &);
    metapb::Region getMetaRegion() const;
    raft_serverpb::MergeState getMergeState() const;

    void tryPreDecodeTiKVValue(Context & context);

    TableID getMappedTableID() const;

private:
    Region() = delete;
    friend class RegionRaftCommandDelegate;

    // Private methods no need to lock mutex, normally

    void doInsert(const std::string & cf, TiKVKey && key, TiKVValue && value);
    void doCheckTable(const DecodedTiKVKey & key) const;
    void doRemove(const std::string & cf, const TiKVKey & key);
    void doDeleteRange(const std::string & cf, const RegionRange & range);

    RegionDataReadInfo readDataByWriteIt(const RegionData::ConstWriteCFIter & write_it, bool need_value = true) const;
    RegionData::WriteCFIter removeDataByWriteIt(const RegionData::WriteCFIter & write_it);

    LockInfoPtr getLockInfo(UInt64 start_ts) const;

    RegionPtr splitInto(RegionMeta && meta);

private:
    RegionData data;
    mutable std::shared_mutex mutex;
    mutable std::mutex predecode_mutex;

    RegionMeta meta;

    IndexReaderPtr index_reader;

    mutable std::atomic<Timepoint> last_persist_time = Clock::now();

    // dirty_flag is used to present whether this region need to be persisted.
    mutable std::atomic<size_t> dirty_flag = 1;

    Logger * log;

    const TableID mapped_table_id;
};

class RegionRaftCommandDelegate : public Region, private boost::noncopyable
{
public:
    /// Only after the task mutex of KVStore is locked, region can apply raft command.
    void onCommand(enginepb::CommandRequest &&, const KVStore &, RegionTable *, RaftCommandResult &);
    const RegionRangeKeys & getRange();
    UInt64 appliedIndex();

private:
    RegionRaftCommandDelegate() = delete;

    Regions execBatchSplit(
        const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term);
    void execChangePeer(
        const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term);
    void execCompactLog(
        const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term);
    void execPrepareMerge(
        const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term);
    RegionID execCommitMerge(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index,
        const UInt64 term, const KVStore & kvstore, RegionTable * region_table);
    void execRollbackMerge(
        const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term);
};

} // namespace DB
