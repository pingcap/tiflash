#pragma once

#include <shared_mutex>

#include <Storages/Transaction/RegionClientCreate.h>
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
        CommittedScanner(const RegionPtr & store_, TableID expected_table_id_)
            : store(store_), lock(store_->mutex), expected_table_id(expected_table_id_)
        {
            const auto & data = store->data.writeCF().getData();
            if (auto it = data.find(expected_table_id); it != data.end())
            {
                write_map_size = it->second.size();
                write_map_it = it->second.begin();
                write_map_it_end = it->second.end();
            }
        }

        bool hasNext() const { return write_map_size && write_map_it != write_map_it_end; }

        auto next(bool need_value = true) { return store->readDataByWriteIt(expected_table_id, write_map_it++, need_value); }

        LockInfoPtr getLockInfo(UInt64 start_ts) { return store->getLockInfo(expected_table_id, start_ts); }

        size_t writeMapSize() const { return write_map_size; }

    private:
        RegionPtr store;
        std::shared_lock<std::shared_mutex> lock;

        size_t write_map_size = 0;
        TableID expected_table_id;
        RegionData::ConstWriteCFIter write_map_it;
        RegionData::ConstWriteCFIter write_map_it_end;
    };

    class CommittedRemover : private boost::noncopyable
    {
    public:
        CommittedRemover(const RegionPtr & store_, TableID expected_table_id_) : store(store_), lock(store_->mutex)
        {
            auto & data = store->data.writeCF().getDataMut();
            write_cf_data_it = data.find(expected_table_id_);
            found = write_cf_data_it != data.end();
        }

        void remove(const RegionWriteCFData::Key & key)
        {
            if (!found)
                return;
            if (auto it = write_cf_data_it->second.find(key); it != write_cf_data_it->second.end())
                store->removeDataByWriteIt(write_cf_data_it->first, it);
        }

    private:
        RegionPtr store;
        std::unique_lock<std::shared_mutex> lock;

        bool found;
        RegionWriteCFData::Data::iterator write_cf_data_it;
    };

public:
    explicit Region(RegionMeta meta_) : meta(std::move(meta_)), client(nullptr), log(&Logger::get(log_name)) {}

    explicit Region(RegionMeta meta_, const RegionClientCreateFunc & region_client_create)
        : meta(std::move(meta_)), client(region_client_create(meta.getRegionVerID())), log(&Logger::get(log_name))
    {}

    TableID insert(const std::string & cf, TiKVKey && key, TiKVValue && value);
    TableID remove(const std::string & cf, const TiKVKey & key);

    CommittedScanner createCommittedScanner(TableID expected_table_id);
    CommittedRemover createCommittedRemover(TableID expected_table_id);

    std::tuple<size_t, UInt64> serialize(WriteBuffer & buf) const;
    static RegionPtr deserialize(ReadBuffer & buf, const RegionClientCreateFunc * region_client_create = nullptr);

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

    TableIDSet getAllWriteCFTables() const;

    using HandleMap = std::unordered_map<HandleID, std::tuple<Timestamp, UInt8>>;

    /// only can be used for applying snapshot. only can be called by single thread.
    void compareAndCompleteSnapshot(HandleMap & handle_map, const TableID table_id, const Timestamp safe_point);
    void compareAndCompleteSnapshot(const Timestamp safe_point, const Region & source_region);

    static ColumnFamilyType getCf(const std::string & cf);
    RegionRaftCommandDelegate & makeRaftCommandDelegate(const KVStoreTaskLock &);

    void tryPreDecodeTiKVValue();

private:
    Region() = delete;
    friend class RegionRaftCommandDelegate;

    // Private methods no need to lock mutex, normally

    TableID doInsert(const std::string & cf, TiKVKey && key, TiKVValue && value);
    TableID doRemove(const std::string & cf, const TiKVKey & key);
    void doDeleteRange(const std::string & cf, const TiKVKey & start_key, const TiKVKey & end_key);

    RegionDataReadInfo readDataByWriteIt(
        const TableID & table_id, const RegionData::ConstWriteCFIter & write_it, bool need_value = true) const;
    RegionData::WriteCFIter removeDataByWriteIt(const TableID & table_id, const RegionData::WriteCFIter & write_it);

    LockInfoPtr getLockInfo(TableID expected_table_id, UInt64 start_ts) const;

    RegionPtr splitInto(RegionMeta meta);

private:
    RegionData data;
    mutable std::shared_mutex mutex;

    RegionMeta meta;

    pingcap::kv::RegionClientPtr client;

    mutable std::atomic<Timepoint> last_persist_time = Clock::now();

    // dirty_flag is used to present whether this region need to be persisted.
    mutable std::atomic<size_t> dirty_flag = 1;

    Logger * log;
};

class RegionRaftCommandDelegate : public Region, private boost::noncopyable
{
public:
    /// Only after the task mutex of KVStore is locked, region can apply raft command.
    void onCommand(enginepb::CommandRequest &&, const KVStore &, RegionTable *, RaftCommandResult &);

private:
    RegionRaftCommandDelegate() = delete;

    Regions execBatchSplit(
        const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term);
    void execChangePeer(
        const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term);
    void execCompactLog(
        const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, const UInt64 index, const UInt64 term);
};

} // namespace DB
