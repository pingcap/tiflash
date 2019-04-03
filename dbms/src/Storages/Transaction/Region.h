#pragma once

#include <functional>
#include <shared_mutex>

#include <Storages/Transaction/RegionData.h>
#include <Storages/Transaction/RegionMeta.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <common/logger_useful.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tikv/RegionClient.h>
#pragma GCC diagnostic pop

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;
using Regions = std::vector<RegionPtr>;

std::pair<HandleID, HandleID> getHandleRangeByTable(const TiKVKey & start_key, const TiKVKey & end_key, TableID table_id);

std::pair<HandleID, HandleID> getHandleRangeByTable(const std::pair<TiKVKey, TiKVKey> & range, TableID table_id);

/// Store all kv data of one region. Including 'write', 'data' and 'lock' column families.
/// TODO: currently the synchronize mechanism is broken and need to fix.
class Region : public std::enable_shared_from_this<Region>
{
public:
    const static UInt32 CURRENT_VERSION;

    const static String lock_cf_name;
    const static String default_cf_name;
    const static String write_cf_name;

    static const auto PutFlag = RegionData::CFModifyFlag::PutFlag;
    static const auto DelFlag = RegionData::CFModifyFlag::DelFlag;

    using LockInfo = RegionData::LockInfo;
    using LockInfoPtr = RegionData::LockInfoPtr;
    using LockInfos = std::vector<LockInfoPtr>;

    class CommittedScanner : private boost::noncopyable
    {
    public:
        CommittedScanner(const RegionPtr & store_, TableID expected_table_id_)
            : store(store_), lock(store_->mutex), expected_table_id(expected_table_id_), write_map_it(store->data.write_cf.map.cbegin())
        {}

        /// Check if next kv exists.
        /// Return InvalidTableID if not.
        TableID hasNext()
        {
            if (expected_table_id != InvalidTableID)
            {
                for (; write_map_it != store->data.write_cf.map.cend(); ++write_map_it)
                {
                    if (likely(std::get<0>(write_map_it->first) == expected_table_id))
                        return expected_table_id;
                }
            }
            else
            {
                if (write_map_it != store->data.write_cf.map.cend())
                    return std::get<0>(write_map_it->first);
            }
            return InvalidTableID;
        }

        auto next(std::vector<RegionWriteCFData::Key> * keys = nullptr) { return store->readDataByWriteIt(write_map_it++, keys); }

        LockInfoPtr getLockInfo(TableID expected_table_id, UInt64 start_ts) { return store->getLockInfo(expected_table_id, start_ts); }

    private:
        RegionPtr store;
        std::shared_lock<std::shared_mutex> lock;

        TableID expected_table_id;
        RegionData::ConstWriteCFIter write_map_it;
    };

    class CommittedRemover : private boost::noncopyable
    {
    public:
        CommittedRemover(const RegionPtr & store_) : store(store_), lock(store_->mutex) {}

        void remove(const RegionWriteCFData::Key & key)
        {
            if (auto it = store->data.write_cf.map.find(key); it != store->data.write_cf.map.end())
                store->removeDataByWriteIt(it);
        }

    private:
        RegionPtr store;
        std::unique_lock<std::shared_mutex> lock;
    };

public:
    explicit Region(RegionMeta && meta_) : meta(std::move(meta_)), client(nullptr), log(&Logger::get("Region")) {}

    explicit Region(const RegionMeta & meta_) : meta(meta_), client(nullptr), log(&Logger::get("Region")) {}

    using RegionClientCreateFunc = std::function<pingcap::kv::RegionClientPtr(pingcap::kv::RegionVerID)>;

    explicit Region(RegionMeta && meta_, const RegionClientCreateFunc & region_client_create)
        : meta(std::move(meta_)), client(region_client_create(meta.getRegionVerID())), log(&Logger::get("Region"))
    {}

    explicit Region(const RegionMeta & meta_, const RegionClientCreateFunc & region_client_create)
        : meta(meta_), client(region_client_create(meta.getRegionVerID())), log(&Logger::get("Region"))
    {}

    TableID insert(const std::string & cf, const TiKVKey & key, const TiKVValue & value);
    TableID remove(const std::string & cf, const TiKVKey & key);

    using BatchInsertNode = std::tuple<const TiKVKey *, const TiKVValue *, const String *>;
    void batchInsert(std::function<bool(BatchInsertNode &)> && f);

    std::tuple<std::vector<RegionPtr>, TableIDSet, bool> onCommand(const enginepb::CommandRequest & cmd);

    std::unique_ptr<CommittedScanner> createCommittedScanner(TableID expected_table_id);
    std::unique_ptr<CommittedRemover> createCommittedRemover();

    size_t serialize(WriteBuffer & buf, enginepb::CommandResponse * response = nullptr);
    static RegionPtr deserialize(ReadBuffer & buf, const RegionClientCreateFunc * region_client_create = nullptr);

    RegionID id() const;
    RegionRange getRange() const;

    enginepb::CommandResponse toCommandResponse() const;
    std::string toString(bool dump_status = true) const;

    bool isPendingRemove() const;
    void setPendingRemove();
    bool isPeerRemoved() const;

    size_t dataSize() const;

    void markPersisted();
    Timepoint lastPersistTime() const;
    size_t persistParm() const;
    void decPersistParm(size_t x);
    void incPersistParm();

    friend bool operator==(const Region & region1, const Region & region2)
    {
        std::shared_lock<std::shared_mutex> lock1(region1.mutex);
        std::shared_lock<std::shared_mutex> lock2(region2.mutex);

        return region1.meta == region2.meta && region1.data == region2.data;
    }

    UInt64 learnerRead();

    void waitIndex(UInt64 index);

    UInt64 getIndex() const;
    UInt64 getProbableIndex() const;

    RegionVersion version() const;
    RegionVersion confVer() const;

    std::pair<HandleID, HandleID> getHandleRangeByTable(TableID table_id) const;

    void reset(Region && new_region);

private:
    // Private methods no need to lock mutex, normally

    TableID doInsert(const String & cf, const TiKVKey & key, const TiKVValue & value);
    TableID doRemove(const String & cf, const TiKVKey & key);

    bool checkIndex(UInt64 index);
    ColumnFamilyType getCf(const String & cf);

    RegionData::ReadInfo readDataByWriteIt(
        const RegionData::ConstWriteCFIter & write_it, std::vector<RegionWriteCFData::Key> * keys = nullptr);
    RegionData::WriteCFIter removeDataByWriteIt(const RegionData::WriteCFIter & write_it);

    LockInfoPtr getLockInfo(TableID expected_table_id, UInt64 start_ts);

    RegionPtr splitInto(const RegionMeta & meta);
    Regions execBatchSplit(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term);
    void execChangePeer(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response, UInt64 index, UInt64 term);

private:
    RegionData data;
    mutable std::shared_mutex mutex;

    RegionMeta meta;

    pingcap::kv::RegionClientPtr client;

    std::atomic<Timepoint> last_persist_time = Clock::now();

    std::atomic<size_t> persist_parm = 1;

    Logger * log;
};

} // namespace DB
