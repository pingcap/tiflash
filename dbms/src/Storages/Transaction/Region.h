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

/// Store all kv data of one region. Including 'write', 'data' and 'lock' column families.
/// TODO: currently the synchronize mechanism is broken and need to fix.
class Region : public std::enable_shared_from_this<Region>
{
public:
    const static UInt32 CURRENT_VERSION;

    const static String lock_cf_name;
    const static String default_cf_name;
    const static String write_cf_name;
    const static String log_name;

    static const auto PutFlag = RegionData::CFModifyFlag::PutFlag;
    static const auto DelFlag = RegionData::CFModifyFlag::DelFlag;

    class CommittedScanner : private boost::noncopyable
    {
    public:
        CommittedScanner(const RegionPtr & store_, TableID expected_table_id_)
            : store(store_), lock(store_->mutex), expected_table_id(expected_table_id_)
        {
            const auto & data = store->data.writeCF().getData();
            if (auto it = data.find(expected_table_id); it != data.end())
            {
                found = true;
                write_map_it = it->second.begin();
                write_map_it_end = it->second.end();
            }
            else
                found = false;
        }

        bool hasNext() const { return found && write_map_it != write_map_it_end; }

        auto next()
        {
            if (!found)
                throw Exception("CommittedScanner table: " + DB::toString(expected_table_id) + " is not found", ErrorCodes::LOGICAL_ERROR);
            return store->readDataByWriteIt(expected_table_id, write_map_it++);
        }

        LockInfoPtr getLockInfo(UInt64 start_ts) { return store->getLockInfo(expected_table_id, start_ts); }

    private:
        RegionPtr store;
        std::shared_lock<std::shared_mutex> lock;

        bool found;
        TableID expected_table_id;
        RegionData::ConstWriteCFIter write_map_it;
        RegionData::ConstWriteCFIter write_map_it_end;
    };

    class CommittedRemover : private boost::noncopyable
    {
    public:
        CommittedRemover(const RegionPtr & store_, TableID expected_table_id_) : store(store_), lock(store_->mutex)
        {
            auto & data = store->data.writeCFMute().getDataMut();
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
    explicit Region(RegionMeta && meta_) : meta(std::move(meta_)), client(nullptr), log(&Logger::get(log_name)) {}

    explicit Region(const RegionMeta & meta_) : meta(meta_), client(nullptr), log(&Logger::get(log_name)) {}

    explicit Region(RegionMeta && meta_, const RegionClientCreateFunc & region_client_create)
        : meta(std::move(meta_)), client(region_client_create(meta.getRegionVerID())), log(&Logger::get(log_name))
    {}

    explicit Region(const RegionMeta & meta_, const RegionClientCreateFunc & region_client_create)
        : meta(meta_), client(region_client_create(meta.getRegionVerID())), log(&Logger::get(log_name))
    {}

    TableID insert(const std::string & cf, const TiKVKey & key, const TiKVValue & value);
    TableID remove(const std::string & cf, const TiKVKey & key);

    using BatchInsertNode = std::tuple<const TiKVKey *, const TiKVValue *, const String *>;
    void batchInsert(std::function<bool(BatchInsertNode &)> && f);

    std::tuple<std::vector<RegionPtr>, TableIDSet, bool> onCommand(const enginepb::CommandRequest & cmd);

    std::unique_ptr<CommittedScanner> createCommittedScanner(TableID expected_table_id);
    std::unique_ptr<CommittedRemover> createCommittedRemover(TableID expected_table_id);

    size_t serialize(WriteBuffer & buf, enginepb::CommandResponse * response = nullptr) const;
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

    HandleRange<HandleID> getHandleRangeByTable(TableID table_id) const;

    void reset(Region && new_region);

    TableIDSet getCommittedRecordTableID() const;

private:
    // Private methods no need to lock mutex, normally

    TableID doInsert(const String & cf, const TiKVKey & key, const TiKVValue & value);
    TableID doRemove(const String & cf, const TiKVKey & key);

    bool checkIndex(UInt64 index);
    static ColumnFamilyType getCf(const String & cf);

    RegionDataReadInfo readDataByWriteIt(const TableID & table_id, const RegionData::ConstWriteCFIter & write_it) const;
    RegionData::WriteCFIter removeDataByWriteIt(const TableID & table_id, const RegionData::WriteCFIter & write_it);

    LockInfoPtr getLockInfo(TableID expected_table_id, UInt64 start_ts) const;

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
