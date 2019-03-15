#pragma once

#include <functional>
#include <unordered_map>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Storages/Transaction/RegionMeta.h>
#include <Storages/Transaction/TiKVHelper.h>
#include <Storages/Transaction/TiKVKeyValue.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tikv/RegionClient.h>
#pragma GCC diagnostic pop

namespace DB
{

class Region;
using RegionPtr = std::shared_ptr<Region>;
using Regions = std::vector<RegionPtr>;
using HandleRange = std::pair<HandleID, HandleID>;

struct RegionQueryInfo
{
    RegionID region_id;
    UInt64 version;
    HandleRange range_in_table;

    bool operator < (const RegionQueryInfo & o) const
    {
        return range_in_table < o.range_in_table;
    }

    bool operator == (const RegionQueryInfo & o) const
    {
        return range_in_table == o.range_in_table;
    }
};

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

    // In both lock_cf and write_cf.
    enum CFModifyFlag : UInt8
    {
        PutFlag = 'P',
        DelFlag = 'D',
        // useless for TiFLASH
        /*
        LockFlag = 'L',
        // In write_cf, only raft leader will use RollbackFlag in txn mode. Learner should ignore it.
        RollbackFlag = 'R',
        */
    };

    using ComputeHash = std::function<void(const RegionPtr &, UInt64, const std::string)>;
    using VerifyHash = std::function<void(const RegionPtr &, UInt64, const std::string)>;

    // This must be an ordered map. Many logics rely on it, like iterating.
    using KVMap = std::map<TiKVKey, TiKVValue>;

    struct CmdCallBack
    {
        ComputeHash compute_hash;
        VerifyHash verify_hash;
    };

    /// A quick-and-dirty copy of LockInfo structure in kvproto.
    /// Used to transmit to client using non-ProtoBuf protocol.
    struct LockInfo
    {
        std::string primary_lock;
        UInt64 lock_version;
        std::string key;
        UInt64 lock_ttl;
    };
    using LockInfoPtr = std::unique_ptr<LockInfo>;
    using LockInfos = std::vector<LockInfoPtr>;

    class CommittedScanRemover : private boost::noncopyable
    {
    public:
        CommittedScanRemover(const RegionPtr & store_, TableID expected_table_id_)
            : lock(store_->mutex), store(store_), expected_table_id(expected_table_id_), write_map_it(store->write_cf.begin())
        {}

        /// Check if next kv exists.
        /// Return InvalidTableID if not.
        TableID hasNext()
        {
            if (expected_table_id != InvalidTableID)
            {
                for (; write_map_it != store->write_cf.end(); ++write_map_it)
                {
                    if (likely(RecordKVFormat::getTableId(write_map_it->first) == expected_table_id))
                        return expected_table_id;
                }
            }
            else
            {
                if (write_map_it != store->write_cf.end())
                    return RecordKVFormat::getTableId(write_map_it->first);
            }
            return InvalidTableID;
        }

        auto next(std::vector<TiKVKey> * keys = nullptr) { return store->readDataByWriteIt(write_map_it++, keys); }

        void remove(TableID remove_table_id)
        {
            for (auto it = store->write_cf.begin(); it != store->write_cf.end();)
            {
                if (RecordKVFormat::getTableId(it->first) == remove_table_id)
                    it = store->removeDataByWriteIt(it);
                else
                    ++it;
            }
        }

        void remove(const TiKVKey & key)
        {
            if (auto it = store->write_cf.find(key); it != store->write_cf.end())
            {
                store->removeDataByWriteIt(it);
            }
        }

        LockInfoPtr getLockInfo(TableID expected_table_id, UInt64 start_ts) { return store->getLockInfo(expected_table_id, start_ts); }

    private:
        std::lock_guard<std::mutex> lock;

        RegionPtr store;
        TableID expected_table_id;
        KVMap::iterator write_map_it;
    };

public:
    explicit Region(RegionMeta && meta_) : meta(std::move(meta_)), client(nullptr), log(&Logger::get("Region")) {}

    explicit Region(const RegionMeta & meta_) : meta(meta_), client(nullptr), log(&Logger::get("Region")) {}

    explicit Region(RegionMeta && meta_, pingcap::kv::RegionClientPtr client_)
        : meta(std::move(meta_)), client(client_), log(&Logger::get("Region"))
    {}

    explicit Region(const RegionMeta & meta_, const pingcap::kv::RegionClientPtr & client_)
        : meta(meta_), client(client_), log(&Logger::get("Region"))
    {}

    TableID insert(const std::string & cf, const TiKVKey & key, const TiKVValue & value);
    TableID remove(const std::string & cf, const TiKVKey & key);

    using BatchInsertNode = std::tuple<const TiKVKey *, const TiKVValue *, const String *>;
    void batchInsert(std::function<bool(BatchInsertNode &)> f);

    std::tuple<RegionPtr, std::vector<RegionPtr>, TableIDSet, bool> onCommand(const enginepb::CommandRequest & cmd, CmdCallBack & persis);

    std::unique_ptr<CommittedScanRemover> createCommittedScanRemover(TableID expected_table_id);

    size_t serialize(WriteBuffer & buf);
    static RegionPtr deserialize(ReadBuffer & buf);

    void calculateCfCrc32(Crc32 & crc32) const;

    RegionID id() const;
    RegionRange getRange() const;

    enginepb::CommandResponse toCommandResponse() const;
    std::string toString(bool dump_status = true) const;

    bool isPendingRemove() const;
    void setPendingRemove();

    size_t dataSize() const;

    void markPersisted();
    Timepoint lastPersistTime() const;
    size_t persistParm() const;
    void updatePersistParm(size_t x);

    void swap(Region & other)
    {
        std::lock_guard<std::mutex> lock1(mutex);
        std::lock_guard<std::mutex> lock2(other.mutex);

        meta.swap(other.meta);

        data_cf.swap(other.data_cf);
        write_cf.swap(other.write_cf);
        lock_cf.swap(other.lock_cf);

        cf_data_size = size_t(other.cf_data_size);
        other.cf_data_size = size_t(cf_data_size);
    }

    friend bool operator==(const Region & region1, const Region & region2)
    {
        std::lock_guard<std::mutex> lock1(region1.mutex);
        std::lock_guard<std::mutex> lock2(region2.mutex);

        return region1.meta == region2.meta && region1.data_cf == region2.data_cf && region1.write_cf == region2.write_cf
            && region1.lock_cf == region2.lock_cf && region1.cf_data_size == region2.cf_data_size;
    }

    UInt64 learner_read();

    void wait_index(UInt64 index);

    UInt64 getIndex() const;

    RegionVersion version() const;
    RegionVersion conf_ver() const;

    std::pair<HandleID, HandleID> getHandleRangeByTable(TableID table_id) const;

private:
    // Private methods no need to lock mutex, normally

    TableID doInsert(const std::string & cf, const TiKVKey & key, const TiKVValue & value);
    TableID doRemove(const std::string & cf, const TiKVKey & key);

    bool checkIndex(UInt64 index);
    KVMap & getCf(const std::string & cf);

    using ReadInfo = std::tuple<UInt64, UInt8, UInt64, TiKVValue>;
    ReadInfo readDataByWriteIt(const KVMap::iterator & write_it, std::vector<TiKVKey> * keys=nullptr);
    KVMap::iterator removeDataByWriteIt(const KVMap::iterator & write_it);

    LockInfoPtr getLockInfo(TableID expected_table_id, UInt64 start_ts);

    RegionPtr splitInto(const RegionMeta & meta) const;
    std::pair<RegionPtr, Regions> execBatchSplit(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response);
    void execChangePeer(const raft_cmdpb::AdminRequest & request, const raft_cmdpb::AdminResponse & response);

private:
    // TODO: We should later change to lock free structure if needed.
    KVMap data_cf;
    KVMap write_cf;
    KVMap lock_cf;

    mutable std::mutex mutex;

    RegionMeta meta;

    pingcap::kv::RegionClientPtr client;

    // Size of data cf & write cf, without lock cf.
    std::atomic<size_t> cf_data_size = 0;

    std::atomic<Timepoint> last_persist_time = Clock::now();

    std::atomic<size_t> persist_parm = 1;

    Logger * log;
};

} // namespace DB
