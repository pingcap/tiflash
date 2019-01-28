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

/// Store all kv data of one region. Including 'write', 'data' and 'lock' column families.
/// TODO: currently the synchronize mechanism is broken and need to fix.
class Region : public std::enable_shared_from_this<Region>
{
public:
    const static String lock_cf_name;
    const static String default_cf_name;
    const static String write_cf_name;

    // In both lock_cf and write_cf.
    enum CFModifyFlag: UInt8
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

        // TODO: 3 times finding is slow: hasNext/next/remove
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

        auto next() { return store->readDataByWriteIt(write_map_it++); }

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

        LockInfoPtr getLockInfo(TableID expected_table_id, UInt64 start_ts)
        {
            return store->getLockInfo(expected_table_id, start_ts);
        }

    private:
        std::lock_guard<std::mutex> lock;

        RegionPtr store;
        TableID expected_table_id;
        KVMap::iterator write_map_it;
    };

public:
    explicit Region(RegionMeta && meta_) : meta(std::move(meta_)), client(nullptr), log(&Logger::get("Region")) {}

    explicit Region(const RegionMeta & meta_) : meta(meta_), client(nullptr), log(&Logger::get("Region")) {}

    explicit Region(RegionMeta && meta_, pingcap::kv::RegionClientPtr client_) : meta(std::move(meta_)), client(client_), log(&Logger::get("Region")) {}

    explicit Region(const RegionMeta & meta_, const pingcap::kv::RegionClientPtr & client_) : meta(meta_), client(client_), log(&Logger::get("Region")) {}

    void insert(const std::string & cf, const TiKVKey & key, const TiKVValue & value);
    void remove(const std::string & cf, const TiKVKey & key);

    std::tuple<RegionPtr, std::vector<RegionPtr>, bool> onCommand(const enginepb::CommandRequest & cmd, CmdCallBack & persis);

    std::unique_ptr<CommittedScanRemover> createCommittedScanRemover(TableID expected_table_id);

    size_t serialize(WriteBuffer & buf);
    static RegionPtr deserialize(ReadBuffer & buf);

    void calculateCfCrc32(Crc32 & crc32) const;

    void markPersisted();

    RegionID id() const;
    RegionRange getRange() const;

    enginepb::CommandResponse toCommandResponse() const;
    std::string toString(bool dump_status = true) const;

    bool isPendingRemove() const;
    void setPendingRemove();

    const Poco::Timestamp & lastPersistTime() const;

    size_t getNewlyAddedRows() const;
    void resetNewlyAddedRows();

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

        newly_added_rows = size_t (other.newly_added_rows);
        other.newly_added_rows = size_t (newly_added_rows);
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

    UInt64 getIndex();

private:
    // Private methods no need to lock mutex, normally

    void doInsert(const std::string & cf, const TiKVKey & key, const TiKVValue & value);
    void doRemove(const std::string & cf, const TiKVKey & key);

    bool checkIndex(UInt64 index);
    KVMap & getCf(const std::string & cf);

    using ReadInfo = std::tuple<UInt64, UInt8, UInt64, TiKVValue>;
    ReadInfo readDataByWriteIt(const KVMap::iterator & write_it);
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

    // Protect CFs only
    mutable std::mutex mutex;

    std::condition_variable cv;

    // Vars below are thread safe

    // The meta_mutex can protect a group operating on meta, during the operating time all reading to meta can be blocked.
    // Since only one thread (onCommand) would modify meta for now, we remove meta_mutex.
    // mutable std::mutex meta_mutex;
    RegionMeta meta;

    pingcap::kv::RegionClientPtr client;

    // These two vars are not exactly correct, because they are not protected by mutex when region splitted
    std::atomic<size_t> cf_data_size = 0;
    std::atomic<size_t> newly_added_rows = 0;

    // Poco::timestamp is not trivially copyable, use a extra mutex for this member
    Poco::Timestamp last_persist_time{0};
    mutable std::mutex persist_time_mutex;

    Logger * log;
};

} // namespace DB
