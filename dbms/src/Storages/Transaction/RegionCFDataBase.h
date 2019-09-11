#pragma once

#include <map>

#include <Storages/Transaction/ExtraCFData.h>

namespace DB
{

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

struct TiKVRangeKey;
using RegionRange = std::pair<TiKVRangeKey, TiKVRangeKey>;

template <typename Trait>
struct RegionCFDataBase
{
    using Key = typename Trait::Key;
    using Value = typename Trait::Value;
    using Map = typename Trait::Map;
    using Data = std::unordered_map<TableID, Map>;
    using Pair = std::pair<Key, Value>;

    static const TiKVKey & getTiKVKey(const Value & val);

    static const TiKVValue & getTiKVValue(const Value & val);

    TableID insert(TiKVKey && key, TiKVValue && value);
    TableID insert(const TableID table_id, std::pair<Key, Value> && kv_pair);
    TableID insert(TiKVKey && key, TiKVValue && value, const DecodedTiKVKey & raw_key);

    static size_t calcTiKVKeyValueSize(const Value & value);

    static size_t calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value);

    size_t remove(TableID table_id, const Key & key, bool quiet = false);

    static bool cmp(const Map & a, const Map & b);

    bool operator==(const RegionCFDataBase & cf) const;

    size_t getSize() const;

    RegionCFDataBase() {}
    RegionCFDataBase(RegionCFDataBase && region);
    RegionCFDataBase & operator=(RegionCFDataBase && region);

    size_t splitInto(const RegionRange & range, RegionCFDataBase & new_region_data);

    size_t serialize(WriteBuffer & buf) const;

    static size_t deserialize(ReadBuffer & buf, RegionCFDataBase & new_region_data);

    const Data & getData() const;

    Data & getDataMut();

    TableIDSet getAllTables() const;

    void deleteRange(const RegionRange & range);

    ExtraCFData<Trait> & getExtra();

private:
    static bool shouldIgnoreInsert(const Value & value);
    static bool shouldIgnoreRemove(const Value & value);

private:
    Data data;
    ExtraCFData<Trait> extra;
};

} // namespace DB
