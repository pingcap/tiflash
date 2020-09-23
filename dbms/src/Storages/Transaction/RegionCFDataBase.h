#pragma once

#include <Storages/Transaction/CFDataPreDecode.h>

#include <map>

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
using RegionDataRes = size_t;

template <typename Trait>
struct RegionCFDataBase
{
    using Key = typename Trait::Key;
    using Value = typename Trait::Value;
    using Map = typename Trait::Map;
    using Data = Map;
    using Pair = std::pair<Key, Value>;
    using Status = bool;

    static const TiKVKey & getTiKVKey(const Value & val);

    static const TiKVValue & getTiKVValue(const Value & val);

    RegionDataRes insert(TiKVKey && key, TiKVValue && value);

    static size_t calcTiKVKeyValueSize(const Value & value);

    static size_t calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value);

    void finishInsert(typename Map::iterator);

    size_t remove(const Key & key, bool quiet = false);

    static bool cmp(const Map & a, const Map & b);

    bool operator==(const RegionCFDataBase & cf) const;

    size_t getSize() const;

    RegionCFDataBase() {}
    RegionCFDataBase(RegionCFDataBase && region);
    RegionCFDataBase & operator=(RegionCFDataBase && region);

    size_t splitInto(const RegionRange & range, RegionCFDataBase & new_region_data);
    size_t mergeFrom(const RegionCFDataBase & ori_region_data);

    size_t serialize(WriteBuffer & buf) const;

    static size_t deserialize(ReadBuffer & buf, RegionCFDataBase & new_region_data);

    const Data & getData() const;

    Data & getDataMut();

    CFDataPreDecode<Trait> & getCFDataPreDecode();

private:
    static bool shouldIgnoreInsert(const Value & value);
    static bool shouldIgnoreRemove(const Value & value);
    RegionDataRes insert(std::pair<Key, Value> && kv_pair);
    RegionDataRes insert(TiKVKey && key, TiKVValue && value, const DecodedTiKVKey & raw_key);

private:
    Data data;
    CFDataPreDecode<Trait> pre_decode;
};

} // namespace DB
