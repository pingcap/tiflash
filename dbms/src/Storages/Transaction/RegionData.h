#pragma once

#include <Storages/Transaction/RegionCFDataBase.h>
#include <Storages/Transaction/RegionCFDataTrait.h>
#include <Storages/Transaction/RegionDataRead.h>
#include <Storages/Transaction/RegionLockInfo.h>

namespace DB
{

enum ColumnFamilyType
{
    Write,
    Default,
    Lock,
};

using RegionWriteCFData = RegionCFDataBase<RegionWriteCFDataTrait>;
using RegionDefaultCFData = RegionCFDataBase<RegionDefaultCFDataTrait>;
using RegionLockCFData = RegionCFDataBase<RegionLockCFDataTrait>;

class RegionData
{
public:
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

    using WriteCFIter = RegionWriteCFData::Map::iterator;
    using ConstWriteCFIter = RegionWriteCFData::Map::const_iterator;

    TableID insert(ColumnFamilyType cf, const TiKVKey & key, const String & raw_key, const TiKVValue & value);

    TableID removeLockCF(const TableID & table_id, const String & raw_key);

    WriteCFIter removeDataByWriteIt(const TableID & table_id, const WriteCFIter & write_it);

    RegionDataReadInfo readDataByWriteIt(const TableID & table_id, const ConstWriteCFIter & write_it) const;

    LockInfoPtr getLockInfo(TableID expected_table_id, Timestamp start_ts) const;

    void splitInto(const RegionRange & range, RegionData & new_region_data);

    size_t dataSize() const;

    void reset(RegionData && new_region_data);

    size_t serialize(WriteBuffer & buf) const;

    static void deserialize(ReadBuffer & buf, RegionData & region_data);

    friend bool operator==(const RegionData & r1, const RegionData & r2) { return r1.isEqual(r2); }

    bool isEqual(const RegionData & r2) const;

    RegionWriteCFData & writeCFMute();

    const RegionWriteCFData & writeCF() const;

    TableIDSet getCommittedRecordTableID() const;

    RegionData() {}

    RegionData(RegionData && data);

private:
    RegionWriteCFData write_cf;
    RegionDefaultCFData default_cf;
    RegionLockCFData lock_cf;

    // Size of data cf & write cf, without lock cf.
    std::atomic<size_t> cf_data_size = 0;
};

} // namespace DB
