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

#include <Common/TiFlashMetrics.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/KVStore/FFI/ColumnFamily.h>
#include <Storages/KVStore/MultiRaft/RegionData.h>
#include <Storages/KVStore/Read/RegionLockInfo.h>
#include <Storages/KVStore/TiKVHelpers/DecodedLockCFValue.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLFORMAT_RAFT_ROW;
} // namespace ErrorCodes

void RegionData::recordMemChange(const RegionDataMemDiff & delta)
{
    if (delta.payload > 0)
    {
        root_of_kvstore_mem_trackers->alloc(delta.payload, false);
    }
    else if (delta.payload < 0)
    {
        root_of_kvstore_mem_trackers->free(-delta.payload);
    }
}

void RegionData::updateMemoryUsage(const RegionDataMemDiff & delta)
{
    cf_data_size += delta.payload;
    decoded_data_size += delta.decoded;
}

void RegionData::resetMemoryUsage()
{
    cf_data_size = 0;
    decoded_data_size = 0;
}

RegionDataMemDiff RegionData::insert(ColumnFamilyType cf, TiKVKey && key, TiKVValue && value, DupCheck mode)
{
    RegionDataMemDiff delta;
    switch (cf)
    {
    case ColumnFamilyType::Write:
    {
        delta = write_cf.insert(std::move(key), std::move(value), mode);
        break;
    }
    case ColumnFamilyType::Default:
    {
        delta = default_cf.insert(std::move(key), std::move(value), mode);
        break;
    }
    case ColumnFamilyType::Lock:
    {
        // By inserting a lock, a old lock of the same key could be replaced. For example, pessimistic lock -> optimistic lock.
        delta = lock_cf.insert(std::move(key), std::move(value), mode);
        break;
    }
    }
    recordMemChange(delta);
    updateMemoryUsage(delta);
    return delta;
}

void RegionData::remove(ColumnFamilyType cf, const TiKVKey & key)
{
    RegionDataMemDiff delta;
    switch (cf)
    {
    case ColumnFamilyType::Write:
    {
        auto raw_key = RecordKVFormat::decodeTiKVKey(key);
        auto pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        // removed by gc, may not exist.
        delta = write_cf.remove(RegionWriteCFData::Key{pk, ts}, true);
        break;
    }
    case ColumnFamilyType::Default:
    {
        auto raw_key = RecordKVFormat::decodeTiKVKey(key);
        auto pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        // removed by gc, may not exist.
        delta = default_cf.remove(RegionDefaultCFData::Key{pk, ts}, true);
        break;
    }
    case ColumnFamilyType::Lock:
    {
        delta = lock_cf.remove(RegionLockCFDataTrait::Key{nullptr, std::string_view(key.data(), key.dataSize())}, true);
        break;
    }
    }
    recordMemChange(delta);
    updateMemoryUsage(delta);
}

RegionData::WriteCFIter RegionData::removeDataByWriteIt(const WriteCFIter & write_it)
{
    const auto & [key, value, decoded_val] = write_it->second;
    const auto & [pk, ts] = write_it->first;

    std::ignore = ts;
    std::ignore = value;
    std::ignore = key;

    RegionDataMemDiff delta;

    if (decoded_val.write_type == RecordKVFormat::CFModifyFlag::PutFlag)
    {
        auto & map = default_cf.getDataMut();

        if (auto data_it = map.find({pk, decoded_val.prewrite_ts}); data_it != map.end())
        {
            delta.sub(RegionDefaultCFData::calcTotalKVSize(data_it->second));
            map.erase(data_it);
        }
    }

    delta.sub(RegionWriteCFData::calcTotalKVSize(write_it->second));
    recordMemChange(delta);
    updateMemoryUsage(delta);

    return write_cf.getDataMut().erase(write_it);
}

/// This function is called by `ReadRegionCommitCache`.
std::optional<RegionDataReadInfo> RegionData::readDataByWriteIt(
    const ConstWriteCFIter & write_it,
    bool need_value,
    RegionID region_id,
    UInt64 applied,
    bool hard_error)
{
    const auto & [key, value, decoded_val] = write_it->second;
    const auto & [pk, ts] = write_it->first;

    std::ignore = value;
    if (pk->empty())
    {
        throw Exception("Observe empty PK: raw key " + key->toDebugString(), ErrorCodes::ILLFORMAT_RAFT_ROW);
    }

    if (!need_value)
        return RegionDataReadInfo{pk, decoded_val.write_type, ts, nullptr};

    if (decoded_val.write_type != RecordKVFormat::CFModifyFlag::PutFlag)
        return RegionDataReadInfo{pk, decoded_val.write_type, ts, nullptr};

    std::string orphan_key_debug_msg;
    if (!decoded_val.short_value)
    {
        const auto & map = default_cf.getData();
        if (auto data_it = map.find({pk, decoded_val.prewrite_ts}); data_it != map.end())
            return RegionDataReadInfo{pk, decoded_val.write_type, ts, RegionDefaultCFDataTrait::getTiKVValue(data_it)};
        else
        {
            if (!hard_error)
            {
                if (orphan_keys_info.omitOrphanWriteKey(key))
                {
                    return std::nullopt;
                }
                orphan_key_debug_msg = fmt::format(
                    "orphan_info: ({}, snapshot_index: {}, {}, orphan key size {})",
                    hard_error ? "" : ", not orphan key",
                    orphan_keys_info.snapshot_index.has_value()
                        ? std::to_string(orphan_keys_info.snapshot_index.value())
                        : "",
                    orphan_keys_info.removed_remained_keys.contains(*key) ? "duplicated write" : "missing default",
                    orphan_keys_info.remainedKeyCount());
            }
            throw Exception(
                fmt::format(
                    "Raw TiDB PK: {}, Prewrite ts: {} can not found in default cf for key: {}, region_id: {}, "
                    "applied_index: {}{}",
                    pk.toDebugString(),
                    decoded_val.prewrite_ts,
                    key->toDebugString(),
                    region_id,
                    applied,
                    orphan_key_debug_msg),
                ErrorCodes::ILLFORMAT_RAFT_ROW);
        }
    }

    return RegionDataReadInfo{pk, decoded_val.write_type, ts, decoded_val.short_value};
}

LockInfoPtr RegionData::getLockInfo(const RegionLockReadQuery & query) const
{
    for (const auto & [pk, value] : lock_cf.getData())
    {
        std::ignore = pk;

        const auto & [tikv_key, tikv_val, lock_info_ptr] = value;
        std::ignore = tikv_key;
        std::ignore = tikv_val;
        const auto & lock_info_raw = *lock_info_ptr;

        if (auto t = lock_info_raw.getLockInfoPtr(query); t != nullptr)
        {
            return t;
        }
    }

    return nullptr;
}

std::shared_ptr<const TiKVValue> RegionData::getLockByKey(const TiKVKey & key) const
{
    const auto & map = lock_cf.getData();
    const auto & lock_key = RegionLockCFDataTrait::Key{nullptr, std::string_view(key.data(), key.dataSize())};
    if (auto lock_it = map.find(lock_key); lock_it != map.end())
    {
        const auto & [tikv_key, tikv_val, lock_info_ptr] = lock_it->second;
        std::ignore = tikv_key;
        std::ignore = lock_info_ptr;
        return tikv_val;
    }

    // It is safe to ignore the missing lock key after restart, print a warning log and return nullptr
    LOG_WARNING(
        Logger::get(),
        "Failed to get lock by key in region data, key={} map_size={} count={}",
        key.toDebugString(),
        map.size(),
        map.count(lock_key));
    return nullptr;
}

void RegionData::splitInto(const RegionRange & range, RegionData & new_region_data)
{
    RegionDataMemDiff size_changed;
    size_changed.add(default_cf.splitInto(range, new_region_data.default_cf));
    size_changed.add(write_cf.splitInto(range, new_region_data.write_cf));
    // recordMemChange: Remember to track memory here if we have a region-wise metrics later.
    size_changed.add(lock_cf.splitInto(range, new_region_data.lock_cf));
    updateMemoryUsage(size_changed);
    new_region_data.updateMemoryUsage(size_changed.negative());
}

void RegionData::mergeFrom(const RegionData & ori_region_data)
{
    RegionDataMemDiff size_changed;
    size_changed.add(default_cf.mergeFrom(ori_region_data.default_cf));
    size_changed.add(write_cf.mergeFrom(ori_region_data.write_cf));
    // recordMemChange: Remember to track memory here if we have a region-wise metrics later.
    size_changed.add(lock_cf.mergeFrom(ori_region_data.lock_cf));
    updateMemoryUsage(size_changed);
    // `mergeFrom` won't delete from source region. So we don't update it here.
}

size_t RegionData::dataSize() const
{
    return cf_data_size;
}

size_t RegionData::totalSize() const
{
    return cf_data_size + decoded_data_size;
}

void RegionData::assignRegionData(RegionData && rhs)
{
    recordMemChange(RegionDataMemDiff{-cf_data_size.load(), -decoded_data_size.load()});
    resetMemoryUsage();

    default_cf = std::move(rhs.default_cf);
    write_cf = std::move(rhs.write_cf);
    lock_cf = std::move(rhs.lock_cf);
    orphan_keys_info = std::move(rhs.orphan_keys_info);

    updateMemoryUsage(RegionDataMemDiff{rhs.cf_data_size.load(), rhs.decoded_data_size.load()});
    rhs.resetMemoryUsage();
}

size_t RegionData::serialize(WriteBuffer & buf) const
{
    size_t total_size = 0;

    total_size += default_cf.serialize(buf);
    total_size += write_cf.serialize(buf);
    total_size += lock_cf.serialize(buf);

    return total_size;
}

void RegionData::deserialize(ReadBuffer & buf, RegionData & region_data)
{
    RegionDataMemDiff size_changed;

    size_changed.add(RegionDefaultCFData::deserialize(buf, region_data.default_cf));
    size_changed.add(RegionWriteCFData::deserialize(buf, region_data.write_cf));
    size_changed.add(RegionLockCFData::deserialize(buf, region_data.lock_cf));

    region_data.updateMemoryUsage(size_changed);
    region_data.recordMemChange(size_changed);
}

RegionWriteCFData & RegionData::writeCF()
{
    return write_cf;
}

RegionDefaultCFData & RegionData::defaultCF()
{
    return default_cf;
}

const RegionWriteCFData & RegionData::writeCF() const
{
    return write_cf;
}

const RegionDefaultCFData & RegionData::defaultCF() const
{
    return default_cf;
}

const RegionLockCFData & RegionData::lockCF() const
{
    return lock_cf;
}

bool RegionData::isEqual(const RegionData & r2) const
{
    return default_cf == r2.default_cf && write_cf == r2.write_cf && lock_cf == r2.lock_cf
        && cf_data_size == r2.cf_data_size;
}

RegionData::RegionData(RegionData && data) noexcept
    : write_cf(std::move(data.write_cf))
    , default_cf(std::move(data.default_cf))
    , lock_cf(std::move(data.lock_cf))
    , cf_data_size(data.cf_data_size.load())
    , decoded_data_size(data.decoded_data_size.load())
{}

RegionData::~RegionData()
{
    recordMemChange(RegionDataMemDiff{-cf_data_size, 0});
    updateMemoryUsage(RegionDataMemDiff{-cf_data_size, 0});
}

String RegionData::summary() const
{
    return fmt::format("write:{},lock:{},default:{}", write_cf.getSize(), lock_cf.getSize(), default_cf.getSize());
}

size_t RegionData::tryCompactionFilter(Timestamp safe_point)
{
    RegionDataMemDiff delta;
    size_t del_write = 0;
    auto & write_map = write_cf.getDataMut();
    auto & default_map = default_cf.getDataMut();
    for (auto write_map_it = write_map.begin(); write_map_it != write_map.end();)
    {
        const auto & decoded_val = std::get<2>(write_map_it->second);
        const auto & [pk, ts] = write_map_it->first;

        if (decoded_val.write_type == RecordKVFormat::CFModifyFlag::PutFlag)
        {
            if (!decoded_val.short_value)
            {
                if (auto data_it = default_map.find({pk, decoded_val.prewrite_ts}); data_it == default_map.end())
                {
                    // if key-val in write cf can not find matched data in default cf and its commit-ts < gc-safe-point, we can clean it safely.
                    if (ts < safe_point)
                    {
                        del_write += 1;
                        delta.sub(RegionWriteCFData::calcTotalKVSize(write_map_it->second));
                        write_map_it = write_map.erase(write_map_it);
                        continue;
                    }
                }
            }
        }
        ++write_map_it;
    }
    recordMemChange(delta);
    updateMemoryUsage(delta);
    // No need to check default cf. Because tikv will gc default cf before write cf.
    return del_write;
}

} // namespace DB
