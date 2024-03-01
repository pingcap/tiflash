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

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/KVStore/FFI/ColumnFamily.h>
#include <Storages/KVStore/MultiRaft/RegionData.h>
#include <Storages/KVStore/Read/RegionLockInfo.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLFORMAT_RAFT_ROW;
} // namespace ErrorCodes

void RegionData::reportAlloc(size_t delta)
{
    root_of_kvstore_mem_trackers->alloc(delta, false);
}

void RegionData::reportDealloc(size_t delta)
{
    root_of_kvstore_mem_trackers->free(delta);
}

void RegionData::reportDelta(size_t prev, size_t current)
{
    if (current >= prev)
    {
        root_of_kvstore_mem_trackers->alloc(current - prev, false);
    }
    else
    {
        root_of_kvstore_mem_trackers->free(prev - current);
    }
}

RegionDataRes RegionData::insert(ColumnFamilyType cf, TiKVKey && key, TiKVValue && value, DupCheck mode)
{
    switch (cf)
    {
    case ColumnFamilyType::Write:
    {
        auto delta = write_cf.insert(std::move(key), std::move(value), mode);
        cf_data_size += delta;
        reportAlloc(delta);
        return delta;
    }
    case ColumnFamilyType::Default:
    {
        auto delta = default_cf.insert(std::move(key), std::move(value), mode);
        cf_data_size += delta;
        reportAlloc(delta);
        return delta;
    }
    case ColumnFamilyType::Lock:
    {
        auto delta = lock_cf.insert(std::move(key), std::move(value), mode);
        cf_data_size += delta;
        if likely (delta >= 0)
        {
            reportAlloc(delta);
        }
        else
        {
            reportDealloc(-delta);
        }
        return 0;
    }
    }
}

void RegionData::remove(ColumnFamilyType cf, const TiKVKey & key)
{
    switch (cf)
    {
    case ColumnFamilyType::Write:
    {
        auto raw_key = RecordKVFormat::decodeTiKVKey(key);
        auto pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        // removed by gc, may not exist.
        auto delta = write_cf.remove(RegionWriteCFData::Key{pk, ts}, true);
        cf_data_size -= delta;
        reportDealloc(delta);
        return;
    }
    case ColumnFamilyType::Default:
    {
        auto raw_key = RecordKVFormat::decodeTiKVKey(key);
        auto pk = RecordKVFormat::getRawTiDBPK(raw_key);
        Timestamp ts = RecordKVFormat::getTs(key);
        // removed by gc, may not exist.
        auto delta = default_cf.remove(RegionDefaultCFData::Key{pk, ts}, true);
        cf_data_size -= delta;
        reportDealloc(delta);
        return;
    }
    case ColumnFamilyType::Lock:
    {
        auto delta
            = lock_cf.remove(RegionLockCFDataTrait::Key{nullptr, std::string_view(key.data(), key.dataSize())}, true);
        cf_data_size -= delta;
        reportDealloc(delta);
        return;
    }
    }
}

RegionData::WriteCFIter RegionData::removeDataByWriteIt(const WriteCFIter & write_it)
{
    const auto & [key, value, decoded_val] = write_it->second;
    const auto & [pk, ts] = write_it->first;

    std::ignore = ts;
    std::ignore = value;
    std::ignore = key;

    if (decoded_val.write_type == RecordKVFormat::CFModifyFlag::PutFlag)
    {
        auto & map = default_cf.getDataMut();

        if (auto data_it = map.find({pk, decoded_val.prewrite_ts}); data_it != map.end())
        {
            auto delta = RegionDefaultCFData::calcTiKVKeyValueSize(data_it->second);
            cf_data_size -= delta;
            map.erase(data_it);
            reportDealloc(delta);
        }
    }

    auto delta = RegionWriteCFData::calcTiKVKeyValueSize(write_it->second);
    cf_data_size -= delta;
    reportDealloc(delta);

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

DecodedLockCFValuePtr RegionData::getLockInfo(const RegionLockReadQuery & query) const
{
    for (const auto & [pk, value] : lock_cf.getData())
    {
        std::ignore = pk;

        const auto & [tikv_key, tikv_val, lock_info_ptr] = value;
        std::ignore = tikv_key;
        std::ignore = tikv_val;
        const auto & lock_info = *lock_info_ptr;

        if (lock_info.lock_version > query.read_tso || lock_info.lock_type == kvrpcpb::Op::Lock
            || lock_info.lock_type == kvrpcpb::Op::PessimisticLock)
            continue;
        if (lock_info.min_commit_ts > query.read_tso)
            continue;
        if (query.bypass_lock_ts && query.bypass_lock_ts->count(lock_info.lock_version))
            continue;
        return lock_info_ptr;
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
    size_t size_changed = 0;
    size_changed += default_cf.splitInto(range, new_region_data.default_cf);
    size_changed += write_cf.splitInto(range, new_region_data.write_cf);
    // reportAlloc: Remember to track memory here if we have a region-wise metrics later.
    size_changed += lock_cf.splitInto(range, new_region_data.lock_cf);
    cf_data_size -= size_changed;
    new_region_data.cf_data_size += size_changed;
}

void RegionData::mergeFrom(const RegionData & ori_region_data)
{
    size_t size_changed = 0;
    size_changed += default_cf.mergeFrom(ori_region_data.default_cf);
    size_changed += write_cf.mergeFrom(ori_region_data.write_cf);
    // reportAlloc: Remember to track memory here if we have a region-wise metrics later.
    size_changed += lock_cf.mergeFrom(ori_region_data.lock_cf);
    cf_data_size += size_changed;
}

size_t RegionData::dataSize() const
{
    return cf_data_size;
}

void RegionData::assignRegionData(RegionData && new_region_data)
{
    default_cf = std::move(new_region_data.default_cf);
    write_cf = std::move(new_region_data.write_cf);
    lock_cf = std::move(new_region_data.lock_cf);
    orphan_keys_info = std::move(new_region_data.orphan_keys_info);

    cf_data_size = new_region_data.cf_data_size.load();
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
    size_t total_size = 0;
    total_size += RegionDefaultCFData::deserialize(buf, region_data.default_cf);
    total_size += RegionWriteCFData::deserialize(buf, region_data.write_cf);
    total_size += RegionLockCFData::deserialize(buf, region_data.lock_cf);

    region_data.cf_data_size += total_size;
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

RegionData::RegionData(RegionData && data)
    : write_cf(std::move(data.write_cf))
    , default_cf(std::move(data.default_cf))
    , lock_cf(std::move(data.lock_cf))
    , cf_data_size(data.cf_data_size.load())
{}

RegionData & RegionData::operator=(RegionData && rhs)
{
    write_cf = std::move(rhs.write_cf);
    default_cf = std::move(rhs.default_cf);
    lock_cf = std::move(rhs.lock_cf);
    reportDelta(cf_data_size, rhs.cf_data_size.load());
    cf_data_size = rhs.cf_data_size.load();
    return *this;
}

} // namespace DB
