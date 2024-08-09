// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/Decode/SnapshotSSTReader.h>

namespace DB::DM
{

SnapshotSSTReader::SnapshotSSTReader(
    const SSTViewVec & snaps,
    const TiFlashRaftProxyHelper * proxy_helper,
    RegionID region_id,
    UInt64 snapshot_index,
    const SSTReader::RegionRangeFilter & region_range,
    std::optional<SSTScanSoftLimit> && soft_limit_,
    const LoggerPtr & log_)
    : soft_limit(std::move(soft_limit_))
    , log(log_)
{
    assert(region_range != nullptr);
    assert(log != nullptr);

    // We have to initialize sst readers at an earlier stage,
    // due to prehandle snapshot of single region feature in raftstore v2.
    std::vector<SSTView> ssts_default;
    std::vector<SSTView> ssts_write;
    std::vector<SSTView> ssts_lock;

    auto make_inner_func = [&](const TiFlashRaftProxyHelper * proxy_helper,
                               SSTView snap,
                               SSTReader::RegionRangeFilter range,
                               const LoggerPtr & log_) {
        return std::make_unique<MonoSSTReader>(proxy_helper, snap, range, log_);
    };
    for (UInt64 i = 0; i < snaps.len; ++i)
    {
        const auto & snapshot = snaps.views[i];
        switch (snapshot.type)
        {
        case ColumnFamilyType::Default:
            ssts_default.push_back(snapshot);
            break;
        case ColumnFamilyType::Write:
            ssts_write.push_back(snapshot);
            break;
        case ColumnFamilyType::Lock:
            ssts_lock.push_back(snapshot);
            break;
        }
    }

    // Pass the log to SSTReader inorder to filter logs by table_id suffix
    if (!ssts_default.empty())
    {
        default_cf_reader = std::make_unique<MultiSSTReader<MonoSSTReader, SSTView>>(
            proxy_helper,
            ColumnFamilyType::Default,
            make_inner_func,
            ssts_default,
            log,
            region_range);
    }
    if (!ssts_write.empty())
    {
        write_cf_reader = std::make_unique<MultiSSTReader<MonoSSTReader, SSTView>>(
            proxy_helper,
            ColumnFamilyType::Write,
            make_inner_func,
            ssts_write,
            log,
            region_range);
    }
    if (!ssts_lock.empty())
    {
        lock_cf_reader = std::make_unique<MultiSSTReader<MonoSSTReader, SSTView>>(
            proxy_helper,
            ColumnFamilyType::Lock,
            make_inner_func,
            ssts_lock,
            log,
            region_range);
    }

    LOG_INFO(
        log,
        "Finish Construct MultiSSTReader, write={} lock={} default={} snapshot_index={}",
        ssts_write.size(),
        ssts_lock.size(),
        ssts_default.size(),
        region_id,
        snapshot_index);
}

// Get the approximate bytes of all CFs
size_t SnapshotSSTReader::getApproxBytes() const
{
    size_t total = 0;
    if (write_cf_reader)
        total += write_cf_reader->approxSize();
    if (lock_cf_reader)
        total += lock_cf_reader->approxSize();
    if (default_cf_reader)
        total += default_cf_reader->approxSize();
    return total;
}

// Try to find the split point by write_cf
std::vector<String> SnapshotSSTReader::findSplitKeys(size_t splits_count) const
{
    return write_cf_reader->findSplitKeys(splits_count);
}

// Returning false means no skip is performed, the reader is intact.
// Returning true means skip is performed, must read from current value.
bool SnapshotSSTReader::maybeSkipBySoftLimit(ColumnFamilyType cf, SSTReader * reader)
{
    assert(reader != nullptr);
    if (!soft_limit.has_value())
        return false;

    const auto & start_limit = soft_limit.value().getStartLimit();
    // If start is set to "", then there is no soft limit for start.
    if (!start_limit)
        return false;

    if (reader && reader->remained())
    {
        auto key = reader->keyView();
        if (soft_limit.value().raw_start < buffToStrView(key))
        {
            // This happens when there is too many untrimmed data,
            // or it is already seeked.
            LOG_TRACE(
                log,
                "Re-Seek backward is forbidden, start_limit={} current={} cf={}",
                soft_limit.value().raw_start.toDebugString(),
                Redact::keyToDebugString(key.data, key.len),
                magic_enum::enum_name(cf));
            return false;
        }
    }
    // Safety `soft_limit` outlives returned base buff view.
    reader->seek(cppStringAsBuff(soft_limit.value().raw_start));

    // Skip other versions of the same PK.
    // TODO(split) use seek to optimize if failed several iterations.
    size_t skipped_times = 0;
    while (reader && reader->remained())
    {
        // Read until find the next pk.
        auto key = reader->keyView();
        // TODO the copy could be eliminated, but with many modifications.
        auto tikv_key = TiKVKey(key.data, key.len);
        auto current_truncated_ts = RecordKVFormat::getRawTiDBPK(RecordKVFormat::decodeTiKVKey(tikv_key));
        // If found a new pk.
        if (current_truncated_ts != start_limit)
        {
            RUNTIME_CHECK_MSG(
                current_truncated_ts > start_limit,
                "current pk decreases as reader advances, skipped_times={} start_raw={} start_pk={} current_pk={} "
                "current_raw={} cf={} log_ident={}",
                skipped_times,
                soft_limit.value().raw_start.toDebugString(),
                start_limit.value().toDebugString(),
                current_truncated_ts.toDebugString(),
                tikv_key.toDebugString(),
                magic_enum::enum_name(cf),
                log->identifier());
            LOG_INFO(
                log,
                "Re-Seek after skipped_times={} start_raw={} start_pk={} current_raw={} current_pk={} cf={}",
                skipped_times,
                soft_limit.value().raw_start.toDebugString(),
                start_limit.value().toDebugString(),
                tikv_key.toDebugString(),
                current_truncated_ts.toDebugString(),
                magic_enum::enum_name(cf));
            return true;
        }
        skipped_times++;
        reader->next();
    }
    // `start_limit` is the last pk of the sst file.
    LOG_INFO(
        log,
        "Re-Seek to the last key of write cf start_raw={} start_pk={} cf={}",
        soft_limit.value().raw_start.toDebugString(),
        start_limit.value().toDebugString(),
        magic_enum::enum_name(cf));
    return false;
}

bool SnapshotSSTReader::maybeStopBySoftLimit(ColumnFamilyType cf, SSTReader * reader)
{
    assert(reader != nullptr);
    if (!soft_limit.has_value())
        return false;

    const auto & end_limit = soft_limit.value().getEndLimit();
    if (!end_limit)
        return false;

    auto key = reader->keyView();
    // TODO the copy could be eliminated, but with many modifications.
    auto tikv_key = TiKVKey(key.data, key.len);
    auto current_truncated_ts = RecordKVFormat::getRawTiDBPK(RecordKVFormat::decodeTiKVKey(tikv_key));
    if (current_truncated_ts > end_limit)
    {
        LOG_INFO(
            log,
            "Reach end for split={} current={} pk={} end_limit={} cf={}",
            soft_limit->toDebugString(),
            tikv_key.toDebugString(),
            current_truncated_ts.toDebugString(),
            end_limit->toDebugString(),
            magic_enum::enum_name(cf));
        // Seek to the end of reader to prevent further check.
        reader->seekToLast();
        return true;
    }
    return false;
}

void SnapshotSSTReader::checkCFFinishedState(ColumnFamilyType cf, SSTReader * reader) const
{
    if (reader == nullptr)
        return;
    // There must be no data left when we write suffix
    if (!reader->remained())
        return;

    // now the stream must be stopped by `soft_limit`, let's check the keys in reader
    RUNTIME_CHECK_MSG(
        soft_limit.has_value(),
        "soft_limit.has_value(), cf={} log_ident={}",
        magic_enum::enum_name(cf),
        log->identifier());
    BaseBuffView cur = reader->keyView();
    RUNTIME_CHECK_MSG(
        buffToStrView(cur) > soft_limit.value().raw_end,
        "cur > raw_end, cf={} log_ident={}",
        magic_enum::enum_name(cf),
        log->identifier());
}

} // namespace DB::DM
