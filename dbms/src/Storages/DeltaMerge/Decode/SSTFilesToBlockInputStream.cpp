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

#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/Decode/SSTFilesToBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/PKSquashingBlockInputStream.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/SSTReader.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/StorageDeltaMerge.h>
#include <common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int ILLFORMAT_RAFT_ROW;
} // namespace DB::ErrorCodes

namespace DB::DM
{
SSTFilesToBlockInputStream::SSTFilesToBlockInputStream( //
    RegionPtr region_,
    UInt64 snapshot_index_,
    const SSTViewVec & snaps_,
    const TiFlashRaftProxyHelper * proxy_helper_,
    TMTContext & tmt_,
    std::optional<SSTScanSoftLimit> && soft_limit_,
    std::shared_ptr<PreHandlingTrace::Item> prehandle_task_,
    SSTFilesToBlockInputStreamOpts && opts_)
    : region(std::move(region_))
    , snapshot_index(snapshot_index_)
    , snaps(snaps_)
    , proxy_helper(proxy_helper_)
    , tmt(tmt_)
    , soft_limit(std::move(soft_limit_))
    , prehandle_task(prehandle_task_)
    , opts(std::move(opts_))
{
    log = Logger::get(opts.log_prefix);

    // We have to initialize sst readers at an earlier stage,
    // due to prehandle snapshot of single region feature in raftstore v2.
    std::vector<SSTView> ssts_default;
    std::vector<SSTView> ssts_write;
    std::vector<SSTView> ssts_lock;

    auto make_inner_func = [&](const TiFlashRaftProxyHelper * proxy_helper,
                               SSTView snap,
                               SSTReader::RegionRangeFilter range,
                               size_t split_id) {
        return std::make_unique<MonoSSTReader>(proxy_helper, snap, range, split_id);
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
            region->getRange(),
            soft_limit.has_value() ? soft_limit.value().split_id : DM::SSTScanSoftLimit::HEAD_OR_ONLY_SPLIT);
    }
    if (!ssts_write.empty())
    {
        write_cf_reader = std::make_unique<MultiSSTReader<MonoSSTReader, SSTView>>(
            proxy_helper,
            ColumnFamilyType::Write,
            make_inner_func,
            ssts_write,
            log,
            region->getRange(),
            soft_limit.has_value() ? soft_limit.value().split_id : DM::SSTScanSoftLimit::HEAD_OR_ONLY_SPLIT);
    }
    if (!ssts_lock.empty())
    {
        lock_cf_reader = std::make_unique<MultiSSTReader<MonoSSTReader, SSTView>>(
            proxy_helper,
            ColumnFamilyType::Lock,
            make_inner_func,
            ssts_lock,
            log,
            region->getRange(),
            soft_limit.has_value() ? soft_limit.value().split_id : DM::SSTScanSoftLimit::HEAD_OR_ONLY_SPLIT);
    }
    LOG_INFO(
        log,
        "Finish Construct MultiSSTReader, write={} lock={} default={} region_id={} snapshot_index={}",
        ssts_write.size(),
        ssts_lock.size(),
        ssts_default.size(),
        this->region->id(),
        snapshot_index);

    // Init stat info.
    process_keys.default_cf = 0;
    process_keys.write_cf = 0;
    process_keys.lock_cf = 0;
    process_keys.default_cf_bytes = 0;
    process_keys.write_cf_bytes = 0;
    process_keys.lock_cf_bytes = 0;
}

SSTFilesToBlockInputStream::~SSTFilesToBlockInputStream() = default;

void SSTFilesToBlockInputStream::readPrefix() {}

void SSTFilesToBlockInputStream::checkFinishedState(SSTReaderPtr & reader, ColumnFamilyType cf)
{
    // There must be no data left when we write suffix
    if (!reader)
        return;
    if (!reader->remained())
        return;
    if (prehandle_task->isAbort())
        return;

    // now the stream must be stopped by `soft_limit`, let's check the keys in reader
    RUNTIME_CHECK_MSG(soft_limit.has_value(), "soft_limit.has_value(), cf={}", magic_enum::enum_name(cf));
    BaseBuffView cur = reader->keyView();
    RUNTIME_CHECK_MSG(
        buffToStrView(cur) > soft_limit.value().raw_end,
        "cur > raw_end, cf={}",
        magic_enum::enum_name(cf));
}

void SSTFilesToBlockInputStream::readSuffix()
{
    checkFinishedState(write_cf_reader, ColumnFamilyType::Write);
    checkFinishedState(default_cf_reader, ColumnFamilyType::Default);
    checkFinishedState(lock_cf_reader, ColumnFamilyType::Lock);

    // reset all SSTReaders and return without writting blocks any more.
    write_cf_reader.reset();
    default_cf_reader.reset();
    lock_cf_reader.reset();
}

Block SSTFilesToBlockInputStream::read()
{
    std::string loaded_write_cf_key;

    while (write_cf_reader && write_cf_reader->remained())
    {
        bool should_stop_advancing = maybeStopBySoftLimit(ColumnFamilyType::Write, write_cf_reader);
        if (should_stop_advancing)
        {
            // Load the last batch
            break;
        }

        // To decode committed rows from key-value pairs into block, we need to load
        // all need key-value pairs from default and lock column families.
        // Check the MVCC (key-format and transaction model) for details
        // https://en.pingcap.com/blog/2016-11-17-mvcc-in-tikv#mvcc
        // To ensure correctness, when loading key-values pairs from the default and
        // the lock column family, we will load all key-values which rowkeys are equal
        // or less that the last rowkey from the write column family.
        {
            BaseBuffView key = write_cf_reader->keyView();
            BaseBuffView value = write_cf_reader->valueView();
            auto tikv_key = TiKVKey(key.data, key.len);
            region->insert(ColumnFamilyType::Write, std::move(tikv_key), TiKVValue(value.data, value.len));
            ++process_keys.write_cf;
            process_keys.write_cf_bytes += (key.len + value.len);
            if (process_keys.write_cf % opts.expected_size == 0)
            {
                loaded_write_cf_key.assign(key.data, key.len);
            }
        } // Notice: `key`, `value` are string-view-like object, should never use after `next` called
        write_cf_reader->next();

        // If there is enough data to form a Block, we will load all keys before `loaded_write_cf_key` in other cf.
        if (process_keys.write_cf % opts.expected_size == 0)
        {
            // If we should form a new block.
            const DecodedTiKVKey rowkey = RecordKVFormat::decodeTiKVKey(TiKVKey(std::move(loaded_write_cf_key)));
            loaded_write_cf_key.clear();

            // Batch the loading from other CFs until we need to decode data
            loadCFDataFromSST(ColumnFamilyType::Default, &rowkey);
            loadCFDataFromSST(ColumnFamilyType::Lock, &rowkey);

            auto block = readCommitedBlock();

            if (block.rows() != 0)
                return block;
            // else continue to decode key-value from write CF.
        }
    }

    // We emit the last block of this sst decode stream here.
    // Load all key-value pairs from other CFs.

    loadCFDataFromSST(ColumnFamilyType::Default, nullptr);
    loadCFDataFromSST(ColumnFamilyType::Lock, nullptr);

    // All uncommitted data are saved in `region`, decode the last committed rows.
    return readCommitedBlock();
}

void SSTFilesToBlockInputStream::loadCFDataFromSST(
    ColumnFamilyType cf,
    const DecodedTiKVKey * const rowkey_to_be_included)
{
    SSTReader * reader;
    SSTReaderPtr * reader_ptr;
    size_t * p_process_keys;
    size_t * p_process_keys_bytes;
    DecodedTiKVKey * last_loaded_rowkey;
    if (cf == ColumnFamilyType::Default)
    {
        reader = default_cf_reader.get();
        reader_ptr = &default_cf_reader;
        p_process_keys = &process_keys.default_cf;
        p_process_keys_bytes = &process_keys.default_cf_bytes;
        last_loaded_rowkey = &default_last_loaded_rowkey;
    }
    else if (cf == ColumnFamilyType::Lock)
    {
        reader = lock_cf_reader.get();
        reader_ptr = &lock_cf_reader;
        p_process_keys = &process_keys.lock_cf;
        p_process_keys_bytes = &process_keys.lock_cf_bytes;
        last_loaded_rowkey = &lock_last_loaded_rowkey;
    }
    else
        throw Exception("Unknown cf, should not happen!");

    if (reader && reader->remained())
    {
        maybeSkipBySoftLimit(cf, *reader_ptr);
    }

    Stopwatch sw;
    auto pre = *p_process_keys_bytes;
    // Simply read to the end of SST file
    if (rowkey_to_be_included == nullptr)
    {
        while (reader && reader->remained())
        {
            if (maybeStopBySoftLimit(cf, *reader_ptr))
            {
                break;
            }
            BaseBuffView key = reader->keyView();
            BaseBuffView value = reader->valueView();
            // TODO: use doInsert to avoid locking
            region->insert(cf, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len), DupCheck::AllowSame);
            (*p_process_keys) += 1;
            (*p_process_keys_bytes) += (key.len + value.len);
            reader->next();
        }
        auto sec = sw.elapsedMilliseconds();
        LOG_DEBUG(
            log,
            "Done loading all kvpairs, CF={} offset={} processed_bytes={} write_cf_offset={} region_id={} split_id={} "
            "snapshot_index={} elapsed_sec={} speed={}",
            CFToName(cf),
            (*p_process_keys),
            (*p_process_keys_bytes),
            process_keys.write_cf,
            region->id(),
            getSplitId(),
            snapshot_index,
            sec,
            ((*p_process_keys_bytes) - pre) * 1.0 / sec);
        return;
    }

    size_t process_keys_offset_end = process_keys.write_cf;
    while (reader && reader->remained())
    {
        // If we have load all keys that less than or equal to `rowkey_to_be_included`, done.
        // We keep an assumption that rowkeys are memory-comparable and they are asc sorted in the SST file
        if (!last_loaded_rowkey->empty() && *last_loaded_rowkey > *rowkey_to_be_included)
        {
            auto sec = sw.elapsedMilliseconds();
            LOG_DEBUG(
                log,
                "Done loading, CF={} offset={} processed_bytes={} write_cf_offset={} last_loaded_rowkey={} "
                "rowkey_to_be_included={} region_id={} snapshot_index={} elapsed_sec={} speed={}",
                CFToName(cf),
                (*p_process_keys),
                (*p_process_keys_bytes),
                process_keys.write_cf,
                Redact::keyToDebugString(last_loaded_rowkey->data(), last_loaded_rowkey->size()),
                (rowkey_to_be_included
                     ? Redact::keyToDebugString(rowkey_to_be_included->data(), rowkey_to_be_included->size())
                     : "<end>"),
                region->id(),
                snapshot_index,
                sec,
                ((*p_process_keys_bytes) - pre) * 1.0 / sec);
            break;
        }

        // Let's try to load keys until process_keys_offset_end
        while (reader && reader->remained() && *p_process_keys < process_keys_offset_end)
        {
            if (maybeStopBySoftLimit(cf, *reader_ptr))
            {
                break;
            }
            {
                BaseBuffView key = reader->keyView();
                BaseBuffView value = reader->valueView();
                // TODO: use doInsert to avoid locking
                region->insert(cf, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));
                (*p_process_keys) += 1;
                (*p_process_keys_bytes) += (key.len + value.len);
                if (*p_process_keys == process_keys_offset_end)
                {
                    *last_loaded_rowkey = RecordKVFormat::decodeTiKVKey(TiKVKey(key.data, key.len));
                }
            } // Notice: `key`, `value` are string-view-like object, should never use after `next` called
            reader->next();
        }

        // Update the end offset.
        // If there are no more key-value, the outer while loop will be break.
        // Else continue to read next batch from current CF.
        process_keys_offset_end += opts.expected_size;
    }
}

Block SSTFilesToBlockInputStream::readCommitedBlock()
{
    if (is_decode_cancelled)
        return {};

    try
    {
        // Read block from `region`. If the schema has been updated, it will
        // throw an exception with code `ErrorCodes::REGION_DATA_SCHEMA_UPDATED`
        return GenRegionBlockDataWithSchema(region, opts.schema_snap, opts.gc_safepoint, opts.force_decode, tmt);
    }
    catch (DB::Exception & e)
    {
        if (e.code() == ErrorCodes::ILLFORMAT_RAFT_ROW)
        {
            // br or lighting may write illegal data into tikv, stop decoding.
            const auto & start_key = region->getMetaRegion().start_key();
            const auto & end_key = region->getMetaRegion().end_key();
            LOG_WARNING(
                log,
                "Got error while reading region committed cache: {}. Stop decoding rows into DTFiles and keep "
                "uncommitted data in region."
                "region_id: {}, applied_index: {}, version: {}, conf_version {}, start_key: {}, end_key: {}",
                e.displayText(),
                region->id(),
                region->appliedIndex(),
                region->version(),
                region->confVer(),
                Redact::keyToDebugString(start_key.data(), start_key.size()),
                Redact::keyToDebugString(end_key.data(), end_key.size()));
            // Cancel the decoding process.
            // Note that we still need to scan data from CFs and keep them in `region`
            is_decode_cancelled = true;
            return {};
        }
        else
            throw;
    }
}

size_t SSTFilesToBlockInputStream::getApproxBytes() const
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

std::vector<std::string> SSTFilesToBlockInputStream::findSplitKeys(size_t splits_count) const
{
    return write_cf_reader->findSplitKeys(splits_count);
}

// Returning false means no skip is performed, the reader is intact.
// Returning true means skip is performed, must read from current value.
bool SSTFilesToBlockInputStream::maybeSkipBySoftLimit(ColumnFamilyType cf, SSTReaderPtr & reader)
{
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
                "Re-Seek backward is forbidden, start_limit {} current {}, cf={}, split_id={}, region_id={}",
                soft_limit.value().raw_start.toDebugString(),
                Redact::keyToDebugString(key.data, key.len),
                magic_enum::enum_name(cf),
                soft_limit.value().split_id,
                region->id());
            return false;
        }
    }
    // Safety `soft_limit` outlives returned base buff view.
    reader->seek(cppStringAsBuff(soft_limit.value().raw_start));

    // Skip other versions of the same PK.
    // TODO(split) use seek to optimize if failed several iterations.
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
                "current pk decreases as reader advances, start_raw {} start_pk {} current {}, cf={}, split_id={}, "
                "region_id={}",
                soft_limit.value().raw_start.toDebugString(),
                start_limit.value().toDebugString(),
                current_truncated_ts.toDebugString(),
                magic_enum::enum_name(cf),
                soft_limit.value().split_id,
                region->id());
            LOG_INFO(
                log,
                "Re-Seek after start_raw {} start_pk {} to {}, current_pk = {}, cf={}, split_id={}, region_id={}",
                soft_limit.value().raw_start.toDebugString(),
                start_limit.value().toDebugString(),
                tikv_key.toDebugString(),
                current_truncated_ts.toDebugString(),
                magic_enum::enum_name(cf),
                soft_limit.value().split_id,
                region->id());
            return true;
        }
        reader->next();
    }
    // `start_limit` is the last pk of the sst file.
    LOG_INFO(
        log,
        "Re-Seek to the last key of write cf start_raw {} start_pk {}, cf={}, split_id={}, region_id={}",
        soft_limit.value().raw_start.toDebugString(),
        start_limit.value().toDebugString(),
        magic_enum::enum_name(cf),
        soft_limit.value().split_id,
        region->id());
    return false;
}

bool SSTFilesToBlockInputStream::maybeStopBySoftLimit(ColumnFamilyType cf, SSTReaderPtr & reader)
{
    if (!soft_limit.has_value())
        return false;
    const SSTScanSoftLimit & sl = soft_limit.value();
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
            "Reach end for split {} current {} pk {} end_limit {}, cf={} split_id={} region_id={}",
            sl.toDebugString(),
            tikv_key.toDebugString(),
            current_truncated_ts.toDebugString(),
            end_limit->toDebugString(),
            magic_enum::enum_name(cf),
            soft_limit.value().split_id,
            region->id());
        // Seek to the end of reader to prevent further check.
        reader->seekToLast();
        return true;
    }
    return false;
}
} // namespace DB::DM
