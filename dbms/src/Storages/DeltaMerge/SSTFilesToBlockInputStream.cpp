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
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/SSTFilesToBlockInputStream.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/SSTReader.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/StorageDeltaMerge.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLFORMAT_RAFT_ROW;
} // namespace ErrorCodes

namespace DM
{
SSTFilesToBlockInputStream::SSTFilesToBlockInputStream( //
    RegionPtr region_,
    UInt64 snapshot_index_,
    const SSTViewVec & snaps_,
    const TiFlashRaftProxyHelper * proxy_helper_,
    TMTContext & tmt_,
    SSTFilesToBlockInputStreamOpts && opts_)
    : region(std::move(region_))
    , snapshot_index(snapshot_index_)
    , snaps(snaps_)
    , proxy_helper(proxy_helper_)
    , tmt(tmt_)
    , opts(std::move(opts_))
{
    log = Logger::get(opts.log_prefix);
}

SSTFilesToBlockInputStream::~SSTFilesToBlockInputStream() = default;

void SSTFilesToBlockInputStream::readPrefix()
{
    std::vector<SSTView> ssts_default;
    std::vector<SSTView> ssts_write;
    std::vector<SSTView> ssts_lock;

    auto make_inner_func
        = [&](const TiFlashRaftProxyHelper * proxy_helper, SSTView snap, SSTReader::RegionRangeFilter range) {
              return std::make_unique<MonoSSTReader>(proxy_helper, snap, range);
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
            region->getRange());
    }
    if (!ssts_write.empty())
    {
        write_cf_reader = std::make_unique<MultiSSTReader<MonoSSTReader, SSTView>>(
            proxy_helper,
            ColumnFamilyType::Write,
            make_inner_func,
            ssts_write,
            log,
            region->getRange());
    }
    if (!ssts_lock.empty())
    {
        lock_cf_reader = std::make_unique<MultiSSTReader<MonoSSTReader, SSTView>>(
            proxy_helper,
            ColumnFamilyType::Lock,
            make_inner_func,
            ssts_lock,
            log,
            region->getRange());
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

void SSTFilesToBlockInputStream::readSuffix()
{
    // There must be no data left when we write suffix
    assert(!write_cf_reader || !write_cf_reader->remained());
    assert(!default_cf_reader || !default_cf_reader->remained());
    assert(!lock_cf_reader || !lock_cf_reader->remained());

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
            region->insert(ColumnFamilyType::Write, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));
            ++process_keys.write_cf;
            process_keys.write_cf_bytes += (key.len + value.len);
            if (process_keys.write_cf % opts.expected_size == 0)
            {
                loaded_write_cf_key.assign(key.data, key.len);
            }
        } // Notice: `key`, `value` are string-view-like object, should never use after `next` called
        write_cf_reader->next();

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

    // Load all key-value pairs from other CFs
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
    size_t * p_process_keys;
    size_t * p_process_keys_bytes;
    DecodedTiKVKey * last_loaded_rowkey;
    if (cf == ColumnFamilyType::Default)
    {
        reader = default_cf_reader.get();
        p_process_keys = &process_keys.default_cf;
        p_process_keys_bytes = &process_keys.default_cf_bytes;
        last_loaded_rowkey = &default_last_loaded_rowkey;
    }
    else if (cf == ColumnFamilyType::Lock)
    {
        reader = lock_cf_reader.get();
        p_process_keys = &process_keys.lock_cf;
        p_process_keys_bytes = &process_keys.lock_cf_bytes;
        last_loaded_rowkey = &lock_last_loaded_rowkey;
    }
    else
        throw Exception("Unknown cf, should not happen!");

    // Simply read to the end of SST file
    if (rowkey_to_be_included == nullptr)
    {
        while (reader && reader->remained())
        {
            BaseBuffView key = reader->keyView();
            BaseBuffView value = reader->valueView();
            // TODO: use doInsert to avoid locking
            region->insert(cf, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len), DupCheck::AllowSame);
            reader->next();
            (*p_process_keys) += 1;
            (*p_process_keys_bytes) += (key.len + value.len);
        }
        LOG_DEBUG(
            log,
            "Done loading all kvpairs, CF={} offset={} processed_bytes={} write_cf_offset={} region_id={} "
            "snapshot_index={}",
            CFToName(cf),
            (*p_process_keys),
            (*p_process_keys_bytes),
            process_keys.write_cf,
            region->id(),
            snapshot_index);
        return;
    }

    size_t process_keys_offset_end = process_keys.write_cf;
    while (reader && reader->remained())
    {
        // If we have load all keys that less than or equal to `rowkey_to_be_included`, done.
        // We keep an assumption that rowkeys are memory-comparable and they are asc sorted in the SST file
        if (!last_loaded_rowkey->empty() && *last_loaded_rowkey > *rowkey_to_be_included)
        {
            LOG_DEBUG(
                log,
                "Done loading, CF={} offset={} processed_bytes={} write_cf_offset={} last_loaded_rowkey={} "
                "rowkey_to_be_included={} region_id={} snapshot_index={}",
                CFToName(cf),
                (*p_process_keys),
                (*p_process_keys_bytes),
                process_keys.write_cf,
                Redact::keyToDebugString(last_loaded_rowkey->data(), last_loaded_rowkey->size()),
                (rowkey_to_be_included
                     ? Redact::keyToDebugString(rowkey_to_be_included->data(), rowkey_to_be_included->size())
                     : "<end>"),
                region->id(),
                snapshot_index);
            break;
        }

        // Let's try to load keys until process_keys_offset_end
        while (reader && reader->remained() && *p_process_keys < process_keys_offset_end)
        {
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
} // namespace DM
} // namespace DB