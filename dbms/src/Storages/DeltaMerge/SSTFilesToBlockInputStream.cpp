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
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/PKSquashingBlockInputStream.h>
#include <Storages/DeltaMerge/SSTFilesToBlockInputStream.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/PartitionStreams.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/SSTReader.h>
#include <Storages/Transaction/TMTContext.h>
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
    const SSTViewVec & snaps_,
    const TiFlashRaftProxyHelper * proxy_helper_,
    DecodingStorageSchemaSnapshotConstPtr schema_snap_,
    Timestamp gc_safepoint_,
    bool force_decode_,
    TMTContext & tmt_,
    size_t expected_size_)
    : region(std::move(region_))
    , snaps(snaps_)
    , proxy_helper(proxy_helper_)
    , schema_snap(std::move(schema_snap_))
    , tmt(tmt_)
    , gc_safepoint(gc_safepoint_)
    , expected_size(expected_size_)
    , log(&Poco::Logger::get("SSTFilesToBlockInputStream"))
    , force_decode(force_decode_)
{
}

SSTFilesToBlockInputStream::~SSTFilesToBlockInputStream() = default;

void SSTFilesToBlockInputStream::readPrefix()
{
    for (UInt64 i = 0; i < snaps.len; ++i)
    {
        auto & snapshot = snaps.views[i];
        switch (snapshot.type)
        {
        case ColumnFamilyType::Default:
            default_cf_reader = std::make_unique<SSTReader>(proxy_helper, snapshot);
            break;
        case ColumnFamilyType::Write:
            write_cf_reader = std::make_unique<SSTReader>(proxy_helper, snapshot);
            break;
        case ColumnFamilyType::Lock:
            lock_cf_reader = std::make_unique<SSTReader>(proxy_helper, snapshot);
            break;
        }
    }

    process_keys.default_cf = 0;
    process_keys.write_cf = 0;
    process_keys.lock_cf = 0;
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
            BaseBuffView key = write_cf_reader->key();
            BaseBuffView value = write_cf_reader->value();
            region->insert(ColumnFamilyType::Write, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));
            ++process_keys.write_cf;
            if (process_keys.write_cf % expected_size == 0)
            {
                loaded_write_cf_key.clear();
                loaded_write_cf_key.assign(key.data, key.len);
            }
        } // Notice: `key`, `value` are string-view-like object, should never use after `next` called
        write_cf_reader->next();

        if (process_keys.write_cf % expected_size == 0)
        {
            const DecodedTiKVKey rowkey = RecordKVFormat::decodeTiKVKey(TiKVKey(std::move(loaded_write_cf_key)));
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

void SSTFilesToBlockInputStream::loadCFDataFromSST(ColumnFamilyType cf, const DecodedTiKVKey * const rowkey_to_be_included)
{
    SSTReader * reader;
    size_t * p_process_keys = &process_keys.default_cf;
    DecodedTiKVKey * last_loaded_rowkey = &default_last_loaded_rowkey;
    if (cf == ColumnFamilyType::Default)
    {
        reader = default_cf_reader.get();
        p_process_keys = &process_keys.default_cf;
        last_loaded_rowkey = &default_last_loaded_rowkey;
    }
    else if (cf == ColumnFamilyType::Lock)
    {
        reader = lock_cf_reader.get();
        p_process_keys = &process_keys.lock_cf;
        last_loaded_rowkey = &lock_last_loaded_rowkey;
    }
    else
        throw Exception("Unknown cf, should not happen!");

    // Simply read to the end of SST file
    if (rowkey_to_be_included == nullptr)
    {
        while (reader && reader->remained())
        {
            BaseBuffView key = reader->key();
            BaseBuffView value = reader->value();
            // TODO: use doInsert to avoid locking
            region->insert(cf, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));
            reader->next();
            (*p_process_keys) += 1;
        }
#ifndef NDEBUG
        LOG_FMT_DEBUG(log, "Done loading all kvpairs from [CF={}] [offset={}] [write_cf_offset={}] ", CFToName(cf), (*p_process_keys), process_keys.write_cf);
#endif
        return;
    }

    size_t process_keys_offset_end = process_keys.write_cf;
    while (reader && reader->remained())
    {
        // If we have load all keys that less than or equal to `rowkey_to_be_included`, done.
        // We keep an assumption that rowkeys are memory-comparable and they are asc sorted in the SST file
        if (!last_loaded_rowkey->empty() && *last_loaded_rowkey > *rowkey_to_be_included)
        {
#ifndef NDEBUG
            LOG_FMT_DEBUG(
                log,
                "Done loading from [CF={}] [offset={}] [write_cf_offset={}] [last_loaded_rowkey={}] [rowkey_to_be_included={}]",
                CFToName(cf),
                (*p_process_keys),
                process_keys.write_cf,
                Redact::keyToDebugString(last_loaded_rowkey->data(), last_loaded_rowkey->size()),
                (rowkey_to_be_included ? Redact::keyToDebugString(rowkey_to_be_included->data(), rowkey_to_be_included->size()) : "<end>"));
#endif
            break;
        }

        // Let's try to load keys until process_keys_offset_end
        while (reader && reader->remained() && *p_process_keys < process_keys_offset_end)
        {
            {
                BaseBuffView key = reader->key();
                BaseBuffView value = reader->value();
                // TODO: use doInsert to avoid locking
                region->insert(cf, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));
                (*p_process_keys) += 1;
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
        process_keys_offset_end += expected_size;
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
        return GenRegionBlockDataWithSchema(region, schema_snap, gc_safepoint, force_decode, tmt);
    }
    catch (DB::Exception & e)
    {
        if (e.code() == ErrorCodes::ILLFORMAT_RAFT_ROW)
        {
            // br or lighting may write illegal data into tikv, stop decoding.
            LOG_FMT_WARNING(log, "Got error while reading region committed cache: {}. Stop decoding rows into DTFiles and keep uncommitted data in region.", e.displayText());
            // Cancel the decoding process.
            // Note that we still need to scan data from CFs and keep them in `region`
            is_decode_cancelled = true;
            return {};
        }
        else
            throw;
    }
}

/// Methods for BoundedSSTFilesToBlockInputStream

BoundedSSTFilesToBlockInputStream::BoundedSSTFilesToBlockInputStream( //
    SSTFilesToBlockInputStreamPtr child,
    const ColId pk_column_id_,
    const DecodingStorageSchemaSnapshotConstPtr & schema_snap)
    : pk_column_id(pk_column_id_)
    , _raw_child(std::move(child))
{
    const bool is_common_handle = schema_snap->is_common_handle;
    // Initlize `mvcc_compact_stream`
    // First refine the boundary of blocks. Note that the rows decoded from SSTFiles are sorted by primary key asc, timestamp desc
    // (https://github.com/tikv/tikv/blob/v5.0.1/components/txn_types/src/types.rs#L103-L108).
    // While DMVersionFilter require rows sorted by primary key asc, timestamp asc, so we need an extra sort in PKSquashing.
    auto stream = std::make_shared<PKSquashingBlockInputStream</*need_extra_sort=*/true>>(_raw_child, pk_column_id, is_common_handle);
    mvcc_compact_stream = std::make_unique<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
        stream,
        *(schema_snap->column_defines),
        _raw_child->gc_safepoint,
        is_common_handle);
}

void BoundedSSTFilesToBlockInputStream::readPrefix()
{
    mvcc_compact_stream->readPrefix();
}

void BoundedSSTFilesToBlockInputStream::readSuffix()
{
    mvcc_compact_stream->readSuffix();
}

Block BoundedSSTFilesToBlockInputStream::read()
{
    return mvcc_compact_stream->read();
}

SSTFilesToBlockInputStream::ProcessKeys BoundedSSTFilesToBlockInputStream::getProcessKeys() const
{
    return _raw_child->process_keys;
}

const RegionPtr BoundedSSTFilesToBlockInputStream::getRegion() const
{
    return _raw_child->region;
}

std::tuple<size_t, size_t, UInt64> //
BoundedSSTFilesToBlockInputStream::getMvccStatistics() const
{
    return std::make_tuple(
        mvcc_compact_stream->getEffectiveNumRows(),
        mvcc_compact_stream->getNotCleanRows(),
        mvcc_compact_stream->getGCHintVersion());
}

} // namespace DM
} // namespace DB
