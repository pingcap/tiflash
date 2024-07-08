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

#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/Decode/SnapshotSSTReader.h>
#include <Storages/KVStore/Decode/PartitionStreams.h>
#include <Storages/KVStore/MultiRaft/PreHandlingTrace.h>
#include <Storages/KVStore/MultiRaft/RegionState.h>

#include <memory>
#include <string_view>

namespace DB
{
class TMTContext;
class Region;
using RegionPtr = std::shared_ptr<Region>;

struct SSTViewVec;
struct TiFlashRaftProxyHelper;
class StorageDeltaMerge;

namespace DM
{
// forward declaration
class SSTFilesToBlockInputStream;
using SSTFilesToBlockInputStreamPtr = std::shared_ptr<SSTFilesToBlockInputStream>;
class BoundedSSTFilesToBlockInputStream;
using BoundedSSTFilesToBlockInputStreamPtr = std::shared_ptr<BoundedSSTFilesToBlockInputStream>;

struct SSTFilesToBlockInputStreamOpts
{
    std::string log_prefix;
    DecodingStorageSchemaSnapshotConstPtr schema_snap;
    Timestamp gc_safepoint;
    // Whether abort when meeting an error in decoding.
    bool force_decode;
    // The expected size of emitted `Block`.
    size_t expected_size;
};

// Read blocks from TiKV's SSTFiles or Tablets.
class SSTFilesToBlockInputStream final : public IBlockInputStream
{
public:
    SSTFilesToBlockInputStream( //
        RegionPtr region_,
        UInt64 snapshot_index_,
        const SnapshotSSTReaderPtr & snap_reader_,
        TMTContext & tmt_,
        std::shared_ptr<PreHandlingTrace::Item> prehandle_task_,
        SSTFilesToBlockInputStreamOpts && opts_);
    ~SSTFilesToBlockInputStream() override;

    String getName() const override { return "SSTFilesToBlockInputStream"; }

    Block getHeader() const override { return toEmptyBlock(*(opts.schema_snap->column_defines)); }

    void readPrefix() override;
    void readSuffix() override;
    Block read() override;

public:
    struct ProcessKeys
    {
        size_t default_cf = 0;
        size_t write_cf = 0;
        size_t lock_cf = 0;
        size_t default_cf_bytes = 0;
        size_t write_cf_bytes = 0;
        size_t lock_cf_bytes = 0;

        inline size_t total() const { return default_cf + write_cf + lock_cf; }
        inline size_t totalBytes() const { return default_cf_bytes + write_cf_bytes + lock_cf_bytes; }
    };

    const ProcessKeys & getProcessKeys() const { return process_keys; }
    size_t getSplitId() const { return snap_reader->getSplitId(); }

    using SSTReaderPtr = std::unique_ptr<SSTReader>;
    bool maybeSkipBySoftLimit()
    {
        return snap_reader->maybeSkipBySoftLimit(ColumnFamilyType::Write, snap_reader->write_cf_reader);
    }

private:
    void loadCFDataFromSST(ColumnFamilyType cf, const DecodedTiKVKey * rowkey_to_be_included);

    // Emits data into block if the transaction to this key is committed.
    Block readCommitedBlock();
    void checkFinishedState(SSTReaderPtr & reader, ColumnFamilyType cf);

private:
    RegionPtr region;
    UInt64 snapshot_index;
    TMTContext & tmt;
    // TODO: merge `soft_limit` into `snap_reader`
    // std::optional<SSTScanSoftLimit> soft_limit;
    std::shared_ptr<PreHandlingTrace::Item> prehandle_task;
    const SSTFilesToBlockInputStreamOpts opts;
    LoggerPtr log;

    SnapshotSSTReaderPtr snap_reader;

    DecodedTiKVKey default_last_loaded_rowkey;
    DecodedTiKVKey lock_last_loaded_rowkey;

    friend class BoundedSSTFilesToBlockInputStream;

    bool is_decode_cancelled = false;

    ProcessKeys process_keys;
};

// Bound the blocks read from SSTFilesToBlockInputStream by column `_tidb_rowid` and
// do some calculation for the `DMFileWriter::BlockProperty` of read blocks.
// Equals to PKSquashingBlockInputStream + DMVersionFilterBlockInputStream<COMPACT>
class BoundedSSTFilesToBlockInputStream final
{
public:
    BoundedSSTFilesToBlockInputStream(
        SSTFilesToBlockInputStreamPtr child,
        ColId pk_column_id_,
        const DecodingStorageSchemaSnapshotConstPtr & schema_snap,
        size_t split_id);

    static String getName() { return "BoundedSSTFilesToBlockInputStream"; }

    void readPrefix();

    void readSuffix();

    Block read();

    SSTFilesToBlockInputStream::ProcessKeys getProcessKeys() const;

    RegionPtr getRegion() const;

    // Return values: (effective rows, not clean rows, is delete rows, gc hint version)
    std::tuple<size_t, size_t, size_t, UInt64> getMvccStatistics() const;

    size_t getSplitId() const;

private:
    const ColId pk_column_id;

    // Note that we only keep _raw_child for getting ingest info / process key, etc. All block should be
    // read from `mvcc_compact_stream`
    const SSTFilesToBlockInputStreamPtr _raw_child; // NOLINT(readability-identifier-naming)
    std::unique_ptr<DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT>> mvcc_compact_stream;
};

} // namespace DM
} // namespace DB
