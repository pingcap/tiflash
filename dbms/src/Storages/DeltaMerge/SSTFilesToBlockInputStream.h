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
#include <Storages/Transaction/PartitionStreams.h>

#include <memory>
#include <string_view>

namespace Poco
{
class Logger;
}

namespace DB
{
class TMTContext;
class Region;
using RegionPtr = std::shared_ptr<Region>;

struct SSTViewVec;
struct TiFlashRaftProxyHelper;
struct SSTReader;
class StorageDeltaMerge;

namespace DM
{
struct ColumnDefine;
using ColumnDefines = std::vector<ColumnDefine>;
using ColumnDefinesPtr = std::shared_ptr<ColumnDefines>;

// forward declaration
class SSTFilesToBlockInputStream;
using SSTFilesToBlockInputStreamPtr = std::shared_ptr<SSTFilesToBlockInputStream>;
class BoundedSSTFilesToBlockInputStream;
using BoundedSSTFilesToBlockInputStreamPtr = std::shared_ptr<BoundedSSTFilesToBlockInputStream>;

class SSTFilesToBlockInputStream final : public IBlockInputStream
{
public:
    SSTFilesToBlockInputStream(RegionPtr region_,
                               const SSTViewVec & snaps_,
                               const TiFlashRaftProxyHelper * proxy_helper_,
                               DecodingStorageSchemaSnapshotConstPtr schema_snap_,
                               Timestamp gc_safepoint_,
                               bool force_decode_,
                               TMTContext & tmt_,
                               size_t expected_size_ = DEFAULT_MERGE_BLOCK_SIZE);
    ~SSTFilesToBlockInputStream() override;

    String getName() const override { return "SSTFilesToBlockInputStream"; }

    Block getHeader() const override { return toEmptyBlock(*(schema_snap->column_defines)); }

    void readPrefix() override;
    void readSuffix() override;
    Block read() override;

public:
    struct ProcessKeys
    {
        size_t default_cf = 0;
        size_t write_cf = 0;
        size_t lock_cf = 0;

        inline size_t total() const { return default_cf + write_cf + lock_cf; }
    };

private:
    void loadCFDataFromSST(ColumnFamilyType cf, const DecodedTiKVKey * rowkey_need_include);

    Block readCommitedBlock();

private:
    RegionPtr region;
    const SSTViewVec & snaps;
    const TiFlashRaftProxyHelper * proxy_helper{nullptr};
    DecodingStorageSchemaSnapshotConstPtr schema_snap;
    TMTContext & tmt;
    const Timestamp gc_safepoint;
    size_t expected_size;
    Poco::Logger * log;

    using SSTReaderPtr = std::unique_ptr<SSTReader>;
    SSTReaderPtr write_cf_reader;
    SSTReaderPtr default_cf_reader;
    SSTReaderPtr lock_cf_reader;

    DecodedTiKVKey default_last_loaded_rowkey;
    DecodedTiKVKey lock_last_loaded_rowkey;

    friend class BoundedSSTFilesToBlockInputStream;

    const bool force_decode;
    bool is_decode_cancelled = false;

    ProcessKeys process_keys;
};

// Bound the blocks read from SSTFilesToBlockInputStream by column `_tidb_rowid` and
// do some calculation for the `DMFileWriter::BlockProperty` of read blocks.
class BoundedSSTFilesToBlockInputStream final
{
public:
    BoundedSSTFilesToBlockInputStream(SSTFilesToBlockInputStreamPtr child,
                                      const ColId pk_column_id_,
                                      const DecodingStorageSchemaSnapshotConstPtr & schema_snap);

    String getName() const { return "BoundedSSTFilesToBlockInputStream"; }

    void readPrefix();

    void readSuffix();

    Block read();

    SSTFilesToBlockInputStream::ProcessKeys getProcessKeys() const;

    const RegionPtr getRegion() const;

    // Return values: (effective rows, not clean rows, gc hint version)
    std::tuple<size_t, size_t, UInt64> getMvccStatistics() const;

private:
    const ColId pk_column_id;

    // Note that we only keep _raw_child for getting ingest info / process key, etc. All block should be
    // read from `mvcc_compact_stream`
    const SSTFilesToBlockInputStreamPtr _raw_child;
    std::unique_ptr<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>> mvcc_compact_stream;
};

} // namespace DM
} // namespace DB
