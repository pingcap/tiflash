#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>

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
using ColumnDefines    = std::vector<ColumnDefine>;
using ColumnDefinesPtr = std::shared_ptr<ColumnDefines>;

// forward declaration
class SSTFilesToBlockInputStream;
using SSTFilesToBlockInputStreamPtr = std::shared_ptr<SSTFilesToBlockInputStream>;
class BoundedSSTFilesToBlockInputStream;
using BoundedSSTFilesToBlockInputStreamPtr = std::shared_ptr<BoundedSSTFilesToBlockInputStream>;

class SSTFilesToBlockInputStream final : public IBlockInputStream
{
public:
    using StorageDeltaMergePtr = std::shared_ptr<StorageDeltaMerge>;
    SSTFilesToBlockInputStream(RegionPtr                      region_,
                               const SSTViewVec &             snaps_,
                               const TiFlashRaftProxyHelper * proxy_helper_,
                               StorageDeltaMergePtr           ingest_storage_,
                               DM::ColumnDefinesPtr           schema_snap_,
                               TMTContext &                   tmt_,
                               size_t                         expected_size_ = DEFAULT_MERGE_BLOCK_SIZE);
    ~SSTFilesToBlockInputStream();

    String getName() const override { return "SSTFilesToBlockInputStream"; }

    Block getHeader() const override { return toEmptyBlock(*schema_snap); }

    void  readPrefix() override;
    void  readSuffix() override;
    Block read() override;

private:
    void scanCF(ColumnFamilyType cf, const std::string_view until = std::string_view{});

    Block readCommitedBlock();

private:
    RegionPtr                      region;
    const SSTViewVec &             snaps;
    const TiFlashRaftProxyHelper * proxy_helper{nullptr};
    const StorageDeltaMergePtr     ingest_storage;
    const DM::ColumnDefinesPtr     schema_snap;
    TMTContext &                   tmt;
    size_t                         expected_size;
    Poco::Logger *                 log;

    using SSTReaderPtr = std::unique_ptr<SSTReader>;
    SSTReaderPtr write_cf_reader;
    SSTReaderPtr default_cf_reader;
    SSTReaderPtr lock_cf_reader;

    friend class BoundedSSTFilesToBlockInputStream;

    bool is_decode_cancelled = false;

    size_t process_keys   = 0;
    size_t process_writes = 0;
};

// Bound the blocks read from SSTFilesToBlockInputStream by column `_tidb_rowid`
class BoundedSSTFilesToBlockInputStream final
{
public:
    BoundedSSTFilesToBlockInputStream(SSTFilesToBlockInputStreamPtr child,
                                      DM::ColumnDefinesPtr          schema_snap_,
                                      const ColId                   pk_column_id_,
                                      const bool                    is_common_handle_,
                                      const Timestamp               gc_safepoint_);

    String getName() const { return "BoundedSSTFilesToBlockInputStream"; }

    void readPrefix();

    void readSuffix();

    Block read();

    size_t getProcessKeys() const;

    const RegionPtr getRegion() const;

    std::tuple<size_t, size_t, UInt64> getMvccStatistics() const;

private:
    const DM::ColumnDefinesPtr schema_snap;
    const ColId                pk_column_id;
    const bool                 is_common_handle;
    const Timestamp            gc_safepoint;
    // Note that we only keep _raw_child for getting ingest info / process key, etc. All block should be
    // read from `mvcc_compact_stream`
    const SSTFilesToBlockInputStreamPtr                                              _raw_child;
    std::unique_ptr<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>> mvcc_compact_stream;
};

} // namespace DM
} // namespace DB
