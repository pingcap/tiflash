#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/ReorganizeBlockInputStream.h>

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

class SSTFilesToBlockInputStream : public IBlockInputStream
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
class BoundedSSTFilesToBlockInputStream final : public ReorganizeBlockInputStream
{
public:
    BoundedSSTFilesToBlockInputStream(SSTFilesToBlockInputStreamPtr child, ColId pk_column_id, bool is_common_handle_);

    String getName() const override { return "BoundedSSTFilesToBlockInputStream"; }

    Block getHeader() const override;

    void readPrefix() override;

    size_t getProcessKeys() const;

    const RegionPtr getRegion() const;

private:
    const SSTFilesToBlockInputStream * getChildStream() const;
};

} // namespace DM
} // namespace DB
