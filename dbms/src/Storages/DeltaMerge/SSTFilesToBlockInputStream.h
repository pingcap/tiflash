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
    SSTFilesToBlockInputStream(RegionPtr                      region_,
                               const SSTViewVec &             snaps_,
                               const TiFlashRaftProxyHelper * proxy_helper_,
                               TMTContext &                   tmt_,
                               size_t                         expected_size_ = DEFAULT_MERGE_BLOCK_SIZE);
    ~SSTFilesToBlockInputStream();

    String getName() const override { return "SSTFilesToBlockInputStream"; }

    Block getHeader() const override
    {
        if (cur_schema)
            return toEmptyBlock(*cur_schema);
        else
            return {};
    }

    void  readPrefix();
    void  readSuffix();
    Block read() override;

private:
    void scanCF(ColumnFamilyType cf, const std::string_view until = std::string_view{});

    Block readCommitedBlock();

private:
    RegionPtr                      region;
    const SSTViewVec &             snaps;
    const TiFlashRaftProxyHelper * proxy_helper{nullptr};
    TMTContext &                   tmt;
    size_t                         expected_size;
    Poco::Logger *                 log;

    using SSTReaderPtr = std::unique_ptr<SSTReader>;
    SSTReaderPtr write_reader;
    SSTReaderPtr default_reader;
    SSTReaderPtr lock_reader;

    friend class BoundedSSTFilesToBlockInputStream;

    std::shared_ptr<StorageDeltaMerge> ingest_storage;
    DM::ColumnDefinesPtr               cur_schema;

    bool is_decode_cancelled = false;

    size_t process_keys   = 0;
    size_t process_writes = 0;
};

// Bound the blocks read from SSTFilesToBlockInputStream by column `_tidb_rowid`
class BoundedSSTFilesToBlockInputStream final : public ReorganizeBlockInputStream
{
public:
    BoundedSSTFilesToBlockInputStream(SSTFilesToBlockInputStreamPtr child, ColId pk_column_id);

    String getName() const override { return "BoundedSSTFilesToBlockInputStream"; }

    Block getHeader() const override;

    std::tuple<std::shared_ptr<StorageDeltaMerge>, DM::ColumnDefinesPtr> ingestingInfo() const;

    size_t getProcessKeys() const;

    const RegionPtr getRegion() const;

private:
    const SSTFilesToBlockInputStream * getChildStream() const;
};

} // namespace DM
} // namespace DB
