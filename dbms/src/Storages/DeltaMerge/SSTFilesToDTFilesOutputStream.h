#pragma once

#include <Common/Stopwatch.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/Page/PageDefines.h>

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

class DMFile;
using DMFilePtr = std::shared_ptr<DMFile>;
class DMFileBlockOutputStream;


class SSTFilesToDTFilesOutputStream : private boost::noncopyable
{
public:
    SSTFilesToDTFilesOutputStream(RegionPtr                      region_,
                                  const SSTViewVec &             snaps_,
                                  uint64_t                       index_,
                                  uint64_t                       term_,
                                  TiDB::SnapshotApplyMethod      method_,
                                  const TiFlashRaftProxyHelper * proxy_helper_,
                                  TMTContext &                   tmt_,
                                  size_t                         expected_size_ = DEFAULT_MERGE_BLOCK_SIZE);
    ~SSTFilesToDTFilesOutputStream();

    void writePrefix();
    void writeSuffix();
    void write();

    PageIds ingestIds() const { return ingest_file_ids; }

private:
    void scanCF(ColumnFamilyType cf, const std::string_view until = std::string_view{});

    void saveCommitedData();

    void finishCurrDTFileStream();

private:
    RegionPtr                       region;
    const SSTViewVec &              snaps;
    const uint64_t                  snap_index;
    const uint64_t                  snap_term;
    const TiDB::SnapshotApplyMethod method;
    const TiFlashRaftProxyHelper *  proxy_helper{nullptr};
    TMTContext &                    tmt;
    size_t                          expected_size;
    Poco::Logger *                  log;

    using SSTReaderPtr = std::unique_ptr<SSTReader>;
    SSTReaderPtr write_reader;
    SSTReaderPtr default_reader;
    SSTReaderPtr lock_reader;

    std::shared_ptr<StorageDeltaMerge>       ingest_storage;
    DM::ColumnDefinesPtr                     cur_schema;
    DMFilePtr                                dt_file;
    std::unique_ptr<DMFileBlockOutputStream> dt_stream;

    PageIds ingest_file_ids;

    size_t    schema_sync_trigger_count = 0;
    size_t    process_keys              = 0;
    size_t    process_writes            = 0;
    Stopwatch watch;
};

} // namespace DM
} // namespace DB
