#pragma once

#include <Common/Stopwatch.h>
#include <Storages/Transaction/RaftStoreProxyFFI/ColumnFamily.h>

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

namespace DM
{

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
                                  const TiFlashRaftProxyHelper * proxy_helper_,
                                  TMTContext &                   tmt_);
    ~SSTFilesToDTFilesOutputStream();

    void writePrefix();
    void writeSuffix();

    void write();

private:
    void scanCF(ColumnFamilyType cf, const std::string_view until = std::string_view{});

    void saveCommitedData();

    void finishCurrDTFileStream();

private:
    RegionPtr                      region;
    const SSTViewVec &             snaps;
    const uint64_t                 snap_index;
    const uint64_t                 snap_term;
    const TiFlashRaftProxyHelper * proxy_helper{nullptr};
    TMTContext &                   tmt;
    Poco::Logger *                 log;

    using SSTReaderPtr = std::unique_ptr<SSTReader>;
    SSTReaderPtr write_reader;
    SSTReaderPtr default_reader;
    SSTReaderPtr lock_reader;

    String                                   snap_dir;
    UInt64                                   curr_file_id = 0;
    DMFilePtr                                dt_file;
    std::unique_ptr<DMFileBlockOutputStream> dt_stream;

    size_t    schema_sync_trigger_count = 0;
    size_t    process_keys              = 0;
    size_t    process_writes            = 0;
    Stopwatch watch;
};

} // namespace DM
} // namespace DB
