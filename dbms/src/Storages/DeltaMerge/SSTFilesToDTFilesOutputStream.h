#pragma once

#include <Common/Stopwatch.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/SSTFilesToBlockInputStream.h>
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
using StorageDeltaMergePtr = std::shared_ptr<StorageDeltaMerge>;
struct DecodingStorageSchemaSnapshot;

namespace DM
{

struct ColumnDefine;
using ColumnDefines    = std::vector<ColumnDefine>;
using ColumnDefinesPtr = std::shared_ptr<ColumnDefines>;

class DMFile;
using DMFilePtr = std::shared_ptr<DMFile>;
class DMFileBlockOutputStream;

enum class FileConvertJobType
{
    ApplySnapshot,
    IngestSST,
};


// This class is tightly coupling with BoundedSSTFilesToBlockInputStream
// to get some info of the decoding process.
class SSTFilesToDTFilesOutputStream : private boost::noncopyable
{
public:
    SSTFilesToDTFilesOutputStream(BoundedSSTFilesToBlockInputStreamPtr  child_,
                                  StorageDeltaMergePtr                  storage_,
                                  const DecodingStorageSchemaSnapshot & schema_snap_,
                                  TiDB::SnapshotApplyMethod             method_,
                                  FileConvertJobType                    job_type_,
                                  TMTContext &                          tmt_);
    ~SSTFilesToDTFilesOutputStream();

    void writePrefix();
    void writeSuffix();
    void write();

    PageIds ingestIds() const;

    // Try to cleanup the files in `ingest_files` quickly.
    void cancel();

private:
    bool newDTFileStream();

    // Stop the process for decoding committed data into DTFiles
    void stop();

private:
    BoundedSSTFilesToBlockInputStreamPtr  child;
    StorageDeltaMergePtr                  storage;
    const DecodingStorageSchemaSnapshot & schema_snap;
    const TiDB::SnapshotApplyMethod       method;
    const FileConvertJobType              job_type;
    TMTContext &                          tmt;
    Poco::Logger *                        log;

    std::unique_ptr<DMFileBlockOutputStream> dt_stream;

    std::vector<DMFilePtr> ingest_files;

    size_t    schema_sync_trigger_count = 0;
    size_t    commit_rows               = 0;
    Stopwatch watch;
};

} // namespace DM
} // namespace DB
