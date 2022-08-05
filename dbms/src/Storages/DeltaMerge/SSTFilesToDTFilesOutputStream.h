// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Stopwatch.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
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
using ColumnDefines = std::vector<ColumnDefine>;
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
    /**
     * When `split_after_rows` or `split_after_size` are > 0, multiple DTFiles will be produced.
     * When `0` is specified for both parameters, only one DTFile will be produced.
     *
     * As the stream is processed by blocks, each DTFile is not ensured truncated at the specified
     * rows or size: it is possible that one DTFile is significantly large, if a large Block
     * is produced by the `child`.
     *
     * @param split_after_rows_ Split for a new DTFile when reaching specified rows.
     * @param split_after_size_ Split for a new DTFile when reaching specified bytes.
     */
    SSTFilesToDTFilesOutputStream(BoundedSSTFilesToBlockInputStreamPtr child_,
                                  StorageDeltaMergePtr storage_,
                                  DecodingStorageSchemaSnapshotConstPtr schema_snap_,
                                  TiDB::SnapshotApplyMethod method_,
                                  FileConvertJobType job_type_,
                                  UInt64 split_after_rows_,
                                  UInt64 split_after_size_,
                                  TMTContext & tmt_);
    ~SSTFilesToDTFilesOutputStream();

    void writePrefix();
    void writeSuffix();
    void write();

    /**
     * The DTFiles that can be ingested. The returned vector is ensured to be sorted by the file range in ascending order.
     */
    SortedExternalDTFileInfos outputFiles() const;

    // Try to cleanup the files in `ingest_files` quickly.
    void cancel();

private:
    bool newDTFileStream();
    bool finalizeDTFileStream();
    void updateRange(Block & block);

private:
    BoundedSSTFilesToBlockInputStreamPtr child;
    StorageDeltaMergePtr storage;
    DecodingStorageSchemaSnapshotConstPtr schema_snap;
    const TiDB::SnapshotApplyMethod method;
    const FileConvertJobType job_type;
    const UInt64 split_after_rows;
    const UInt64 split_after_size;
    TMTContext & tmt;
    Poco::Logger * log;

    std::unique_ptr<DMFileBlockOutputStream> dt_stream;

    std::vector<DMFilePtr> ingest_files;
    std::vector<RowKeyRange> ingest_files_range;

    /**
     * How many rows has been committed to the current DTFile.
     */
    size_t committed_rows_this_dt_file = 0;
    size_t committed_bytes_this_dt_file = 0;

    /**
     * How many rows has been committed so far.
     */
    size_t total_committed_rows = 0;
    size_t total_committed_bytes = 0;

    Stopwatch watch;
};

} // namespace DM
} // namespace DB
