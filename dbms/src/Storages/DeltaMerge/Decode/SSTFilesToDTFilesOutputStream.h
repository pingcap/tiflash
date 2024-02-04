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

#include <Common/Stopwatch.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/Decode/SSTFilesToBlockInputStream.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/DeltaMerge/File/DMFile_fwd.h>
#include <Storages/KVStore/MultiRaft/PreHandlingTrace.h>
#include <Storages/Page/PageDefinesBase.h>

#include <memory>

namespace DB
{
class TMTContext;
class Region;
using RegionPtr = std::shared_ptr<Region>;

struct SSTViewVec;
struct TiFlashRaftProxyHelper;
class SSTReader;
class StorageDeltaMerge;
using StorageDeltaMergePtr = std::shared_ptr<StorageDeltaMerge>;
struct DecodingStorageSchemaSnapshot;

namespace DM
{
class DMFileBlockOutputStream;

enum class FileConvertJobType
{
    ApplySnapshot,
    IngestSST,
};

// This class is tightly coupling with BoundedSSTFilesToBlockInputStream
// to get some info of the decoding process.
template <typename ChildStream>
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
    SSTFilesToDTFilesOutputStream(
        const std::string & log_prefix_,
        ChildStream child_,
        StorageDeltaMergePtr storage_,
        DecodingStorageSchemaSnapshotConstPtr schema_snap_,
        FileConvertJobType job_type_,
        UInt64 split_after_rows_,
        UInt64 split_after_size_,
        UInt64 region_id_,
        std::shared_ptr<PreHandlingTrace::Item> prehandle_task_,
        Context & context);
    ~SSTFilesToDTFilesOutputStream();

    void writePrefix();
    void writeSuffix();
    void write();

    /**
     * The DTFiles that can be ingested. The returned vector is ensured to be sorted by the file range in ascending order.
     */
    std::vector<ExternalDTFileInfo> outputFiles() const;

    // Try to cleanup the files in `ingest_files` quickly.
    void cancel();

    bool isAbort() const { return prehandle_task->isAbort(); }

    size_t getTotalCommittedBytes() const { return total_committed_bytes; }
    size_t getTotalBytesOnDisk() const { return total_bytes_on_disk; }

private:
    /**
     * Generate a DMFilePtr and its DMFileBlockOutputStream.
     */
    bool newDTFileStream();
    /**
     * Close the current DMFile stream.
     */
    bool finalizeDTFileStream();

    /**
     * Update the range for the current DTFile.
     */
    void updateRangeFromNonEmptyBlock(Block & block);

private:
    ChildStream child;
    StorageDeltaMergePtr storage;
    DecodingStorageSchemaSnapshotConstPtr schema_snap;
    const FileConvertJobType job_type;
    const UInt64 split_after_rows;
    const UInt64 split_after_size;
    const UInt64 region_id;
    std::shared_ptr<PreHandlingTrace::Item> prehandle_task;
    Context & context;
    LoggerPtr log;

    std::unique_ptr<DMFileBlockOutputStream> dt_stream;

    std::vector<DMFilePtr> ingest_files;
    std::vector<std::optional<RowKeyRange>> ingest_files_range;

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
    size_t total_bytes_on_disk = 0;

    Stopwatch watch;
};

class MockSSTFilesToDTFilesOutputStreamChild : private boost::noncopyable
{
public:
    MockSSTFilesToDTFilesOutputStreamChild(BlockInputStreamPtr mock_data_, RegionPtr mock_region_) //
        : mock_data(mock_data_)
        , mock_region(mock_region_)
    {}

    void readPrefix() { mock_data->readPrefix(); }

    void readSuffix() { mock_data->readSuffix(); }

    RegionPtr getRegion() const { return mock_region; }

    Block read() { return mock_data->read(); }

    static std::tuple<size_t, size_t, size_t, UInt64> getMvccStatistics() { return {}; }

    static SSTFilesToBlockInputStream::ProcessKeys getProcessKeys() { return {}; }

    static size_t getSplitId() { return DM::SSTScanSoftLimit::HEAD_OR_ONLY_SPLIT; }

protected:
    BlockInputStreamPtr mock_data;
    RegionPtr mock_region;
};

using MockSSTFilesToDTFilesOutputStreamChildPtr = std::shared_ptr<MockSSTFilesToDTFilesOutputStreamChild>;


} // namespace DM
} // namespace DB
