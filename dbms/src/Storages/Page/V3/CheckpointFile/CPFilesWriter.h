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

#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/CheckpointFile/CPDataFileStat.h>
#include <Storages/Page/V3/CheckpointFile/CPDataFileWriter.h>
#include <Storages/Page/V3/CheckpointFile/CPDumpStat.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileWriter.h>
#include <Storages/Page/V3/CheckpointFile/CPWriteDataSource.h>
#include <Storages/Page/V3/CheckpointFile/Proto/common.pb.h>
#include <Storages/Page/V3/CheckpointFile/fwd.h>
#include <Storages/Page/V3/PageEntriesEdit.h>

#include <unordered_set>

namespace DB::PS::V3
{

class CPFilesWriter : private boost::noncopyable
{
public:
    struct Options
    {
        const String & data_file_path_pattern;
        const String & data_file_id_pattern;
        const String & manifest_file_path;
        const String & manifest_file_id;
        const CPWriteDataSourcePtr data_source;

        /**
         * The list of lock files that will be always appended to the checkpoint file.
         *
         * Note: In addition to the specified lock files, the checkpoint file will also contain
         * lock files from `writeEditsAndApplyCheckpointInfo`.
         */
        const std::unordered_set<String> & must_locked_files = {};
        UInt64 sequence;
        UInt64 max_data_file_size = 256 * 1024 * 1024;
        UInt64 max_edit_records_per_part = 100000;
    };

    static CPFilesWriterPtr create(Options options) { return std::make_unique<CPFilesWriter>(std::move(options)); }

    explicit CPFilesWriter(Options options);

    struct PrefixInfo
    {
        const CheckpointProto::WriterInfo & writer;
        const uint64_t sequence;
        const uint64_t last_sequence;
    };

    /**
     * Must be called first, before other `writeXxx`.
     */
    void writePrefix(const PrefixInfo & info);

    /**
     * If the entry's remote file_id is contains by `file_ids_to_compact`, then the
     * entry data will be write down to the new data file.
     *
     * This function can be called multiple times if there are too many edits and
     * you want to write in a streaming way. You are also allowed to not call this
     * function at all, if there is no edit.
     *
     * You must call `writeSuffix` finally, if you don't plan to write edits anymore.
     */
    struct CompactOptions
    {
        bool compact_all_data;
        std::unordered_set<String> file_ids;

        explicit CompactOptions(bool full_compact = false)
            : compact_all_data(full_compact)
        {}
        explicit CompactOptions(const std::unordered_set<String> & file_ids)
            : compact_all_data(false)
            , file_ids(file_ids)
        {}
    };
    CPDataDumpStats writeEditsAndApplyCheckpointInfo(
        universal::PageEntriesEdit & edit,
        const CompactOptions & options = CompactOptions(false),
        bool manifest_only = false);

    /**
     * This function must be called, and must be called last, after other `writeXxx`.
     */
    [[nodiscard]] std::vector<String> writeSuffix();

    void abort();

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    enum class WriteStage
    {
        WritingPrefix,
        WritingEdits,
        WritingFinished,
    };

    void newDataWriter();

    const String manifest_file_id;
    const String data_file_id_pattern;
    const String data_file_path_pattern;
    const UInt64 sequence;
    const UInt64 max_data_file_size;
    Int32 data_file_index = 0;
    CPDataFileWriterPtr data_writer;
    const CPManifestFileWriterPtr manifest_writer;
    const CPWriteDataSourcePtr data_source;

    std::unordered_set<String> locked_files;
    WriteStage write_stage = WriteStage::WritingPrefix;
    std::vector<String> data_file_paths;
    CheckpointProto::DataFilePrefix data_prefix;
    UInt64 current_write_size = 0;
    UInt64 total_written_records = 0;
    LoggerPtr log;
};

} // namespace DB::PS::V3
