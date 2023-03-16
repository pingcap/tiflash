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

#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/CheckpointFile/CPDataFileWriter.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileWriter.h>
#include <Storages/Page/V3/CheckpointFile/CPWriteDataSource.h>
#include <Storages/Page/V3/CheckpointFile/Proto/common.pb.h>
#include <Storages/Page/V3/CheckpointFile/fwd.h>
#include <Storages/Page/V3/PageEntriesEdit.h>

namespace DB::PS::V3
{

class CPFilesWriter : private boost::noncopyable
{
public:
    struct Options
    {
        const std::string & data_file_path;
        const std::string & data_file_id;
        const std::string & manifest_file_path;
        const std::string & manifest_file_id;
        const CPWriteDataSourcePtr data_source;

        /**
         * The list of lock files that will be always appended to the checkpoint file.
         *
         * Note: In addition to the specified lock files, the checkpoint file will also contain
         * lock files from `writeEditsAndApplyCheckpointInfo`.
         */
        const std::unordered_set<String> & must_locked_files = {};
    };

    static CPFilesWriterPtr create(Options options)
    {
        return std::make_unique<CPFilesWriter>(std::move(options));
    }

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
     * This function can be called multiple times if there are too many edits and
     * you want to write in a streaming way. You are also allowed to not call this
     * function at all, if there is no edit.
     *
     * You must call `writeSuffix` finally, if you don't plan to write edits anymore.
     */
    bool /* has_new_data */ writeEditsAndApplyCheckpointInfo(universal::PageEntriesEdit & edit);

    /**
     * This function must be called, and must be called last, after other `writeXxx`.
     */
    void writeSuffix();

    void flush()
    {
        data_writer->flush();
        manifest_writer->flush();
    }

    ~CPFilesWriter()
    {
        flush();
    }

private:
    enum class WriteStage
    {
        WritingPrefix,
        WritingEdits,
        WritingFinished,
    };

    const std::string manifest_file_id;
    const CPDataFileWriterPtr data_writer;
    const CPManifestFileWriterPtr manifest_writer;
    const CPWriteDataSourcePtr data_source;

    std::unordered_set<String> locked_files;
    WriteStage write_stage = WriteStage::WritingPrefix;

    LoggerPtr log;
};

} // namespace DB::PS::V3
