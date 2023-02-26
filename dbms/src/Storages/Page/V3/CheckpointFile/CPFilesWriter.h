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
        CPWriteDataSourcePtr data_source;
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

    void writePrefix(const PrefixInfo & info)
    {
        auto create_at_ms = Poco::Timestamp().epochMicroseconds() / 1000;

        CheckpointProto::DataFilePrefix data_prefix;
        data_prefix.set_local_sequence(info.sequence);
        data_prefix.set_create_at_ms(create_at_ms);
        data_prefix.mutable_writer_info()->CopyFrom(info.writer);
        data_prefix.set_manifest_file_id(manifest_file_id);
        data_prefix.set_sub_file_index(0);
        data_writer->writePrefix(data_prefix);

        CheckpointProto::ManifestFilePrefix manifest_prefix;
        manifest_prefix.set_local_sequence(info.sequence);
        manifest_prefix.set_last_local_sequence(info.last_sequence);
        manifest_prefix.set_create_at_ms(create_at_ms);
        manifest_prefix.mutable_writer_info()->CopyFrom(info.writer);
        manifest_writer->writePrefix(manifest_prefix);
    }

    /// This function can be called multiple times.
    bool /* has_new_data */ writeEditsAndApplyRemoteInfo(universal::PageEntriesEdit & edit)
    {
        auto & records = edit.getMutRecords();
        if (records.empty())
            return false;

        // 1. Iterate all edits, find these entry edits without the checkpoint info.
        for (auto & rec_edit : records)
        {
            if (rec_edit.type != EditRecordType::VAR_ENTRY)
                continue;
            if (rec_edit.entry.checkpoint_info.has_value())
                continue;

            // 2. For entry edits without the checkpoint info, write them to the data file, and assign a new checkpoint info.
            auto page = data_source->read({rec_edit.page_id, rec_edit.entry});
            RUNTIME_CHECK(page.isValid());
            auto data_location = data_writer->write(
                rec_edit.page_id,
                rec_edit.version,
                page.data.begin(),
                page.data.size());
            rec_edit.entry.checkpoint_info = {
                .data_location = data_location,
                .is_local_data_reclaimed = false,
            };
        }

        // 3. Write down everything to the manifest.
        manifest_writer->writeEdits(edit);

        return data_writer->writtenRecords() > 0;
    }

    void writeEditsFinish()
    {
        manifest_writer->writeEditsFinish();
    }

    void writeSuffix()
    {
        data_writer->writeSuffix();
        manifest_writer->writeSuffix();
    }

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
    const std::string manifest_file_id;
    const CPDataFileWriterPtr data_writer;
    const CPManifestFileWriterPtr manifest_writer;
    CPWriteDataSourcePtr data_source;

    LoggerPtr log;
};

} // namespace DB::PS::V3
