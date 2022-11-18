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

#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/Remote/CheckpointDataFileWriter.h>
#include <Storages/Page/V3/Remote/CheckpointManifestFileWriter.h>
#include <Storages/Page/V3/Remote/Proto/common.pb.h>

#include <memory>

namespace DB::PS::V3
{

template <typename PSDirTrait, typename PSBlobTrait>
class CheckpointFilesWriter;

template <typename PSDirTrait, typename PSBlobTrait>
using CheckpointFilesWriterPtr = std::unique_ptr<CheckpointFilesWriter<PSDirTrait, PSBlobTrait>>;

template <typename PSDirTrait, typename PSBlobTrait>
class CheckpointFilesWriter
{
public:
    struct Info
    {
        const std::shared_ptr<const Remote::WriterInfo> writer;
        const uint64_t sequence;
        const uint64_t last_sequence;
    };

    struct Options
    {
        const Info & info;

        CheckpointDataFileWriterPtr<PSDirTrait> data_writer;
        CheckpointManifestFileWriterPtr<PSDirTrait> manifest_writer;

        /**
         * The caller must ensure `blob_store` is valid when using the CheckpointFilesWriter.
         */
        BlobStore<PSBlobTrait> & blob_store;

        const LoggerPtr & log;
    };

    static CheckpointFilesWriterPtr<PSDirTrait, PSBlobTrait> create(Options options)
    {
        return std::make_unique<CheckpointFilesWriter>(std::move(options));
    }

    explicit CheckpointFilesWriter(Options options)
        : info(options.info)
        , data_writer(std::move(options.data_writer))
        , manifest_writer(std::move(options.manifest_writer))
        , blob_store(options.blob_store)
        , log(options.log->getChild("CheckpointFilesWriter"))
    {
    }

    void writePrefix()
    {
        Remote::DataFilePrefix data_prefix;
        data_prefix.set_local_sequence(info.sequence);
        data_prefix.set_create_at_ms(Poco::Timestamp().epochMicroseconds() / 1000);
        data_prefix.mutable_writer_info()->CopyFrom(*info.writer);
        data_prefix.set_manifest_file_id(manifest_writer->getFileId());
        data_prefix.set_sub_file_index(0);
        data_writer->writePrefix(data_prefix);
    }

    void writeSuffix()
    {
        data_writer->writeSuffix();
    }

    bool /* has_new_data */ writeEditsAndApplyRemoteInfo(typename PSDirTrait::PageEntriesEdit & edit)
    {
        LOG_DEBUG(log, "Begin writeEditsAndApplyRemoteInfo, edit_n={}", edit.size());

        auto & records = edit.getMutRecords();
        if (records.empty())
            return false;

        // 1. Iterate all edits, find these entry edits without the remote info.
        for (auto & rec_edit : records)
        {
            if (rec_edit.type != EditRecordType::VAR_ENTRY)
                continue;
            if (rec_edit.entry.remote_info.has_value())
                continue;

            LOG_DEBUG(log, "Processing edit={}", rec_edit.toDebugString());

            // 2. For entry edits without the remote info, write them to the data file, and assign a new remote info.
            typename PSDirTrait::PageIdAndEntry id_and_entry{rec_edit.page_id, rec_edit.entry};
            auto page = blob_store.read(id_and_entry);
            RUNTIME_CHECK(page.isValid());
            auto data_location = data_writer->write(rec_edit.page_id, rec_edit.version, page.data.begin(), page.data.size());
            rec_edit.entry.remote_info = RemoteDataInfo{
                .data_location = data_location,
                .is_local_data_reclaimed = false,
            };
        }

        // 3. Write down everything to the manifest.
        Remote::ManifestFilePrefix manifest_prefix;
        manifest_prefix.mutable_writer_info()->CopyFrom(*info.writer);
        manifest_prefix.set_local_sequence(info.sequence);
        manifest_prefix.set_last_local_sequence(info.last_sequence);
        manifest_prefix.set_create_at_ms(Poco::Timestamp().epochMicroseconds() / 1000);
        manifest_writer->write(manifest_prefix, edit);

        return data_writer->writtenRecords() > 0;
    }

private:
    const Info info;
    const CheckpointDataFileWriterPtr<PSDirTrait> data_writer;
    const CheckpointManifestFileWriterPtr<PSDirTrait> manifest_writer;
    BlobStore<PSBlobTrait> & blob_store;

    LoggerPtr log;
};

} // namespace DB::PS::V3
