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

#include <Storages/Page/V3/CheckpointFile/CPFilesWriter.h>

namespace DB::PS::V3
{

CPFilesWriter::CPFilesWriter(CPFilesWriter::Options options)
    : manifest_file_id(options.manifest_file_id)
    , data_writer(CPDataFileWriter::create({
          .file_path = options.data_file_path,
          .file_id = options.data_file_id,
      }))
    , manifest_writer(CPManifestFileWriter::create({
          .file_path = options.manifest_file_path,
      }))
    , data_source(options.data_source)
    , locked_files(options.must_locked_files)
    , log(Logger::get())
{
}

void CPFilesWriter::writePrefix(const CPFilesWriter::PrefixInfo & info)
{
    RUNTIME_CHECK_MSG(write_stage == WriteStage::WritingPrefix, "unexpected write stage {}", magic_enum::enum_name(write_stage));

    auto create_at_ms = Poco::Timestamp().epochMicroseconds() / 1000;

    CheckpointProto::DataFilePrefix data_prefix;
    data_prefix.set_file_format(1);
    data_prefix.set_local_sequence(info.sequence);
    data_prefix.set_create_at_ms(create_at_ms);
    data_prefix.mutable_writer_info()->CopyFrom(info.writer);
    data_prefix.set_manifest_file_id(manifest_file_id);
    data_prefix.set_sub_file_index(0);
    data_writer->writePrefix(data_prefix);

    CheckpointProto::ManifestFilePrefix manifest_prefix;
    manifest_prefix.set_file_format(1);
    manifest_prefix.set_local_sequence(info.sequence);
    manifest_prefix.set_last_local_sequence(info.last_sequence);
    manifest_prefix.set_create_at_ms(create_at_ms);
    manifest_prefix.mutable_writer_info()->CopyFrom(info.writer);
    manifest_writer->writePrefix(manifest_prefix);

    write_stage = WriteStage::WritingEdits;
}

bool CPFilesWriter::writeEditsAndApplyCheckpointInfo(universal::PageEntriesEdit & edits)
{
    RUNTIME_CHECK_MSG(write_stage == WriteStage::WritingEdits, "unexpected write stage {}", magic_enum::enum_name(write_stage));

    auto & records = edits.getMutRecords();
    if (records.empty())
        return false;

    // 1. Iterate all edits, find these entry edits without the checkpoint info
    //    and collect the lock files from applied entries.
    for (auto & rec_edit : records)
    {
        if (rec_edit.type == EditRecordType::VAR_EXTERNAL)
        {
            RUNTIME_CHECK(
                rec_edit.entry.checkpoint_info.has_value() && //
                rec_edit.entry.checkpoint_info->data_location.data_file_id && //
                !rec_edit.entry.checkpoint_info->data_location.data_file_id->empty());
            // for example, the s3 fullpath of external id
            locked_files.emplace(*rec_edit.entry.checkpoint_info->data_location.data_file_id);
            continue;
        }

        if (rec_edit.type != EditRecordType::VAR_ENTRY)
            continue;

        if (rec_edit.entry.checkpoint_info.has_value())
        {
            // for example, the s3 fullpath that was written in the previous uploaded CheckpointDataFile
            locked_files.emplace(*rec_edit.entry.checkpoint_info->data_location.data_file_id);
            continue;
        }

        // 2. For entry edits without the checkpoint info, write them to the data file,
        // and assign a new checkpoint info.
        auto page = data_source->read({rec_edit.page_id, rec_edit.entry});
        RUNTIME_CHECK(page.isValid());
        auto data_location = data_writer->write(
            rec_edit.page_id,
            rec_edit.version,
            page.data.begin(),
            page.data.size());
        RUNTIME_CHECK(page.data.size() == rec_edit.entry.size, page.data.size(), rec_edit.entry.size);
        rec_edit.entry.checkpoint_info = {
            .data_location = data_location,
            .is_local_data_reclaimed = false,
        };
        locked_files.emplace(*data_location.data_file_id);
    }

    // 3. Write down everything to the manifest.
    manifest_writer->writeEdits(edits);

    return data_writer->writtenRecords() > 0;
}

void CPFilesWriter::writeSuffix()
{
    RUNTIME_CHECK_MSG(write_stage == WriteStage::WritingEdits, "unexpected write stage {}", magic_enum::enum_name(write_stage));

    manifest_writer->writeEditsFinish();
    manifest_writer->writeLocks(locked_files);
    manifest_writer->writeLocksFinish();

    data_writer->writeSuffix();
    manifest_writer->writeSuffix();

    write_stage = WriteStage::WritingFinished;
}

} // namespace DB::PS::V3