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

#include <Common/Stopwatch.h>
#include <Poco/File.h>
#include <Storages/Page/V3/CheckpointFile/CPDumpStat.h>
#include <Storages/Page/V3/CheckpointFile/CPFilesWriter.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntryCheckpointInfo.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <fmt/core.h>

#include <memory>
#include <unordered_map>

namespace DB::PS::V3
{

CPFilesWriter::CPFilesWriter(CPFilesWriter::Options options)
    : manifest_file_id(options.manifest_file_id)
    , data_file_id_pattern(options.data_file_id_pattern)
    , data_file_path_pattern(options.data_file_path_pattern)
    , sequence(options.sequence)
    , max_data_file_size(options.max_data_file_size)
    , manifest_writer(CPManifestFileWriter::create({
          .file_path = options.manifest_file_path,
          .max_edit_records_per_part = options.max_edit_records_per_part,
      }))
    , data_source(options.data_source)
    , locked_files(options.must_locked_files)
    , log(Logger::get())
{}

void CPFilesWriter::writePrefix(const CPFilesWriter::PrefixInfo & info)
{
    RUNTIME_CHECK_MSG(
        write_stage == WriteStage::WritingPrefix,
        "unexpected write stage {}",
        magic_enum::enum_name(write_stage));

    auto create_at_ms = Poco::Timestamp().epochMicroseconds() / 1000;

    // Init the common fields of DataPrefix.
    data_prefix.set_file_format(1);
    data_prefix.set_local_sequence(info.sequence);
    data_prefix.mutable_writer_info()->CopyFrom(info.writer);
    data_prefix.set_manifest_file_id(manifest_file_id);
    // Create data_writer.
    newDataWriter();

    CheckpointProto::ManifestFilePrefix manifest_prefix;
    manifest_prefix.set_file_format(1);
    manifest_prefix.set_local_sequence(info.sequence);
    manifest_prefix.set_last_local_sequence(info.last_sequence);
    manifest_prefix.set_create_at_ms(create_at_ms);
    manifest_prefix.mutable_writer_info()->CopyFrom(info.writer);
    manifest_writer->writePrefix(manifest_prefix);

    write_stage = WriteStage::WritingEdits;
}

CPDataDumpStats CPFilesWriter::writeEditsAndApplyCheckpointInfo(
    universal::PageEntriesEdit & edits,
    const CPFilesWriter::CompactOptions & options,
    bool manifest_only)
{
    RUNTIME_CHECK_MSG(
        write_stage == WriteStage::WritingEdits,
        "unexpected write stage {}",
        magic_enum::enum_name(write_stage));

    if (edits.empty())
        return {.has_new_data = false};

    CPDataDumpStats write_down_stats;
    for (size_t i = 0; i < static_cast<size_t>(StorageType::_MAX_STORAGE_TYPE_); ++i)
    {
        write_down_stats.num_keys[i] = 0;
        write_down_stats.num_bytes[i] = 0;
    }

    std::unordered_map<String, size_t> compact_stats;
    bool last_page_is_raft_data = true;

    // 1. Iterate all edits, find these entry edits without the checkpoint info
    //    and collect the lock files from applied entries.
    auto & records = edits.getMutRecords();
    write_down_stats.num_records = records.size();
    LOG_DEBUG(
        log,
        "Prepare to dump {} records, sequence={}, manifest_file_id={}",
        write_down_stats.num_records,
        sequence,
        manifest_file_id);
    for (auto & rec_edit : records)
    {
        StorageType id_storage_type = StorageType::Unknown;

        {
            id_storage_type = UniversalPageIdFormat::getUniversalPageIdType(rec_edit.page_id);
            // all keys are included in the manifest
            write_down_stats.num_keys[static_cast<size_t>(id_storage_type)] += 1;
            // this is the page data size of all latest version keys, including some uploaded in the
            // previous checkpoint
            write_down_stats.num_existing_bytes[static_cast<size_t>(id_storage_type)] += rec_edit.entry.size;

            if (id_storage_type == StorageType::LocalKV)
            {
                // These pages only contains local data which will not be dumped into checkpoint.
                continue;
            }
        }

        if (rec_edit.type == EditRecordType::VAR_EXTERNAL)
        {
            RUNTIME_CHECK_MSG(
                rec_edit.entry.checkpoint_info.is_valid && rec_edit.entry.checkpoint_info.data_location.data_file_id
                    && !rec_edit.entry.checkpoint_info.data_location.data_file_id->empty(),
                "the checkpoint info of external id is not set, record={}",
                rec_edit);
            // add lock for the s3 fullpath of external id
            locked_files.emplace(*rec_edit.entry.checkpoint_info.data_location.data_file_id);
            write_down_stats.num_ext_pages += 1;
            continue;
        }

        if (rec_edit.type != EditRecordType::VAR_ENTRY)
        {
            if (rec_edit.type == EditRecordType::VAR_REF)
            {
                write_down_stats.num_ref_pages += 1;
            }
            else if (rec_edit.type == EditRecordType::VAR_DELETE)
            {
                write_down_stats.num_delete_records += 1;
            }
            else
            {
                write_down_stats.num_other_records += 1;
            }
            continue;
        }

        assert(rec_edit.type == EditRecordType::VAR_ENTRY);
        // No need to read and write page data for the manifest only checkpoint.
        if (manifest_only)
        {
            continue;
        }
        bool is_compaction = false;
        if (rec_edit.entry.checkpoint_info.has_value())
        {
            const auto file_id = *rec_edit.entry.checkpoint_info.data_location.data_file_id;
            if (!options.compact_all_data && !options.file_ids.contains(file_id))
            {
                // add lock for the s3 fullpath that was written in the previous uploaded CheckpointDataFile
                locked_files.emplace(file_id);
                write_down_stats.num_pages_unchanged += 1;
                continue;
            }
            // else we rewrite this entry data to the data file generated by this checkpoint, so that
            // the old, fragmented data will be compacted. The outdated CheckpointDataFiles will be
            // removed by S3GCManager after the new manifest uploaded.
            is_compaction = true;
            compact_stats.try_emplace(file_id, 0).first->second += rec_edit.entry.size;
        }

        bool current_page_is_raft_data = (id_storage_type == StorageType::RaftEngine);
        if (current_write_size
                > 0 // If current_write_size is 0, data_writer is a empty file, not need to create a new one.
            && (current_page_is_raft_data != last_page_is_raft_data // Data type changed
                || (max_data_file_size != 0 && current_write_size >= max_data_file_size))) // or reach size limit.
        {
            newDataWriter();
        }
        last_page_is_raft_data = current_page_is_raft_data;

        // 2. For entry edits without the checkpoint info, or it is stored on an existing data file that needs compact,
        // write the entry data to the data file, and assign a new checkpoint info.
        Stopwatch sw;
        try
        {
            auto page = data_source->read({rec_edit.page_id, rec_edit.entry});
            RUNTIME_CHECK_MSG(
                page.isValid(),
                "failed to read page, record={} elapsed={:.3f}s",
                rec_edit,
                sw.elapsedSeconds());
            auto data_location
                = data_writer->write(rec_edit.page_id, rec_edit.version, page.data.begin(), page.data.size());
            // the page data size uploaded in this checkpoint
            write_down_stats.num_bytes[static_cast<size_t>(id_storage_type)] += rec_edit.entry.size;
            current_write_size += data_location.size_in_file;
            RUNTIME_CHECK(page.data.size() == rec_edit.entry.size, page.data.size(), rec_edit.entry.size);
            bool is_local_data_reclaimed
                = rec_edit.entry.checkpoint_info.has_value() && rec_edit.entry.checkpoint_info.is_local_data_reclaimed;
            rec_edit.entry.checkpoint_info = OptionalCheckpointInfo(data_location, true, is_local_data_reclaimed);
            locked_files.emplace(*data_location.data_file_id);
            if (is_compaction)
            {
                write_down_stats.compact_data_bytes += rec_edit.entry.size;
                write_down_stats.num_pages_compact += 1;
            }
            else
            {
                write_down_stats.incremental_data_bytes += rec_edit.entry.size;
                write_down_stats.num_pages_incremental += 1;
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "failed to read and write page, record={} elapsed={:.3f}s", rec_edit, sw.elapsedSeconds());
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }
    }

    LOG_DEBUG(log, "compact stats: {}", compact_stats);

    // 3. Write down everything to the manifest.
    manifest_writer->writeEdits(edits);

    write_down_stats.has_new_data = total_written_records + data_writer->writtenRecords() > 0;
    return write_down_stats;
}

std::vector<String> CPFilesWriter::writeSuffix()
{
    RUNTIME_CHECK_MSG(
        write_stage == WriteStage::WritingEdits,
        "unexpected write stage {}",
        magic_enum::enum_name(write_stage));

    manifest_writer->writeEditsFinish();
    manifest_writer->writeLocks(locked_files);
    manifest_writer->writeLocksFinish();

    data_writer->writeSuffix();
    data_writer->flush();
    manifest_writer->writeSuffix();
    manifest_writer->flush();

    write_stage = WriteStage::WritingFinished;
    return data_file_paths;
}

void CPFilesWriter::newDataWriter()
{
    if (data_writer != nullptr)
    {
        total_written_records += data_writer->writtenRecords();
        data_writer->writeSuffix();
        data_writer->flush();
    }
    current_write_size = 0;
    data_file_paths.push_back(fmt::format(
        fmt::runtime(data_file_path_pattern),
        fmt::arg("seq", sequence),
        fmt::arg("index", data_file_index)));

    data_writer = CPDataFileWriter::create({
        .file_path = data_file_paths.back(),
        .file_id = fmt::format(
            fmt::runtime(data_file_id_pattern),
            fmt::arg("seq", sequence),
            fmt::arg("index", data_file_index)),
    });
    data_prefix.set_create_at_ms(Poco::Timestamp().epochMicroseconds() / 1000);
    data_prefix.set_sub_file_index(data_file_index);
    data_writer->writePrefix(data_prefix);
    ++data_file_index;
}

void CPFilesWriter::abort()
{
    for (const auto & s : data_file_paths)
    {
        if (Poco::File f(s); f.exists())
        {
            f.remove();
        }
    }
    if likely (manifest_writer != nullptr)
    {
        manifest_writer->abort();
    }
}

} // namespace DB::PS::V3
