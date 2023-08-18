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

#include <Storages/Page/V3/CheckpointFile/CPDataFileWriter.h>

namespace DB::PS::V3
{

void CPDataFileWriter::writePrefix(const CheckpointProto::DataFilePrefix & prefix)
{
    RUNTIME_CHECK_MSG(
        write_stage == WriteStage::WritingPrefix,
        "unexpected write stage {}",
        magic_enum::enum_name(write_stage));

    std::string json;
    google::protobuf::util::MessageToJsonString(prefix, &json);
    writeStringBinary(json, *file_writer);

    write_stage = WriteStage::WritingRecords;
}

CheckpointLocation CPDataFileWriter::write(UniversalPageId page_id, PageVersion version, const char * data, size_t n)
{
    RUNTIME_CHECK_MSG(
        write_stage == WriteStage::WritingRecords,
        "unexpected write stage {}",
        magic_enum::enum_name(write_stage));

    // Every record is prefixed with the length, so that this data file can be parsed standalone.
    writeIntBinary(n, *file_writer);

    // TODO: getMaterializedBytes only works for FramedChecksumWriteBuffer, but does not work for a normal WriteBufferFromFile.
    //       There must be something wrong and should be fixed.
    // uint64_t file_offset = file_writer->getMaterializedBytes();
    // file_writer->write(data, n);
    // uint64_t write_n = file_writer->getMaterializedBytes() - file_offset;

    uint64_t file_offset = file_writer->count();
    file_writer->write(data, n);
    uint64_t write_n = file_writer->count() - file_offset;
    RUNTIME_CHECK(write_n == n, write_n, n); // Note: When we add compression later, write_n == n may be false.

    auto * suffix_record = file_suffix.add_records();
    suffix_record->set_page_id(page_id.asStr());
    suffix_record->set_version_sequence(version.sequence);
    suffix_record->set_version_epoch(version.epoch);
    suffix_record->set_offset_in_file(file_offset);
    suffix_record->set_size_in_file(write_n);

    return CheckpointLocation{
        .data_file_id = file_id,
        .offset_in_file = file_offset,
        .size_in_file = write_n,
    };
}

void CPDataFileWriter::writeSuffix()
{
    if (write_stage == WriteStage::WritingFinished)
        return; // writeSuffix can be called multiple times without causing issues.
    if (write_stage != WriteStage::WritingRecords)
        RUNTIME_CHECK_MSG(false, "unexpected write stage {}", magic_enum::enum_name(write_stage));

    std::string json;
    google::protobuf::util::MessageToJsonString(file_suffix, &json);
    writeStringBinary(json, *file_writer);

    write_stage = WriteStage::WritingFinished;
}

} // namespace DB::PS::V3
