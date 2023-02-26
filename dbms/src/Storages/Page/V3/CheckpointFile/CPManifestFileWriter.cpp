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

#include <Storages/Page/V3/CheckpointFile/CPManifestFileWriter.h>
#include <Storages/Page/V3/CheckpointFile/ProtoHelper.h>

#include <magic_enum.hpp>

namespace DB::PS::V3
{

void CPManifestFileWriter::writePrefix(const CheckpointProto::ManifestFilePrefix & prefix)
{
    RUNTIME_CHECK_MSG(write_stage == WriteStage::WritingPrefix, "unexpected write stage {}", magic_enum::enum_name(write_stage));

    details::writeMessageWithLength(*compressed_writer, prefix);
    write_stage = WriteStage::WritingEdits;
}

void CPManifestFileWriter::flush()
{
    compressed_writer->next();
    file_writer->next();
    file_writer->sync();
}

void CPManifestFileWriter::writeEdits(const universal::PageEntriesEdit & edit)
{
    if (write_stage != WriteStage::WritingEdits)
        RUNTIME_CHECK_MSG(false, "unexpected write stage {}", magic_enum::enum_name(write_stage));

    CheckpointProto::ManifestFileEditsPart part;
    for (const auto & edit_record : edit.getRecords())
    {
        auto * out_record = part.add_edits();
        *out_record = edit_record.toProto();
    }
    part.set_has_more(true);
    details::writeMessageWithLength(*compressed_writer, part);
}

void CPManifestFileWriter::writeEditsFinish()
{
    if (write_stage == WriteStage::WritingEditsFinished)
        return; // Ignore calling finish multiple times.
    if (write_stage != WriteStage::WritingEdits)
        RUNTIME_CHECK_MSG(false, "unexpected write stage {}", magic_enum::enum_name(write_stage));

    CheckpointProto::ManifestFileEditsPart part;
    part.set_has_more(false);
    details::writeMessageWithLength(*compressed_writer, part);

    write_stage = WriteStage::WritingEditsFinished;
}

void CPManifestFileWriter::writeLocks()
{
    if (write_stage < WriteStage::WritingEditsFinished)
        writeEditsFinish(); // Trying to fast-forward. There may be exceptions.
    if (write_stage > WriteStage::WritingLocks)
        RUNTIME_CHECK_MSG(false, "unexpected write stage {}", magic_enum::enum_name(write_stage));

    // Not implemented. Currently write nothing else.

    CheckpointProto::ManifestFileLocksPart part;
    part.set_has_more(true);
    details::writeMessageWithLength(*compressed_writer, part);

    write_stage = WriteStage::WritingLocks;
}

void CPManifestFileWriter::writeLocksFinish()
{
    if (write_stage == WriteStage::WritingLocksFinished)
        return; // Ignore calling finish multiple times.
    if (write_stage < WriteStage::WritingEditsFinished)
        writeEditsFinish(); // Trying to fast-forward. There may be exceptions.
    if (write_stage > WriteStage::WritingLocksFinished)
        RUNTIME_CHECK_MSG(false, "unexpected write stage {}", magic_enum::enum_name(write_stage));

    CheckpointProto::ManifestFileLocksPart part;
    part.set_has_more(false);
    details::writeMessageWithLength(*compressed_writer, part);

    write_stage = WriteStage::WritingLocksFinished;
}

void CPManifestFileWriter::writeSuffix()
{
    if (write_stage == WriteStage::WritingFinished)
        return;
    if (write_stage < WriteStage::WritingLocksFinished)
        writeLocksFinish(); // Trying to fast-forward. There may be exceptions.

    // Currently we do nothing in write suffix.

    write_stage = WriteStage::WritingFinished;
}

} // namespace DB::PS::V3
