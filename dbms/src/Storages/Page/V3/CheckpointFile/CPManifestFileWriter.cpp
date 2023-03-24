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

    if (edit.empty())
        return;

    CheckpointProto::ManifestFileEditsPart part;
    part.set_has_more(true);
    for (const auto & edit_record : edit.getRecords())
    {
        auto * out_record = part.add_edits();
        *out_record = edit_record.toProto();
    }
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

void CPManifestFileWriter::writeLocks(const std::unordered_set<String> & lock_files)
{
    if (write_stage < WriteStage::WritingEditsFinished)
        writeEditsFinish(); // Trying to fast-forward. There may be exceptions.
    if (write_stage > WriteStage::WritingLocks)
        RUNTIME_CHECK_MSG(false, "unexpected write stage {}", magic_enum::enum_name(write_stage));

    if (lock_files.empty())
        return;

    CheckpointProto::ManifestFileLocksPart part;
    part.set_has_more(true);
    for (const auto & lock_file : lock_files)
        part.add_locks()->set_name(lock_file);
    // Always sort the lock files in order to write out deterministic results.
    std::sort(
        part.mutable_locks()->begin(),
        part.mutable_locks()->end(),
        [](const CheckpointProto::LockFile & a, const CheckpointProto::LockFile & b) {
            return a.name() < b.name();
        });
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
