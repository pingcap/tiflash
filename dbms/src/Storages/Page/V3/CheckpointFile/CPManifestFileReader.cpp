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

#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/CheckpointFile/ProtoHelper.h>

#include <magic_enum.hpp>

namespace DB::PS::V3
{

CheckpointProto::ManifestFilePrefix CPManifestFileReader::readPrefix()
{
    RUNTIME_CHECK_MSG(
        read_stage == ReadStage::ReadingPrefix,
        "unexpected read stage {}",
        magic_enum::enum_name(read_stage));

    CheckpointProto::ManifestFilePrefix ret;
    details::readMessageWithLength(*compressed_reader, ret);
    read_stage = ReadStage::ReadingEdits;
    return ret;
}

std::optional<universal::PageEntriesEdit> CPManifestFileReader::readEdits(
    CheckpointProto::StringsInternMap & strings_map)
{
    if (read_stage == ReadStage::ReadingEditsFinished)
        return std::nullopt;
    RUNTIME_CHECK_MSG(
        read_stage == ReadStage::ReadingEdits,
        "unexpected read stage {}",
        magic_enum::enum_name(read_stage));

    CheckpointProto::ManifestFileEditsPart part;
    details::readMessageWithLength(*compressed_reader, part);

    if (!part.has_more())
    {
        read_stage = ReadStage::ReadingEditsFinished;
        return std::nullopt;
    }

    universal::PageEntriesEdit edit;
    auto & records = edit.getMutRecords();
    for (const auto & remote_rec : part.edits())
        records.emplace_back(universal::PageEntriesEdit::EditRecord::fromProto(remote_rec, strings_map));

    return edit;
}

std::optional<std::unordered_set<String>> CPManifestFileReader::readLocks()
{
    if (read_stage == ReadStage::ReadingLocksFinished)
        return std::nullopt;
    if (read_stage == ReadStage::ReadingEditsFinished)
        read_stage = ReadStage::ReadingLocks;
    RUNTIME_CHECK_MSG(
        read_stage == ReadStage::ReadingLocks,
        "unexpected read stage {}",
        magic_enum::enum_name(read_stage));

    CheckpointProto::ManifestFileLocksPart part;
    details::readMessageWithLength(*compressed_reader, part);

    if (!part.has_more())
    {
        read_stage = ReadStage::ReadingLocksFinished;
        return std::nullopt;
    }

    std::unordered_set<String> locks;
    locks.reserve(part.locks_size());
    for (const auto & lock : part.locks())
        locks.emplace(lock.name());
    return locks;
}

} // namespace DB::PS::V3
