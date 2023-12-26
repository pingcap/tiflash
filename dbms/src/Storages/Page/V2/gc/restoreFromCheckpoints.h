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
#include <Storages/Page/V2/PageStorage.h>

#include <optional>
#include <utility>

namespace DB::PS::V2
{
template <class MergineQueue>
static std::tuple<std::optional<PageFile>, std::optional<WriteBatch::SequenceID>, PageFileSet> //
restoreFromCheckpoints(
    MergineQueue & merging_queue,
    PageStorage::VersionedPageEntries & version_set,
    PageStorage::StatisticsInfo & info,
    const String & storage_name,
    LoggerPtr logger)
{
    // The sequence number of checkpoint. We should ignore the WriteBatch with
    // smaller number than checkpoint's.
    WriteBatch::SequenceID checkpoint_wb_sequence = 0;

    std::vector<PageFile> checkpoints;

    PageEntriesEdit last_checkpoint_edits;
    PageFileIdAndLevel last_checkpoint_file_id;
    // Collect all checkpoints file, but just restore from the latest checkpoint.
    while (!merging_queue.empty() //
           && merging_queue.top()->belongingPageFile().getType() == PageFile::Type::Checkpoint)
    {
        auto reader = merging_queue.top();
        merging_queue.pop();

        last_checkpoint_edits = reader->getEdits();
        last_checkpoint_file_id = reader->fileIdLevel();
        checkpoint_wb_sequence = reader->writeBatchSequence();

        checkpoints.emplace_back(reader->belongingPageFile());
    }
    if (checkpoints.empty())
        return {std::nullopt, std::nullopt, {}};

    // Old checkpoints can be removed
    PageFileSet page_files_to_drop;
    for (size_t i = 0; i < checkpoints.size() - 1; ++i)
        page_files_to_drop.emplace(checkpoints[i]);
    try
    {
        // Apply edits from latest checkpoint
        version_set.apply(last_checkpoint_edits);
        info.mergeEdits(last_checkpoint_edits);
    }
    catch (Exception & e)
    {
        /// TODO: Better diagnostics.
        throw;
    }

    if (!checkpoints.empty() && checkpoint_wb_sequence == 0)
    {
        // backward compatibility
        while (!merging_queue.empty() && merging_queue.top()->fileIdLevel() <= last_checkpoint_file_id)
        {
            auto reader = merging_queue.top();
            LOG_INFO(
                logger,
                "{} Removing old PageFile: {} after restore checkpoint PageFile_{}_{}",
                storage_name,
                reader->belongingPageFile().toString(),
                last_checkpoint_file_id.first,
                last_checkpoint_file_id.second);
            if (reader->writeBatchSequence() != 0)
            {
                throw Exception(
                    "Try to remove old PageFile: " + reader->belongingPageFile().toString()
                        + " after restore checkpoint PageFile_" + DB::toString(last_checkpoint_file_id.first) + "_"
                        + DB::toString(last_checkpoint_file_id.second)
                        + ", but write batch sequence is not zero: " + DB::toString(reader->writeBatchSequence()),
                    ErrorCodes::LOGICAL_ERROR);
            }

            // This file is older that last_checkpoint, can be removed later
            page_files_to_drop.emplace(reader->belongingPageFile());
            merging_queue.pop();
        }
    }
    LOG_INFO(
        logger,
        "{} restore {} from checkpoint PageFile_{}_{} sequence: {}",
        storage_name,
        info.toString(),
        last_checkpoint_file_id.first,
        last_checkpoint_file_id.second,
        checkpoint_wb_sequence);
    // The latest checkpoint, the WriteBatch's sequence of latest checkpoint, old PageFiles that somehow have not been clean before
    return {checkpoints.back(), checkpoint_wb_sequence, page_files_to_drop};
}

} // namespace DB::PS::V2
