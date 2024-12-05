// Copyright 2024 PingCAP, Inc.
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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/DeltaMerge/VersionChain/DMFileHandleIndex.h>

namespace DB::DM
{

template <Int64OrString Handle>
class VersionChain
{
public:
    // TODO: just for compile test
    VersionChain();

    VersionChain(const Context & global_context, const Segment & seg);

    DISALLOW_COPY_AND_MOVE(VersionChain);

    std::shared_ptr<const std::vector<RowID>> replaySnapshot(
        const DMContext & dm_context,
        const SegmentSnapshot & snapshot);

private:
    UInt32 replayBlock(
        const DMContext & dm_context,
        const IColumnFileDataProviderPtr & data_provider,
        const ColumnFile & cf,
        UInt32 offset);
    UInt32 replayColumnFileBig(const DMContext & dm_context, const ColumnFileBig & cf_big);
    UInt32 replayDeleteRange(const ColumnFileDeleteRange & cf_delete_range);

    std::optional<RowID> findBaseVersionFromDMFileOrDeleteRangeList(Handle h);

    std::mutex mtx;
    UInt32 replayed_rows_and_deletes = 0; // delta.getRows() + delta.getDeletes()
    std::shared_ptr<std::vector<RowID>> base_versions; // base_versions->size() == delta.getRows()
    std::shared_ptr<std::map<Handle, RowID>> new_handle_to_row_ids;
    using DMFileOrDeleteRange = std::variant<RowKeyRange, DMFileHandleIndex<Handle>>;
    std::shared_ptr<std::vector<DMFileOrDeleteRange>> dmfile_or_delete_range_list;
};
} // namespace DB::DM
