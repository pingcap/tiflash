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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/DeltaMerge/VersionChain/DMFileHandleIndex.h>

namespace DB::DM
{

struct DMContext;
struct SegmentSnapshot;
class ColumnFile;
class ColumnFileBig;
class ColumnFileDeleteRange;

template <Int64OrString Handle>
class VersionChain
{
public:
    VersionChain()
        : base_versions(std::make_shared<std::vector<RowID>>())
        , new_handle_to_row_ids(std::make_shared<std::map<Handle, RowID>>())
        , dmfile_or_delete_range_list(std::make_shared<std::vector<DMFileOrDeleteRange>>())
    {}

    [[nodiscard]] std::shared_ptr<const std::vector<RowID>> replaySnapshot(
        const DMContext & dm_context,
        const SegmentSnapshot & snapshot);

private:
    [[nodiscard]] UInt32 replayBlock(
        const DMContext & dm_context,
        const IColumnFileDataProviderPtr & data_provider,
        const ColumnFile & cf,
        UInt32 offset);
    [[nodiscard]] UInt32 replayColumnFileBig(const DMContext & dm_context, const ColumnFileBig & cf_big);
    [[nodiscard]] UInt32 replayDeleteRange(const ColumnFileDeleteRange & cf_delete_range);

    [[nodiscard]] std::optional<RowID> findBaseVersionFromDMFileOrDeleteRangeList(Handle h);

    DISALLOW_COPY_AND_MOVE(VersionChain);

    std::mutex mtx;
    UInt32 replayed_rows_and_deletes = 0; // delta.getRows() + delta.getDeletes()
    std::shared_ptr<std::vector<RowID>> base_versions; // base_versions->size() == delta.getRows()
    std::shared_ptr<std::map<Handle, RowID>> new_handle_to_row_ids; // TODO: shared_ptr is unneccessary
    using DMFileOrDeleteRange = std::variant<RowKeyRange, DMFileHandleIndex<Handle>>;
    std::shared_ptr<std::vector<DMFileOrDeleteRange>> dmfile_or_delete_range_list; // TODO: shared_ptr is unneccessary
};
} // namespace DB::DM
