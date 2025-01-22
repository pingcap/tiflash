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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider_fwd.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/DeltaMerge/VersionChain/DMFileHandleIndex.h>

#include <span>

namespace DB::DM
{

struct DMContext;
struct SegmentSnapshot;
class ColumnFile;
class ColumnFileBig;
class ColumnFileDeleteRange;

template <ExtraHandleType HandleType>
class VersionChain
{
public:
    VersionChain()
        : base_versions(std::make_shared<std::vector<RowID>>())
    {}

    // Deep copy, only use for micro-benchmark or unit-tests.
    VersionChain(const VersionChain & other)
        : replayed_rows_and_deletes(other.replayed_rows_and_deletes)
        , base_versions(std::make_shared<std::vector<RowID>>(*(other.base_versions)))
        , new_handle_to_row_ids(other.new_handle_to_row_ids)
        , dmfile_or_delete_range_list(other.dmfile_or_delete_range_list)
    {}

    VersionChain & operator=(const VersionChain &) = delete;
    VersionChain(VersionChain &&) = delete;
    VersionChain & operator=(VersionChain &&) = delete;

    [[nodiscard]] std::shared_ptr<const std::vector<RowID>> replaySnapshot(
        const DMContext & dm_context,
        const SegmentSnapshot & snapshot);

    [[nodiscard]] UInt32 getReplayedRows() const { return base_versions->size(); }

private:
    [[nodiscard]] UInt32 replayBlock(
        const DMContext & dm_context,
        const IColumnFileDataProviderPtr & data_provider,
        const ColumnFile & cf,
        const UInt32 offset,
        const UInt32 stable_rows,
        const bool calculate_read_packs);
    [[nodiscard]] UInt32 replayColumnFileBig(
        const DMContext & dm_context,
        const ColumnFileBig & cf_big,
        const UInt32 stable_rows);
    [[nodiscard]] UInt32 replayDeleteRange(const ColumnFileDeleteRange & cf_delete_range);

    template <HandleRefType HandleRef>
    [[nodiscard]] std::optional<RowID> findBaseVersionFromDMFileOrDeleteRangeList(
        const DMContext & dm_context,
        HandleRef h);
    template <typename Iterator>
    void calculateReadPacks(Iterator begin, Iterator end);
    void cleanHandleColumn();

    static std::pair<HandleType, HandleType> convertRowKeyRange(const RowKeyRange & range);

    std::mutex mtx;
    UInt32 replayed_rows_and_deletes = 0; // delta.getRows() + delta.getDeletes()
    std::shared_ptr<std::vector<RowID>> base_versions; // base_versions->size() == delta.getRows()
    std::map<HandleType, RowID, std::less<>> new_handle_to_row_ids;
    using DMFileOrDeleteRange = std::variant<RowKeyRange, DMFileHandleIndex<HandleType>>;
    std::vector<DMFileOrDeleteRange> dmfile_or_delete_range_list;
};

inline std::variant<VersionChain<Int64>, VersionChain<String>> createVersionChain(bool is_common_handle)
{
    if (is_common_handle)
        return std::variant<VersionChain<Int64>, VersionChain<String>>{std::in_place_type<VersionChain<String>>};
    else
        return std::variant<VersionChain<Int64>, VersionChain<String>>{std::in_place_type<VersionChain<Int64>>};
}
} // namespace DB::DM
