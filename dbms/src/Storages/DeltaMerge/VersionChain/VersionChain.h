// Copyright 2025 PingCAP, Inc.
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
#include <Storages/DeltaMerge/StableValueSpace.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/DeltaMerge/VersionChain/DMFileHandleIndex.h>
#include <Storages/DeltaMerge/VersionChain/NewHandleIndex.h>
#include <Storages/DeltaMerge/VersionChain/VersionChain_fwd.h>

namespace DB::DM
{

struct DMContext;
struct SegmentSnapshot;
class ColumnFile;
using ColumnFilePtr = std::shared_ptr<ColumnFile>;
using ColumnFiles = std::vector<ColumnFilePtr>;
class ColumnFileBig;
class ColumnFileDeleteRange;

namespace tests
{
class NewHandleIndexTest;
class VersionChainTest;
} // namespace tests

// VersionChain is to maintain the position of the base(oldest) version corresponding to each version in the Delta.
// If a handle A has three existing versions, from the oldest to the newest, they are A1, A2, A3.
// For a normal version chain, it looks like: A3 -> A2 -> A1.
// The version chain here is a little different, it looks like: A2 -> A1; A3 -> A1.
// The oldest existing version is called `base version` and the `base version` is the pivot of the version chain.

// In order to find the base version of a handle, we introduce `NewHandleIndex` and `DMFileHandleIndex` to help.
// - `NewHandleIndex` maintains the base versions in the delta, like `newly_inserted_handle -> row_id`.
// - DMFileHandleIndex encapsulates the handle column of a DMFile and provides interface for locating the position of corresponding handle.
template <ExtraHandleType HandleType>
class VersionChain
{
private:
    using HandleRefType = typename ExtraHandleRefType<HandleType>::type;

public:
    VersionChain()
        : base_versions(std::make_shared<std::vector<RowID>>())
    {}

    VersionChain & operator=(const VersionChain &) = delete;
    VersionChain(VersionChain &&) = delete;
    VersionChain & operator=(VersionChain &&) = delete;

    // Traverse the snapshot data in chronological order from the oldest to the newest,
    // and apply them to update the version chain.
    // `replayed_rows_and_deletes` is the number of rows and deletes has been replayed.
    // So data before `replayed_rows_and_deletes` will be skipped.
    // The return value is the `base_versions` that not older than the data of `snapshot`.
    [[nodiscard]] std::shared_ptr<const std::vector<RowID>> replaySnapshot(
        const DMContext & dm_context,
        const SegmentSnapshot & snapshot);

    size_t getBytes() const;

#ifdef DBMS_PUBLIC_GTEST
    [[nodiscard]] auto getReplayedRows() const { return base_versions->size(); }
    [[nodiscard]] auto deepCopy() const { return VersionChain(*this); }
#endif

private:
    VersionChain(const VersionChain & other)
        : replayed_rows_and_deletes(other.replayed_rows_and_deletes)
        , base_versions(std::make_shared<std::vector<RowID>>(*(other.base_versions)))
        , new_handle_to_row_ids(other.new_handle_to_row_ids)
        , dmfile_or_delete_range_list(other.dmfile_or_delete_range_list)
    {}

    [[nodiscard]] std::shared_ptr<const std::vector<RowID>> replaySnapshotImpl(
        const DMContext & dm_context,
        const SegmentSnapshot & snapshot);
    void resetImpl()
    {
        replayed_rows_and_deletes = 0;
        base_versions = std::make_shared<std::vector<RowID>>();
        new_handle_to_row_ids = NewHandleIndex<HandleType>{};
        dmfile_or_delete_range_list = std::vector<DMFileOrDeleteRange>{};
    }

    // Handling normal write requests. Mainly for ColumnFileInMemory and ColumnFileTiny.
    [[nodiscard]] UInt32 replayBlock(
        const DMContext & dm_context,
        const IColumnFileDataProviderPtr & data_provider,
        const ColumnFile & cf,
        UInt32 offset,
        UInt32 stable_rows,
        bool calculate_read_packs,
        DeltaValueReader & delta_reader);

    [[nodiscard]] UInt32 replayColumnFileBig(
        const DMContext & dm_context,
        const ColumnFileBig & cf_big,
        UInt32 stable_rows,
        const StableValueSpace::Snapshot & stable,
        std::span<const ColumnFilePtr> preceding_cfs,
        DeltaValueReader & delta_reader);

    [[nodiscard]] UInt32 replayDeleteRange(
        const ColumnFileDeleteRange & cf_delete_range,
        DeltaValueReader & delta_reader,
        UInt32 stable_rows);

    [[nodiscard]] std::optional<RowID> findBaseVersionFromDMFileOrDeleteRangeList(
        const DMContext & dm_context,
        HandleRefType h);

    template <typename Iter>
    void calculateReadPacks(Iter begin, Iter end);

    void cleanHandleColumn();

    template <typename Iter>
    void replayHandles(
        const DMContext & dm_context,
        Iter begin,
        Iter end,
        UInt32 stable_rows,
        DeltaValueReader & delta_reader);

    static DeltaValueReader createDeltaValueReader(const DMContext & dm_context, const DeltaSnapshotPtr & delta_snap);

    friend class tests::NewHandleIndexTest;
    friend class tests::VersionChainTest;

    mutable std::mutex mtx;
    UInt32 replayed_rows_and_deletes = 0; // delta.getRows() + delta.getDeletes()
    // After replaySnapshot, base_versions->size() == delta.getRows().
    // The records in delta correspond one-to-one with base_versions.
    // Base version means the oldest version that has not been garbage collected yet.
    // (*base_versions)[n] is the row id of the oldest version of the n-th record in delta.
    // And different versions of the same record has the same base version except the base version itself.
    // The previous version of the base version is NotExistRowID.
    // Therefore, base version of a record acts as a pivot.
    std::shared_ptr<std::vector<RowID>> base_versions;
    NewHandleIndex<HandleType> new_handle_to_row_ids;
    using DMFileOrDeleteRange = std::variant<RowKeyRange, DMFileHandleIndex<HandleType>>;
    std::vector<DMFileOrDeleteRange> dmfile_or_delete_range_list;
};

inline GenericVersionChainPtr createVersionChain(bool is_common_handle)
{
    if (is_common_handle)
        return std::make_shared<GenericVersionChain>(std::in_place_type<VersionChain<String>>);
    else
        return std::make_shared<GenericVersionChain>(std::in_place_type<VersionChain<Int64>>);
}

inline size_t getVersionChainBytes(const GenericVersionChain & version_chain)
{
    return std::visit([](auto && v) { return v.getBytes(); }, version_chain);
}

enum class VersionChainMode : Int64
{
    // Generating MVCC bitmap by using delta index.
    Disabled = 0,
    // Generating MVCC bitmap by using version chain.
    Enabled = 1,
};

} // namespace DB::DM
