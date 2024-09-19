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

#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <random>
#include <vector>

namespace DB
{
namespace DM
{
namespace tests
{
class SegmentTestBasic : public DB::base::TiFlashStorageTestBasic
{
public:
    struct SegmentTestOptions
    {
        bool is_common_handle = false;
        DB::Settings db_settings;
    };

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        reloadWithOptions({});
    }

public:
    void reloadWithOptions(SegmentTestOptions config);

    /**
     * When `check_rows` is true, it will compare the rows num before and after the segment update.
     * So if there is some write during the segment update, it will report false failure if `check_rows` is true.
     */
    std::optional<PageIdU64> splitSegment(
        PageIdU64 segment_id,
        Segment::SplitMode split_mode = Segment::SplitMode::Auto,
        bool check_rows = true);
    std::optional<PageIdU64> splitSegmentAt(
        PageIdU64 segment_id,
        Int64 split_at,
        Segment::SplitMode split_mode = Segment::SplitMode::Auto,
        bool check_rows = true);
    void mergeSegment(const std::vector<PageIdU64> & segments, bool check_rows = true);
    void mergeSegmentDelta(PageIdU64 segment_id, bool check_rows = true);
    void flushSegmentCache(PageIdU64 segment_id);

    /**
     * When begin_key is specified, new rows will be written from specified key. Otherwise, new rows may be
     * written randomly in the segment range.
     */
    void writeSegment(PageIdU64 segment_id, UInt64 write_rows = 100, std::optional<Int64> start_at = std::nullopt);
    void ingestDTFileIntoDelta(
        PageIdU64 segment_id,
        UInt64 write_rows = 100,
        std::optional<Int64> start_at = std::nullopt,
        bool clear = false);
    void ingestDTFileByReplace(
        PageIdU64 segment_id,
        UInt64 write_rows = 100,
        std::optional<Int64> start_at = std::nullopt,
        bool clear = false);
    void writeSegmentWithDeletedPack(
        PageIdU64 segment_id,
        UInt64 write_rows = 100,
        std::optional<Int64> start_at = std::nullopt);
    void deleteRangeSegment(PageIdU64 segment_id);

    /**
     * This function does not check rows.
     */
    void replaceSegmentData(PageIdU64 segment_id, const DMFilePtr & file, SegmentSnapshotPtr snapshot = nullptr);
    void replaceSegmentData(PageIdU64 segment_id, const Block & block, SegmentSnapshotPtr snapshot = nullptr);

    /**
     * This function does not check rows.
     * Returns whether replace is successful.
     */
    bool replaceSegmentStableData(PageIdU64 segment_id, const DMFilePtr & file);

    /**
     * Returns whether segment stable index is created.
     */
    bool ensureSegmentStableIndex(PageIdU64 segment_id, IndexInfosPtr local_index_infos);

    Block prepareWriteBlock(Int64 start_key, Int64 end_key, bool is_deleted = false);
    Block prepareWriteBlockInSegmentRange(
        PageIdU64 segment_id,
        UInt64 total_write_rows,
        std::optional<Int64> write_start_key = std::nullopt,
        bool is_deleted = false);

    size_t getSegmentRowNumWithoutMVCC(PageIdU64 segment_id);
    size_t getSegmentRowNum(PageIdU64 segment_id);
    bool isSegmentDefinitelyEmpty(PageIdU64 segment_id);

    PageIdU64 getRandomSegmentId();

    /**
     * You must pass at least 2 segments. Checks whether all segments passed in are sharing the same stable.
     */
    [[nodiscard]] bool areSegmentsSharingStable(const PageIdU64s & segments_id) const;

    std::pair<Int64, Int64> getSegmentKeyRange(PageIdU64 segment_id) const;

    void printFinishedOperations() const;

    std::vector<Block> readSegment(PageIdU64 segment_id, bool need_row_id, const RowKeyRanges & ranges);
    ColumnPtr getSegmentRowId(PageIdU64 segment_id, const RowKeyRanges & ranges);
    ColumnPtr getSegmentHandle(PageIdU64 segment_id, const RowKeyRanges & ranges);
    void writeSegmentWithDeleteRange(PageIdU64 segment_id, Int64 begin, Int64 end);
    RowKeyValue buildRowKeyValue(Int64 key);
    static RowKeyRange buildRowKeyRange(Int64 begin, Int64 end);

    size_t getPageNumAfterGC(StorageType type, NamespaceID ns_id) const;

    std::set<PageIdU64> getAliveExternalPageIdsWithoutGC(NamespaceID ns_id) const;
    std::set<PageIdU64> getAliveExternalPageIdsAfterGC(NamespaceID ns_id) const;

protected:
    std::mt19937 random;

    // <segment_id, segment_ptr>
    std::map<PageIdU64, SegmentPtr> segments;

    // <name, number_of_success_runs>
    std::map<std::string, size_t> operation_statistics;

    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefinesPtr & columns);

    const ColumnDefinesPtr & tableColumns() const { return table_columns; }

    virtual Block prepareWriteBlockImpl(Int64 start_key, Int64 end_key, bool is_deleted);

    virtual void prepareColumns(const ColumnDefinesPtr &) {}

    /**
     * Reload a new DMContext according to latest storage status.
     * For example, if you have changed the settings, you should grab a new DMContext.
     */
    void reloadDMContext();
    std::unique_ptr<DMContext> createDMContext();

    std::pair<SegmentPtr, SegmentSnapshotPtr> getSegmentForRead(PageIdU64 segment_id);

private:
    SegmentPtr reload(bool is_common_handle, const ColumnDefinesPtr & pre_define_columns, DB::Settings && db_settings);

protected:
    inline static constexpr PageIdU64 NAMESPACE_ID = 100;

    /// all these var lives as ref in dm_context
    std::shared_ptr<StoragePathPool> storage_path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    /// dm_context
    std::unique_ptr<DMContext> dm_context;
    ColumnDefinesPtr table_columns;
    DM::DeltaMergeStore::Settings settings;

    SegmentPtr root_segment;
    UInt64 version = 0;
    SegmentTestOptions options;

    LoggerPtr logger_op;
    LoggerPtr logger;
};
} // namespace tests
} // namespace DM
} // namespace DB
