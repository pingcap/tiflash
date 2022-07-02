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
#pragma once

#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

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
    };

public:
    void reloadWithOptions(SegmentTestOptions config);

    std::optional<PageId> splitSegment(PageId segment_id);
    void mergeSegment(PageId left_segment_id, PageId right_segment_id);
    void mergeSegmentDelta(PageId segment_id);
    void flushSegmentCache(PageId segment_id);
    void writeSegment(PageId segment_id, UInt64 write_rows = 100);
    void writeSegmentWithDeletedPack(PageId segment_id);
    void deleteRangeSegment(PageId segment_id);


    void writeRandomSegment();
    void writeRandomSegmentWithDeletedPack();
    void deleteRangeRandomSegment();
    void splitRandomSegment();
    void mergeRandomSegment();
    void mergeDeltaRandomSegment();
    void flushCacheRandomSegment();

    void randomSegmentTest(size_t operator_count);

    PageId createNewSegmentWithSomeData();
    size_t getSegmentRowNumWithoutMVCC(PageId segment_id);
    size_t getSegmentRowNum(PageId segment_id);
    void checkSegmentRow(PageId segment_id, size_t expected_row_num);
    std::pair<Int64, Int64> getSegmentKeyRange(SegmentPtr segment);

protected:
    // <segment_id, segment_ptr>
    std::map<PageId, SegmentPtr> segments;

    enum SegmentOperaterType
    {
        Write = 0,
        DeleteRange,
        Split,
        Merge,
        MergeDelta,
        FlushCache,
        WriteDeletedPack,
        SegmentOperaterMax
    };

    const std::vector<std::function<void()>> segment_operator_entries = {
        [this] { writeRandomSegment(); },
        [this] { deleteRangeRandomSegment(); },
        [this] { splitRandomSegment(); },
        [this] { mergeRandomSegment(); },
        [this] { mergeDeltaRandomSegment(); },
        [this] { flushCacheRandomSegment(); },
        [this] {
            writeRandomSegmentWithDeletedPack();
        }};

    PageId getRandomSegmentId();

    std::pair<PageId, PageId> getRandomMergeablePair();

    RowKeyRange commanHandleKeyRange();

    SegmentPtr reload(bool is_common_handle, const ColumnDefinesPtr & pre_define_columns = {}, DB::Settings && db_settings = DB::Settings());

    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefinesPtr & columns);

    const ColumnDefinesPtr & tableColumns() const { return table_columns; }

    DMContext & dmContext() { return *dm_context; }

protected:
    /// all these var lives as ref in dm_context
    std::unique_ptr<StoragePathPool> storage_path_pool;
    std::unique_ptr<StoragePool> storage_pool;
    /// dm_context
    std::unique_ptr<DMContext> dm_context;
    ColumnDefinesPtr table_columns;
    DM::DeltaMergeStore::Settings settings;

    SegmentPtr root_segment;
    UInt64 version = 0;
    SegmentTestOptions options;
};
} // namespace tests
} // namespace DM
} // namespace DB