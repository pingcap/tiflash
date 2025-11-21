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

#include <Common/Logger.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/VersionChain/ColumnView.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>

namespace DB::DM::tests
{

class SegmentBitmapFilterTest
    : public SegmentTestBasic
    , public testing::WithParamInterface</*is_common_handle*/ bool>
{
public:
    void SetUp() override;

protected:
    DB::LoggerPtr log = DB::Logger::get("SegmentBitmapFilterTest");
    static constexpr auto SEG_ID = DELTA_MERGE_FIRST_SEGMENT_ID;
    ColumnPtr hold_row_id;
    ColumnPtr hold_handle;
    bool is_common_handle = false;

    void setRowKeyRange(Int64 begin, Int64 end, bool including_right_boundary);

    void writeSegmentGeneric(
        std::string_view seg_data,
        std::optional<std::tuple<Int64, Int64, bool>> seg_rowkey_range = std::nullopt);

    /*
    0----------------stable_rows----------------stable_rows + delta_rows <-- append
    | stable value space | delta value space ..........................  <-- append
    |--------------------|--ColumnFilePersisted--|ColumnFileInMemory...  <-- append
    |--------------------|-Tiny|DeleteRange|Big--|ColumnFileInMemory...  <-- append

    `seg_data`: s:[a, b)|d_tiny:[a, b)|d_tiny_del:[a, b)|d_big:[a, b)|d_dr:[a, b)|d_mem:[a, b)|d_mem_del
    - s: stable
    - d_tiny: delta ColumnFileTiny
    - d_tiny_del: delta ColumnFileTiny with delete flag
    - d_big: delta ColumnFileBig
    - d_dr: delta delete range

    Returns {row_id, handle}.
    */
    template <typename HandleType>
    void writeSegment(std::string_view seg_data, std::optional<std::tuple<Int64, Int64, bool>> seg_rowkey_range);

    void writeSegment(const SegDataUnit & unit);

    struct TestCase
    {
        std::string seg_data;
        size_t expected_size;
        std::string expected_row_id;
        std::string expected_handle;
        std::optional<std::tuple<Int64, Int64, bool>> seg_rowkey_range;

        RowKeyRanges read_ranges;

        const std::optional<String> expected_bitmap;
    };

    void runTestCaseGeneric(TestCase test_case, int caller_line);

    template <typename HandleType>
    void runTestCase(TestCase test_case, int caller_line);

    DMFilePackFilterResults loadPackFilterResults(const SegmentSnapshotPtr & snap, const RowKeyRanges & ranges);

    void checkHandle(PageIdU64 seg_id, std::string_view seq_ranges, int caller_line);

    struct CheckBitmapOptions
    {
        const PageIdU64 seg_id;
        const int caller_line; // For debug
        const UInt64 read_ts = std::numeric_limits<UInt64>::max();
        const std::optional<RowKeyRanges> read_ranges;
        const std::optional<String> expected_bitmap;
        const DMFilePackFilterResults rs_filter_results;

        String toDebugString() const
        {
            return fmt::format(
                "seg_id={}, caller_line={}, read_ts={}, read_ranges={}, expected_bitmap={}",
                seg_id,
                caller_line,
                read_ts,
                read_ranges,
                expected_bitmap);
        }
    };

    void checkBitmap(const CheckBitmapOptions & opt);

    UInt64 estimatedBytesOfInternalColumns(UInt64 start_ts, Int64 enable_version_chain);
};
} // namespace DB::DM::tests
