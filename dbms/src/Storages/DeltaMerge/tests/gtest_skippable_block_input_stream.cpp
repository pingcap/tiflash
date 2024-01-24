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

#include <Columns/ColumnsCommon.h>
#include <Common/Logger.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/LateMaterializationBlockInputStream.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <boost_wrapper/string.h>
#include <common/defines.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{

class MockFilterBlockInputStream : public IProfilingBlockInputStream
{
public:
    explicit MockFilterBlockInputStream(const BlockInputStreamPtr & input_)
        : input(input_)
        , e(time(nullptr))
    {}

    String getName() const override { return "MockFilter"; }

    Block getHeader() const override { return input->getHeader(); }

    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    Block readImpl(FilterPtr & res_filter, bool return_filter) override
    {
        assert(return_filter);
        auto blk = input->read();
        if (!blk)
            return blk;

        filter.resize(blk.rows());
        res_filter = &filter;
        size_t mode = e() % 3;
        if (mode == 0)
        {
            std::fill(filter.begin(), filter.end(), 1);
        }
        else if (mode == 1)
        {
            std::fill(filter.begin(), filter.end(), 0);
        }
        else
        {
            std::transform(filter.begin(), filter.end(), filter.begin(), [&e = e](auto) {
                return e() % 8192 == 0 ? 1 : 0;
            });
            filter[e() % blk.rows()] = 1; // should not be all 0.
        }
        total_filter.insert(total_filter.end(), res_filter->begin(), res_filter->end());
        return blk;
    }

public:
    IColumn::Filter total_filter{};

private:
    BlockInputStreamPtr input;
    IColumn::Filter filter{};
    std::default_random_engine e;
};

class SkippableBlockInputStreamTest : public SegmentTestBasic
{
protected:
    DB::LoggerPtr log = DB::Logger::get("SkippableBlockInputStreamTest");
    static constexpr auto SEG_ID = DELTA_MERGE_FIRST_SEGMENT_ID;
    RowKeyRanges read_ranges;


    std::shared_ptr<ConcatSkippableBlockInputStream<false>> getInputStream(
        const SegmentPtr & segment,
        const SegmentSnapshotPtr & snapshot,
        const ColumnDefines & columns_to_read,
        const RowKeyRanges & read_ranges)
    {
        auto enable_handle_clean_read = !hasColumn(columns_to_read, EXTRA_HANDLE_COLUMN_ID);
        constexpr auto is_fast_scan = true;
        auto enable_del_clean_read = !hasColumn(columns_to_read, TAG_COLUMN_ID);

        SkippableBlockInputStreamPtr stable_stream = snapshot->stable->getInputStream(
            *dm_context,
            columns_to_read,
            read_ranges,
            EMPTY_RS_OPERATOR,
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE,
            enable_handle_clean_read,
            is_fast_scan,
            enable_del_clean_read);

        auto columns_to_read_ptr = std::make_shared<ColumnDefines>(columns_to_read);
        SkippableBlockInputStreamPtr delta_stream = std::make_shared<DeltaValueInputStream>(
            *dm_context,
            snapshot->delta,
            columns_to_read_ptr,
            segment->getRowKeyRange());

        auto stream = std::dynamic_pointer_cast<ConcatSkippableBlockInputStream<false>>(stable_stream);
        stream->appendChild(delta_stream, snapshot->delta->getRows());
        return stream;
    }

    void testReadWithFilterCase(std::string_view seg_data)
    {
        auto seg_data_units = parseSegData(seg_data);
        for (const auto & unit : seg_data_units)
        {
            writeSegment(unit);
        }

        auto [segment, snapshot] = getSegmentForRead(SEG_ID);
        ColumnDefines columns_to_read = {
            getExtraHandleColumnDefine(options.is_common_handle),
            getVersionColumnDefine(),
        };

        auto stream1 = getInputStream(segment, snapshot, columns_to_read, read_ranges);
        auto stream2 = getInputStream(segment, snapshot, columns_to_read, read_ranges);

        stream1->readPrefix();
        stream2->readPrefix();

        std::default_random_engine e(time(nullptr));
        for (auto blk = stream1->read(); blk; blk = stream1->read())
        {
            IColumn::Filter filter(blk.rows(), 1);
            std::transform(filter.begin(), filter.end(), filter.begin(), [&e](auto) {
                return e() % 8192 == 0 ? 1 : 0;
            });
            filter[e() % blk.rows()] = 1; // should not be all 0.
            size_t passed_count = countBytesInFilter(filter);
            for (auto & col : blk)
            {
                col.column = col.column->filter(filter, passed_count);
            }
            auto blk2 = stream2->readWithFilter(filter);
            ASSERT_EQ(blk.startOffset(), blk2.startOffset());
            ASSERT_BLOCK_EQ(blk, blk2);
        }
        ASSERT_BLOCK_EQ(stream2->read(), Block{});
        stream1->readSuffix();
        stream2->readSuffix();
    }

    void testLateMaterializationCase(std::string_view seg_data)
    {
        auto seg_data_units = parseSegData(seg_data);
        for (const auto & unit : seg_data_units)
        {
            writeSegment(unit);
        }

        auto [segment, snapshot] = getSegmentForRead(SEG_ID);
        ColumnDefines columns_to_read = {
            getExtraHandleColumnDefine(options.is_common_handle),
            getVersionColumnDefine(),
        };

        BlockInputStreamPtr stream = getInputStream(segment, snapshot, columns_to_read, read_ranges);
        BlockInputStreamPtr filter_cloumn_stream = std::make_shared<MockFilterBlockInputStream>(stream);
        auto rest_column_stream = getInputStream(segment, snapshot, columns_to_read, read_ranges);

        size_t total_rows = snapshot->stable->getRows() + snapshot->delta->getRows();
        auto bitmap_filter = std::make_shared<BitmapFilter>(total_rows, 1);
        std::default_random_engine e(time(nullptr));
        for (size_t i = 0; i < 10; ++i)
        {
            size_t start = e() % total_rows;
            size_t limit = e() % (total_rows - start);
            bitmap_filter->set(start, limit, false);
        }

        auto late_materialization_stream = std::make_shared<LateMaterializationBlockInputStream>(
            columns_to_read,
            filter_cloumn_stream,
            rest_column_stream,
            bitmap_filter,
            DEFAULT_BLOCK_SIZE);
        late_materialization_stream->readPrefix();
        Blocks blks1;
        while (true)
        {
            auto blk = late_materialization_stream->read();
            if (!blk)
                break;
            blks1.emplace_back(std::move(blk));
        }
        Block block1 = vstackBlocks(std::move(blks1));
        late_materialization_stream->readSuffix();
        stream = getInputStream(segment, snapshot, columns_to_read, read_ranges);
        auto filter_stream = std::dynamic_pointer_cast<MockFilterBlockInputStream>(filter_cloumn_stream);
        IColumn::Filter * filter = &filter_stream->total_filter;
        bitmap_filter->rangeAnd(*filter, 0, total_rows);
        size_t passed_count = countBytesInFilter(*filter);
        Blocks blks2;
        while (true)
        {
            auto blk = stream->read();
            if (!blk)
                break;
            blks2.emplace_back(std::move(blk));
        }
        Block block2 = vstackBlocks(std::move(blks2));
        for (auto & col : block2)
        {
            col.column = col.column->filter(*filter, passed_count);
        }
        ASSERT_BLOCK_EQ(block1, block2);
    }

    void writeSegment(const SegDataUnit & unit)
    {
        const auto & type = unit.type;
        auto [begin, end] = unit.range;

        if (type == "d_mem")
        {
            SegmentTestBasic::writeSegment(SEG_ID, end - begin, begin);
        }
        else if (type == "d_mem_del")
        {
            SegmentTestBasic::writeSegmentWithDeletedPack(SEG_ID, end - begin, begin);
        }
        else if (type == "d_tiny")
        {
            SegmentTestBasic::writeSegment(SEG_ID, end - begin, begin);
            SegmentTestBasic::flushSegmentCache(SEG_ID);
        }
        else if (type == "d_tiny_del")
        {
            SegmentTestBasic::writeSegmentWithDeletedPack(SEG_ID, end - begin, begin);
            SegmentTestBasic::flushSegmentCache(SEG_ID);
        }
        else if (type == "d_big")
        {
            SegmentTestBasic::ingestDTFileIntoDelta(SEG_ID, end - begin, begin, false);
        }
        else if (type == "d_dr")
        {
            SegmentTestBasic::writeSegmentWithDeleteRange(SEG_ID, begin, end);
        }
        else if (type == "s")
        {
            SegmentTestBasic::writeSegment(SEG_ID, end - begin, begin);
            SegmentTestBasic::mergeSegmentDelta(SEG_ID);
        }
        else
        {
            RUNTIME_CHECK(false, type);
        }
    }
};

TEST_F(SkippableBlockInputStreamTest, InMemory1)
try
{
    testReadWithFilterCase("d_mem:[0, 1000)");
    testLateMaterializationCase("d_mem:[0, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory2)
try
{
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem:[0, 1000)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem:[0, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory3)
try
{
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem:[100, 200)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem:[100, 200)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory4)
try
{
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem:[-100, 100)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem:[-100, 100)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory5)
try
{
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem_del:[0, 1000)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem_del:[0, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory6)
try
{
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem_del:[100, 200)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem_del:[100, 200)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory7)
try
{
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem_del:[-100, 100)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem_del:[-100, 100)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Tiny1)
try
{
    testReadWithFilterCase("d_tiny:[100, 500)|d_mem:[200, 1000)");
    testLateMaterializationCase("d_tiny:[100, 500)|d_mem:[200, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, TinyDel1)
try
{
    testReadWithFilterCase("d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)");
    testLateMaterializationCase("d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, DeleteRange)
try
{
    testReadWithFilterCase("d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)");
    testLateMaterializationCase("d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Big)
try
{
    testReadWithFilterCase("d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)");
    testLateMaterializationCase("d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Stable1)
try
{
    testReadWithFilterCase("s:[0, 1024)|d_dr:[0, 1023)");
    testLateMaterializationCase("s:[0, 1024)|d_dr:[0, 1023)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Stable2)
try
{
    testReadWithFilterCase("s:[0, 102294)|d_dr:[0, 1023)");
    testLateMaterializationCase("s:[0, 102294)|d_dr:[0, 1023)");
}
CATCH


TEST_F(SkippableBlockInputStreamTest, Stable3)
try
{
    testReadWithFilterCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)");
    testLateMaterializationCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)");
}
CATCH


TEST_F(SkippableBlockInputStreamTest, Mix)
try
{
    testReadWithFilterCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)");
    testLateMaterializationCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)");
}
CATCH

} // namespace DB::DM::tests