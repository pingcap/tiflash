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

#include <Columns/ColumnsCommon.h>
#include <Common/Logger.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/RowKeyOrderedBlockInputStream.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/defines.h>
#include <gtest/gtest.h>

#include <boost/algorithm/string.hpp>


namespace DB::DM::tests
{

namespace
{

// "[a, b)" => std::pair{a, b}
template <typename T>
std::pair<T, T> parseRange(String & str_range)
{
    boost::algorithm::trim(str_range);
    RUNTIME_CHECK(str_range.front() == '[' && str_range.back() == ')', str_range);
    std::vector<String> values;
    str_range = str_range.substr(1, str_range.size() - 2);
    boost::split(values, str_range, boost::is_any_of(","));
    RUNTIME_CHECK(values.size() == 2, str_range);
    return {static_cast<T>(std::stol(values[0])), static_cast<T>(std::stol(values[1]))};
}

// "[a, b)|[c, d)" => [std::pair{a, b}, std::pair{c, d}]
template <typename T>
std::vector<std::pair<T, T>> parseRanges(std::string_view str_ranges)
{
    std::vector<String> ranges;
    boost::split(ranges, str_ranges, boost::is_any_of("|"));
    RUNTIME_CHECK(!ranges.empty(), str_ranges);
    std::vector<std::pair<T, T>> vector_ranges;
    vector_ranges.reserve(ranges.size());
    for (auto & r : ranges)
    {
        vector_ranges.emplace_back(parseRange<T>(r));
    }
    return vector_ranges;
}

struct SegDataUnit
{
    String type;
    std::pair<Int64, Int64> range;
};

// "type:[a, b)" => SegDataUnit
SegDataUnit parseSegDataUnit(String & s)
{
    boost::algorithm::trim(s);
    std::vector<String> values;
    boost::split(values, s, boost::is_any_of(":"));
    RUNTIME_CHECK(values.size() == 2, s);
    return SegDataUnit{boost::algorithm::trim_copy(values[0]), parseRange<Int64>(values[1])};
}

void check(const std::vector<SegDataUnit> & seg_data_units)
{
    RUNTIME_CHECK(!seg_data_units.empty());
    std::vector<size_t> stable_units;
    std::vector<size_t> mem_units;
    for (size_t i = 0; i < seg_data_units.size(); i++)
    {
        const auto & type = seg_data_units[i].type;
        if (type == "s")
        {
            stable_units.emplace_back(i);
        }
        else if (type == "d_mem" || type == "d_mem_del")
        {
            mem_units.emplace_back(i);
        }
        auto [begin, end] = seg_data_units[i].range;
        RUNTIME_CHECK(begin < end, begin, end);
    }
    RUNTIME_CHECK(stable_units.empty() || (stable_units.size() == 1 && stable_units[0] == 0));
    std::vector<size_t> expected_mem_units(mem_units.size());
    std::iota(expected_mem_units.begin(), expected_mem_units.end(), seg_data_units.size() - mem_units.size());
    RUNTIME_CHECK(mem_units == expected_mem_units, expected_mem_units, mem_units);
}

std::vector<SegDataUnit> parseSegData(std::string_view seg_data)
{
    std::vector<String> str_seg_data_units;
    boost::split(str_seg_data_units, seg_data, boost::is_any_of("|"));
    RUNTIME_CHECK(!str_seg_data_units.empty(), seg_data);
    std::vector<SegDataUnit> seg_data_units;
    seg_data_units.reserve(str_seg_data_units.size());
    for (auto & s : str_seg_data_units)
    {
        seg_data_units.emplace_back(parseSegDataUnit(s));
    }
    check(seg_data_units);
    return seg_data_units;
}

} // namespace

class SkippableBlockInputStreamTest : public SegmentTestBasic
{
protected:
    DB::LoggerPtr log = DB::Logger::get("SkippableBlockInputStreamTest");
    static constexpr auto SEG_ID = DELTA_MERGE_FIRST_SEGMENT_ID;
    RowKeyRanges read_ranges;


    SkippableBlockInputStreamPtr getInputStream(const SegmentPtr & segment,
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

        return std::make_shared<RowKeyOrderedBlockInputStream>(columns_to_read, stable_stream, delta_stream, snapshot->stable->getDMFilesRows(), dm_context->tracing_id);
    }

    void testSkipBlockCase(std::string_view seg_data, std::vector<size_t> skip_block_idxs = {})
    {
        auto seg_data_units = parseSegData(seg_data);
        for (const auto & unit : seg_data_units)
        {
            writeSegment(unit);
        }

        auto [segment, snapshot] = getSegmentForRead(SEG_ID);
        ColumnDefines columns_to_read = {getExtraHandleColumnDefine(options.is_common_handle),
                                         getVersionColumnDefine()};

        auto stream = getInputStream(segment, snapshot, columns_to_read, read_ranges);

        stream->readPrefix();
        std::vector<Block> expected_blks;
        for (auto blk = stream->read(); blk; blk = stream->read())
        {
            expected_blks.push_back(std::move(blk));
        }
        stream->readSuffix();

        stream = getInputStream(segment, snapshot, columns_to_read, read_ranges);

        stream->readPrefix();
        for (size_t i = 0; i < expected_blks.size(); ++i)
        {
            if (std::find(skip_block_idxs.begin(), skip_block_idxs.end(), i) != skip_block_idxs.end())
            {
                size_t skipped_rows = stream->skipNextBlock();
                ASSERT_EQ(skipped_rows, expected_blks[i].rows());
                continue;
            }
            auto blk = stream->read();
            ASSERT_BLOCK_EQ(expected_blks[i], blk);
        }
        ASSERT_BLOCK_EQ(stream->read(), Block{});
        stream->readSuffix();
    }

    void testReadWithFilterCase(std::string_view seg_data)
    {
        auto seg_data_units = parseSegData(seg_data);
        for (const auto & unit : seg_data_units)
        {
            writeSegment(unit);
        }

        auto [segment, snapshot] = getSegmentForRead(SEG_ID);
        ColumnDefines columns_to_read = {getExtraHandleColumnDefine(options.is_common_handle),
                                         getVersionColumnDefine()};

        auto stream1 = getInputStream(segment, snapshot, columns_to_read, read_ranges);
        auto stream2 = getInputStream(segment, snapshot, columns_to_read, read_ranges);

        stream1->readPrefix();
        stream2->readPrefix();

        std::default_random_engine e;
        for (auto blk = stream1->read(); blk; blk = stream1->read())
        {
            IColumn::Filter filter(blk.rows(), 1);
            std::transform(filter.begin(), filter.end(), filter.begin(), [&e](auto) { return e() % 2 == 0 ? 0 : 1; });
            size_t passed_count = countBytesInFilter(filter);
            for (auto & col : blk)
            {
                col.column = col.column->filter(filter, passed_count);
            }
            auto blk2 = stream2->readWithFilter(filter);
            ASSERT_BLOCK_EQ(blk, blk2);
        }
        ASSERT_BLOCK_EQ(stream2->read(), Block{});
        stream1->readSuffix();
        stream2->readSuffix();
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
    testSkipBlockCase("d_mem:[0, 1000)");
    testReadWithFilterCase("d_mem:[0, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory2)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem:[0, 1000)", {0});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem:[0, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory3)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem:[100, 200)", {3, 6, 9});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem:[100, 200)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory4)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem:[-100, 100)", {0, 1, 3, 4, 5, 6, 7, 8});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem:[-100, 100)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory5)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem_del:[0, 1000)", {4, 5, 6});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem_del:[0, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory6)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem_del:[100, 200)", {});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem_del:[100, 200)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory7)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem_del:[-100, 100)", {0, 1, 2, 3, 4, 5, 6, 7, 8});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem_del:[-100, 100)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Tiny1)
try
{
    testSkipBlockCase("d_tiny:[100, 500)|d_mem:[200, 1000)", {1, 2, 3, 4, 5, 6});
    testReadWithFilterCase("d_tiny:[100, 500)|d_mem:[200, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, TinyDel1)
try
{
    testSkipBlockCase("d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)", {7, 8, 9});
    testReadWithFilterCase("d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, DeleteRange)
try
{
    testSkipBlockCase("d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)", {1, 2, 3, 4, 5, 9});
    testReadWithFilterCase("d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Big)
try
{
    testSkipBlockCase("d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)", {1, 3, 4, 9});
    testReadWithFilterCase("d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Stable1)
try
{
    testSkipBlockCase("s:[0, 1024)|d_dr:[0, 1023)", {0});
    testReadWithFilterCase("s:[0, 1024)|d_dr:[0, 1023)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Stable2)
try
{
    testSkipBlockCase("s:[0, 102294)|d_dr:[0, 1023)", {2});
    testReadWithFilterCase("s:[0, 102294)|d_dr:[0, 1023)");
}
CATCH


TEST_F(SkippableBlockInputStreamTest, Stable3)
try
{
    testSkipBlockCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)", {0});
    testReadWithFilterCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)");
}
CATCH


TEST_F(SkippableBlockInputStreamTest, Mix)
try
{
    testSkipBlockCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)", {1, 2});
    testReadWithFilterCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)");
}
CATCH

} // namespace DB::DM::tests