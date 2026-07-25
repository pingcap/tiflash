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
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/LateMaterializationBlockInputStream.h>
#include <Storages/DeltaMerge/MultiStageLateMaterializationBlockInputStream.h>
#include <Storages/DeltaMerge/tests/gtest_segment_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <boost_wrapper/string.h>
#include <common/defines.h>
#include <gtest/gtest.h>

#include <mutex>

namespace DB::DM::tests
{
namespace
{
constexpr ColumnID MULTI_STAGE_FILTER_COL_ID = 101;
constexpr ColumnID MULTI_STAGE_REST_COL_ID = 102;
const String MULTI_STAGE_FILTER_COL_NAME = "f";
const String MULTI_STAGE_REST_COL_NAME = "v";
const String MULTI_STAGE_FILTER_TMP_COL_NAME = "__residual_filter";
const String MULTI_STAGE_FILTER_CONST_COL_NAME = "__residual_filter_const";

ColumnWithTypeAndName makeUInt8Column(const std::vector<UInt8> & values, const String & name, ColumnID column_id)
{
    auto type = std::make_shared<DataTypeUInt8>();
    auto column = type->createColumn();
    for (const auto value : values)
        column->insert(Field(static_cast<UInt64>(value)));
    return {std::move(column), type, name, column_id};
}

ColumnWithTypeAndName makeInt64Column(const std::vector<Int64> & values, const String & name, ColumnID column_id)
{
    auto type = std::make_shared<DataTypeInt64>();
    auto column = type->createColumn();
    for (const auto value : values)
        column->insert(Field(value));
    return {std::move(column), type, name, column_id};
}

Block makeMultiStageBlock(
    UInt64 start_offset,
    std::optional<std::vector<UInt8>> filter_values,
    std::optional<std::vector<Int64>> rest_values)
{
    Block block;
    if (filter_values)
        block.insert(makeUInt8Column(*filter_values, MULTI_STAGE_FILTER_COL_NAME, MULTI_STAGE_FILTER_COL_ID));
    if (rest_values)
        block.insert(makeInt64Column(*rest_values, MULTI_STAGE_REST_COL_NAME, MULTI_STAGE_REST_COL_ID));
    block.setStartOffset(start_offset);
    return block;
}

IColumn::Filter makeFilter(std::initializer_list<UInt8> values)
{
    IColumn::Filter filter;
    filter.assign(values.begin(), values.end());
    return filter;
}

std::vector<Int64> makeRestValues(Int64 begin, size_t rows)
{
    std::vector<Int64> values;
    values.reserve(rows);
    for (size_t i = 0; i < rows; ++i)
        values.push_back(begin + static_cast<Int64>(i));
    return values;
}

std::vector<UInt8> makeResidualValues(size_t rows, size_t passed_rows)
{
    RUNTIME_CHECK(passed_rows <= rows);
    std::vector<UInt8> values(rows, 0);
    std::fill(values.begin(), values.begin() + passed_rows, 1);
    return values;
}

size_t drainMultiStageStream(const BlockInputStreamPtr & stream)
{
    size_t rows = 0;
    for (auto block = stream->read(); block; block = stream->read())
        rows += block.rows();
    return rows;
}

class DeterministicStage0FilterInputStream : public IProfilingBlockInputStream
{
public:
    explicit DeterministicStage0FilterInputStream(std::vector<IColumn::Filter> filters_)
        : filters(std::move(filters_))
    {}

    String getName() const override { return "DeterministicStage0Filter"; }

    Block getHeader() const override
    {
        return makeMultiStageBlock(0, std::vector<UInt8>{}, std::nullopt);
    }

    Block readImpl() override
    {
        FilterPtr filter_ignored;
        return readImpl(filter_ignored, false);
    }

    Block readImpl(FilterPtr & res_filter, bool return_filter) override
    {
        RUNTIME_CHECK(return_filter);
        if (next_block >= filters.size())
            return {};

        auto & filter = filters[next_block];
        res_filter = &filter;
        auto block = makeMultiStageBlock(
            start_offset,
            std::vector<UInt8>(filter.size(), 1),
            std::nullopt);
        start_offset += filter.size();
        ++next_block;
        return block;
    }

private:
    std::vector<IColumn::Filter> filters;
    size_t next_block = 0;
    UInt64 start_offset = 0;
};

class DeterministicSkippableBlockInputStream : public SkippableBlockInputStream
{
public:
    DeterministicSkippableBlockInputStream(
        ColumnDefines columns_to_read_,
        std::vector<std::vector<UInt8>> filter_values_,
        std::vector<std::vector<Int64>> rest_values_)
        : columns_to_read(std::move(columns_to_read_))
        , filter_values(std::move(filter_values_))
        , rest_values(std::move(rest_values_))
    {
        RUNTIME_CHECK(filter_values.size() == rest_values.size());
    }

    String getName() const override { return "DeterministicSkippable"; }

    Block getHeader() const override { return toEmptyBlock(columns_to_read); }

    bool getSkippedRows(size_t & skip_rows) override
    {
        if (next_block >= rest_values.size())
            return false;
        skip_rows = rest_values[next_block].size();
        return true;
    }

    size_t skipNextBlock() override
    {
        if (next_block >= rest_values.size())
            return 0;
        const auto rows = rest_values[next_block].size();
        start_offset += rows;
        ++next_block;
        ++skip_count;
        return rows;
    }

    Block readWithFilter(const IColumn::Filter & filter) override
    {
        RUNTIME_CHECK(next_block < rest_values.size());
        ++read_with_filter_count;
        auto block = buildCurrentBlock();
        RUNTIME_CHECK(filter.size() == block.rows());
        const auto passed_count = countBytesInFilter(filter);
        for (auto & col : block)
            col.column = col.column->filter(filter, passed_count);
        advance();
        return block;
    }

    Block read() override
    {
        if (next_block >= rest_values.size())
            return {};
        ++read_count;
        auto block = buildCurrentBlock();
        advance();
        return block;
    }

    size_t getReadCount() const { return read_count; }
    size_t getReadWithFilterCount() const { return read_with_filter_count; }
    size_t getSkipCount() const { return skip_count; }

private:
    Block buildCurrentBlock() const
    {
        std::optional<std::vector<UInt8>> filter_col;
        std::optional<std::vector<Int64>> rest_col;
        for (const auto & col : columns_to_read)
        {
            if (col.id == MULTI_STAGE_FILTER_COL_ID)
                filter_col = filter_values[next_block];
            else if (col.id == MULTI_STAGE_REST_COL_ID)
                rest_col = rest_values[next_block];
            else
                RUNTIME_CHECK_MSG(false, "Unexpected column id {}", col.id);
        }
        return makeMultiStageBlock(start_offset, std::move(filter_col), std::move(rest_col));
    }

    void advance()
    {
        start_offset += rest_values[next_block].size();
        ++next_block;
    }

private:
    ColumnDefines columns_to_read;
    std::vector<std::vector<UInt8>> filter_values;
    std::vector<std::vector<Int64>> rest_values;
    size_t next_block = 0;
    UInt64 start_offset = 0;
    size_t read_count = 0;
    size_t read_with_filter_count = 0;
    size_t skip_count = 0;
};

ColumnDefines makeMultiStageColumnsToRead()
{
    return {
        ColumnDefine(MULTI_STAGE_FILTER_COL_ID, MULTI_STAGE_FILTER_COL_NAME, std::make_shared<DataTypeUInt8>()),
        ColumnDefine(MULTI_STAGE_REST_COL_ID, MULTI_STAGE_REST_COL_NAME, std::make_shared<DataTypeInt64>()),
    };
}

ColumnDefines makeMultiStageFilterColumns()
{
    return {
        ColumnDefine(MULTI_STAGE_FILTER_COL_ID, MULTI_STAGE_FILTER_COL_NAME, std::make_shared<DataTypeUInt8>()),
    };
}

ColumnDefines makeMultiStageRestColumns()
{
    return {
        ColumnDefine(MULTI_STAGE_REST_COL_ID, MULTI_STAGE_REST_COL_NAME, std::make_shared<DataTypeInt64>()),
    };
}

PushDownFilterPtr makeResidualFilterForMultiStageTest()
{
    auto filter_columns = std::make_shared<ColumnDefines>(makeMultiStageFilterColumns());
    auto actions = std::make_shared<ExpressionActions>(toEmptyBlock(*filter_columns).getNamesAndTypes());
    actions->add(ExpressionAction::copyColumn(MULTI_STAGE_FILTER_COL_NAME, MULTI_STAGE_FILTER_TMP_COL_NAME));
    return std::make_shared<PushDownFilter>(
        EMPTY_RS_OPERATOR,
        actions,
        nullptr,
        filter_columns,
        MULTI_STAGE_FILTER_TMP_COL_NAME,
        nullptr,
        nullptr);
}

void ensureMultiStageTestFunctionsRegistered()
{
    static std::once_flag once;
    std::call_once(once, [] {
        try
        {
            DB::registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Another test suite may have already registered the functions.
        }
    });
}

PushDownFilterPtr makeResidualFunctionFilterForMultiStageTest()
{
    ensureMultiStageTestFunctionsRegistered();

    auto filter_columns = std::make_shared<ColumnDefines>(makeMultiStageFilterColumns());
    auto actions = std::make_shared<ExpressionActions>(toEmptyBlock(*filter_columns).getNamesAndTypes());

    auto const_column_type = std::make_shared<DataTypeUInt8>();
    actions->add(ExpressionAction::addColumn({
        const_column_type->createColumnConst(1, Field(static_cast<UInt64>(1))),
        const_column_type,
        MULTI_STAGE_FILTER_CONST_COL_NAME,
    }));

    auto equals_builder = FunctionFactory::instance().get("equals", *DB::tests::TiFlashTestEnv::getContext());
    actions->add(ExpressionAction::applyFunction(
        equals_builder,
        {MULTI_STAGE_FILTER_COL_NAME, MULTI_STAGE_FILTER_CONST_COL_NAME},
        MULTI_STAGE_FILTER_TMP_COL_NAME));

    return std::make_shared<PushDownFilter>(
        EMPTY_RS_OPERATOR,
        actions,
        nullptr,
        filter_columns,
        MULTI_STAGE_FILTER_TMP_COL_NAME,
        nullptr,
        nullptr);
}

void assertMultiStageRows(const Block & block, const std::vector<UInt8> & filter_values, const std::vector<Int64> & rest_values)
{
    ASSERT_TRUE(block);
    ASSERT_EQ(block.rows(), filter_values.size());
    ASSERT_EQ(block.rows(), rest_values.size());
    ASSERT_EQ(block.columns(), 2);
    ASSERT_COLUMN_EQ(
        makeUInt8Column(filter_values, MULTI_STAGE_FILTER_COL_NAME, MULTI_STAGE_FILTER_COL_ID),
        block.getByName(MULTI_STAGE_FILTER_COL_NAME));
    ASSERT_COLUMN_EQ(
        makeInt64Column(rest_values, MULTI_STAGE_REST_COL_NAME, MULTI_STAGE_REST_COL_ID),
        block.getByName(MULTI_STAGE_REST_COL_NAME));
}
} // namespace

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
            return {};

        filter.resize(blk.rows());
        res_filter = &filter;
        size_t mode = e() % 3;
        if (mode == 0)
        {
            std::fill(filter.begin(), filter.end(), 0);
        }
        else if (mode == 1)
        {
            std::fill(filter.begin(), filter.end(), 1);
        }
        else
        {
            std::transform(filter.begin(), filter.end(), filter.begin(), [&e = e](auto) {
                return e() % 8192 == 0 ? 1 : 0;
            });
            filter[e() % blk.rows()] = 1; // should not be all 0.
        }
        total_filter.insert(total_filter.end(), filter.begin(), filter.end());
        return blk;
    }

public:
    IColumn::Filter filter{};
    IColumn::Filter total_filter{};

private:
    BlockInputStreamPtr input;
    std::default_random_engine e;
};

class SkippableBlockInputStreamTest : public SegmentTestBasic
{
protected:
    DB::LoggerPtr log = DB::Logger::get("SkippableBlockInputStreamTest");
    static constexpr auto SEG_ID = DELTA_MERGE_FIRST_SEGMENT_ID;
    RowKeyRanges read_ranges;

    String default_filter_column_name;

    SkippableBlockInputStreamPtr getInputStream(
        const SegmentPtr & segment,
        const SegmentSnapshotPtr & snapshot,
        const ColumnDefines & columns_to_read,
        const RowKeyRanges & read_ranges)
    {
        return segment->getConcatSkippableBlockInputStream(
            nullptr,
            snapshot,
            *dm_context,
            columns_to_read,
            read_ranges,
            EMPTY_RS_OPERATOR,
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE,
            ReadTag::Internal);
    }

    void testSkipBlockCase(std::string_view seg_data, std::vector<size_t> skip_block_idxs = {})
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

        auto stream = getInputStream(segment, snapshot, columns_to_read, read_ranges);

        stream->readPrefix();
        std::vector<Block> expected_blks;
        for (auto blk = stream->read(); blk; blk = stream->read())
        {
            expected_blks.push_back(std::move(blk));
        }
        stream->readSuffix();

        stream = getInputStream(segment, snapshot, columns_to_read, read_ranges);

        size_t offset = 0;
        stream->readPrefix();
        for (size_t i = 0; i < expected_blks.size(); ++i)
        {
            if (std::find(skip_block_idxs.begin(), skip_block_idxs.end(), i) != skip_block_idxs.end())
            {
                offset += expected_blks[i].rows();
                size_t skipped_rows = stream->skipNextBlock();
                ASSERT_EQ(skipped_rows, expected_blks[i].rows());
                continue;
            }
            auto blk = stream->read();
            ASSERT_EQ(offset, blk.startOffset());
            offset += blk.rows();
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
            default_filter_column_name,
            filter_cloumn_stream,
            rest_column_stream,
            bitmap_filter,
            "test");
        late_materialization_stream->readPrefix();
        auto normal_stream = getInputStream(segment, snapshot, columns_to_read, read_ranges);
        normal_stream->readPrefix();
        auto filter_stream = std::dynamic_pointer_cast<MockFilterBlockInputStream>(filter_cloumn_stream);
        while (true)
        {
            auto blk1 = late_materialization_stream->read();
            if (!blk1)
                break;
            Block blk2;
            while (!blk2)
            {
                blk2 = normal_stream->read();
                auto & filter = filter_stream->total_filter;
                IColumn::Filter block_filter(
                    filter.cbegin() + blk2.startOffset(),
                    filter.cbegin() + blk2.startOffset() + blk2.rows());
                bitmap_filter->rangeAnd(block_filter, blk2.startOffset(), blk2.rows());
                size_t passed_count = countBytesInFilter(block_filter);
                if (passed_count == 0)
                {
                    blk2 = {};
                    continue;
                }
                for (auto & col : blk2)
                {
                    col.column = col.column->filter(block_filter, passed_count);
                }
            }
            ASSERT_BLOCK_EQ(blk1, blk2);
        }
        late_materialization_stream->readSuffix();
        normal_stream->readSuffix();
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
    testLateMaterializationCase("d_mem:[0, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, MultiStageLateMaterializationComposeFilters)
try
{
    IColumn::Filter stage0_filter{1, 0, 1, 1, 0, 1};
    IColumn::Filter residual_filter{0, 1, 0, 1};
    auto combined_filter
        = MultiStageLateMaterializationBlockInputStream::composeFilters(&stage0_filter, 6, residual_filter);
    ASSERT_EQ(
        std::vector<UInt8>(combined_filter.begin(), combined_filter.end()),
        std::vector<UInt8>({0, 0, 1, 0, 0, 1}));

    IColumn::Filter residual_filter_without_stage0{1, 0, 1};
    auto combined_filter_without_stage0
        = MultiStageLateMaterializationBlockInputStream::composeFilters(nullptr, 3, residual_filter_without_stage0);
    ASSERT_EQ(
        std::vector<UInt8>(combined_filter_without_stage0.begin(), combined_filter_without_stage0.end()),
        std::vector<UInt8>(residual_filter_without_stage0.begin(), residual_filter_without_stage0.end()));
}
CATCH

TEST_F(SkippableBlockInputStreamTest, MultiStageLateMaterializationDirectMode)
try
{
    std::vector<IColumn::Filter> stage0_filters;
    stage0_filters.emplace_back(makeFilter({1, 1, 1, 1, 1, 1}));
    auto stage0_stream = std::make_shared<DeterministicStage0FilterInputStream>(std::move(stage0_filters));
    auto stage1_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageFilterColumns(),
        std::vector<std::vector<UInt8>>{{1, 1, 1, 1, 1, 1}},
        std::vector<std::vector<Int64>>{{10, 11, 12, 13, 14, 15}});
    auto final_rest_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageRestColumns(),
        std::vector<std::vector<UInt8>>{{1, 1, 1, 1, 1, 1}},
        std::vector<std::vector<Int64>>{{10, 11, 12, 13, 14, 15}});

    auto bitmap_filter = std::make_shared<BitmapFilter>(6, 1);
    auto stream = std::make_shared<MultiStageLateMaterializationBlockInputStream>(
        makeMultiStageColumnsToRead(),
        stage0_stream,
        stage1_stream,
        final_rest_stream,
        makeResidualFilterForMultiStageTest(),
        bitmap_filter,
        "test");

    assertMultiStageRows(stream->read(), {1, 1, 1, 1, 1, 1}, {10, 11, 12, 13, 14, 15});
    ASSERT_BLOCK_EQ(stream->read(), Block{});
    ASSERT_EQ(stage1_stream->getReadCount(), 1);
    ASSERT_EQ(stage1_stream->getReadWithFilterCount(), 0);
    ASSERT_EQ(final_rest_stream->getReadCount(), 1);
    ASSERT_EQ(final_rest_stream->getReadWithFilterCount(), 0);
}
CATCH

TEST_F(SkippableBlockInputStreamTest, MultiStageLateMaterializationLateMode)
try
{
    constexpr size_t rows = DEFAULT_MERGE_BLOCK_SIZE * 2 + 4;
    std::vector<UInt8> residual_values(rows, 0);
    residual_values[1] = 1;
    residual_values[rows - 2] = 1;
    auto rest_values = makeRestValues(100, rows);

    std::vector<IColumn::Filter> stage0_filters;
    stage0_filters.emplace_back(rows, 1);
    auto stage0_stream = std::make_shared<DeterministicStage0FilterInputStream>(std::move(stage0_filters));
    auto stage1_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageFilterColumns(),
        std::vector<std::vector<UInt8>>{residual_values},
        std::vector<std::vector<Int64>>{rest_values});
    auto final_rest_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageRestColumns(),
        std::vector<std::vector<UInt8>>{residual_values},
        std::vector<std::vector<Int64>>{rest_values});

    auto bitmap_filter = std::make_shared<BitmapFilter>(rows, 1);
    auto stream = std::make_shared<MultiStageLateMaterializationBlockInputStream>(
        makeMultiStageColumnsToRead(),
        stage0_stream,
        stage1_stream,
        final_rest_stream,
        makeResidualFilterForMultiStageTest(),
        bitmap_filter,
        "test");

    assertMultiStageRows(stream->read(), {1, 1}, {101, 100 + static_cast<Int64>(rows) - 2});
    ASSERT_BLOCK_EQ(stream->read(), Block{});
    ASSERT_EQ(stage1_stream->getReadCount(), 1);
    ASSERT_EQ(stage1_stream->getReadWithFilterCount(), 0);
    ASSERT_EQ(final_rest_stream->getReadCount(), 0);
    ASSERT_EQ(final_rest_stream->getReadWithFilterCount(), 1);
}
CATCH

TEST_F(SkippableBlockInputStreamTest, MultiStageLateMaterializationComputedResidualFilter)
try
{
    constexpr size_t rows = DEFAULT_MERGE_BLOCK_SIZE * 2 + 4;
    std::vector<UInt8> residual_values(rows, 0);
    residual_values[1] = 1;
    residual_values[rows - 2] = 1;
    auto rest_values = makeRestValues(100, rows);

    std::vector<IColumn::Filter> stage0_filters;
    stage0_filters.emplace_back(rows, 1);
    auto stage0_stream = std::make_shared<DeterministicStage0FilterInputStream>(std::move(stage0_filters));
    auto stage1_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageFilterColumns(),
        std::vector<std::vector<UInt8>>{residual_values},
        std::vector<std::vector<Int64>>{rest_values});
    auto final_rest_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageRestColumns(),
        std::vector<std::vector<UInt8>>{residual_values},
        std::vector<std::vector<Int64>>{rest_values});

    auto bitmap_filter = std::make_shared<BitmapFilter>(rows, 1);
    auto stream = std::make_shared<MultiStageLateMaterializationBlockInputStream>(
        makeMultiStageColumnsToRead(),
        stage0_stream,
        stage1_stream,
        final_rest_stream,
        makeResidualFunctionFilterForMultiStageTest(),
        bitmap_filter,
        "test");

    assertMultiStageRows(stream->read(), {1, 1}, {101, 100 + static_cast<Int64>(rows) - 2});
    ASSERT_BLOCK_EQ(stream->read(), Block{});
    ASSERT_EQ(stage1_stream->getReadCount(), 1);
    ASSERT_EQ(stage1_stream->getReadWithFilterCount(), 0);
    ASSERT_EQ(final_rest_stream->getReadCount(), 0);
    ASSERT_EQ(final_rest_stream->getReadWithFilterCount(), 1);
}
CATCH

TEST_F(SkippableBlockInputStreamTest, MultiStageLateMaterializationStage0FiltersAllRows)
try
{
    std::vector<IColumn::Filter> stage0_filters;
    stage0_filters.emplace_back(makeFilter({0, 0, 0, 0}));
    auto stage0_stream = std::make_shared<DeterministicStage0FilterInputStream>(std::move(stage0_filters));
    auto stage1_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageFilterColumns(),
        std::vector<std::vector<UInt8>>{{1, 1, 1, 1}},
        std::vector<std::vector<Int64>>{{10, 11, 12, 13}});
    auto final_rest_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageRestColumns(),
        std::vector<std::vector<UInt8>>{{1, 1, 1, 1}},
        std::vector<std::vector<Int64>>{{10, 11, 12, 13}});

    auto bitmap_filter = std::make_shared<BitmapFilter>(4, 1);
    auto stream = std::make_shared<MultiStageLateMaterializationBlockInputStream>(
        makeMultiStageColumnsToRead(),
        stage0_stream,
        stage1_stream,
        final_rest_stream,
        makeResidualFilterForMultiStageTest(),
        bitmap_filter,
        "test");

    ASSERT_BLOCK_EQ(stream->read(), Block{});
    ASSERT_EQ(stage1_stream->getSkipCount(), 1);
    ASSERT_EQ(final_rest_stream->getSkipCount(), 1);
    ASSERT_EQ(stage1_stream->getReadCount(), 0);
    ASSERT_EQ(final_rest_stream->getReadCount(), 0);
}
CATCH

TEST_F(SkippableBlockInputStreamTest, MultiStageLateMaterializationResidualFiltersAllRows)
try
{
    std::vector<IColumn::Filter> stage0_filters;
    stage0_filters.emplace_back(makeFilter({1, 1, 1, 1}));
    auto stage0_stream = std::make_shared<DeterministicStage0FilterInputStream>(std::move(stage0_filters));
    auto stage1_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageFilterColumns(),
        std::vector<std::vector<UInt8>>{{0, 0, 0, 0}},
        std::vector<std::vector<Int64>>{{10, 11, 12, 13}});
    auto final_rest_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageRestColumns(),
        std::vector<std::vector<UInt8>>{{1, 1, 1, 1}},
        std::vector<std::vector<Int64>>{{10, 11, 12, 13}});

    auto bitmap_filter = std::make_shared<BitmapFilter>(4, 1);
    auto stream = std::make_shared<MultiStageLateMaterializationBlockInputStream>(
        makeMultiStageColumnsToRead(),
        stage0_stream,
        stage1_stream,
        final_rest_stream,
        makeResidualFilterForMultiStageTest(),
        bitmap_filter,
        "test");

    ASSERT_BLOCK_EQ(stream->read(), Block{});
    ASSERT_EQ(stage1_stream->getReadCount(), 1);
    ASSERT_EQ(final_rest_stream->getSkipCount(), 1);
    ASSERT_EQ(final_rest_stream->getReadCount(), 0);
}
CATCH

TEST_F(SkippableBlockInputStreamTest, MultiStageLateMaterializationStage0FiltersSomeRows)
try
{
    std::vector<IColumn::Filter> stage0_filters;
    stage0_filters.emplace_back(makeFilter({1, 0, 1, 0, 1, 0}));
    auto stage0_stream = std::make_shared<DeterministicStage0FilterInputStream>(std::move(stage0_filters));
    auto stage1_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageFilterColumns(),
        std::vector<std::vector<UInt8>>{{1, 0, 1, 0, 1, 0}},
        std::vector<std::vector<Int64>>{{10, 11, 12, 13, 14, 15}});
    auto final_rest_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageRestColumns(),
        std::vector<std::vector<UInt8>>{{1, 1, 1, 1, 1, 1}},
        std::vector<std::vector<Int64>>{{10, 11, 12, 13, 14, 15}});

    auto bitmap_filter = std::make_shared<BitmapFilter>(6, 1);
    auto stream = std::make_shared<MultiStageLateMaterializationBlockInputStream>(
        makeMultiStageColumnsToRead(),
        stage0_stream,
        stage1_stream,
        final_rest_stream,
        makeResidualFilterForMultiStageTest(),
        bitmap_filter,
        "test");

    assertMultiStageRows(stream->read(), {1, 1, 1}, {10, 12, 14});
    ASSERT_BLOCK_EQ(stream->read(), Block{});
    ASSERT_EQ(stage1_stream->getReadCount(), 1);
    ASSERT_EQ(stage1_stream->getReadWithFilterCount(), 0);
    ASSERT_EQ(final_rest_stream->getReadCount(), 1);
    ASSERT_EQ(final_rest_stream->getReadWithFilterCount(), 0);
}
CATCH

TEST_F(SkippableBlockInputStreamTest, MultiStageLateMaterializationAdaptiveLateMode)
try
{
    constexpr size_t sample_rows_per_block = 4096;
    constexpr size_t final_rows = DEFAULT_MERGE_BLOCK_SIZE * 6 + 1024;
    constexpr size_t final_passed_rows = DEFAULT_MERGE_BLOCK_SIZE * 4;

    std::vector<IColumn::Filter> stage0_filters;
    std::vector<std::vector<UInt8>> filter_values;
    std::vector<std::vector<Int64>> rest_values;
    for (size_t i = 0; i < 4; ++i)
    {
        stage0_filters.emplace_back(sample_rows_per_block, 1);
        filter_values.emplace_back(makeResidualValues(sample_rows_per_block, 1));
        rest_values.emplace_back(makeRestValues(static_cast<Int64>(i * sample_rows_per_block), sample_rows_per_block));
    }
    stage0_filters.emplace_back(final_rows, 1);
    filter_values.emplace_back(makeResidualValues(final_rows, final_passed_rows));
    rest_values.emplace_back(makeRestValues(100000, final_rows));

    auto stage0_stream = std::make_shared<DeterministicStage0FilterInputStream>(std::move(stage0_filters));
    auto stage1_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageFilterColumns(),
        filter_values,
        rest_values);
    auto final_rest_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageRestColumns(),
        filter_values,
        rest_values);

    auto bitmap_filter = std::make_shared<BitmapFilter>(4 * sample_rows_per_block + final_rows, 1);
    auto stream = std::make_shared<MultiStageLateMaterializationBlockInputStream>(
        makeMultiStageColumnsToRead(),
        stage0_stream,
        stage1_stream,
        final_rest_stream,
        makeResidualFilterForMultiStageTest(),
        bitmap_filter,
        "test");

    ASSERT_EQ(drainMultiStageStream(stream), 4 + final_passed_rows);
    ASSERT_EQ(final_rest_stream->getReadWithFilterCount(), 1);
}
CATCH

TEST_F(SkippableBlockInputStreamTest, MultiStageLateMaterializationAdaptiveDirectMode)
try
{
    constexpr size_t sample_rows_per_block = 4096;
    constexpr size_t final_rows = DEFAULT_MERGE_BLOCK_SIZE * 6 + 1024;
    constexpr size_t final_passed_rows = 2;

    std::vector<IColumn::Filter> stage0_filters;
    std::vector<std::vector<UInt8>> filter_values;
    std::vector<std::vector<Int64>> rest_values;
    for (size_t i = 0; i < 4; ++i)
    {
        stage0_filters.emplace_back(sample_rows_per_block, 1);
        filter_values.emplace_back(makeResidualValues(sample_rows_per_block, sample_rows_per_block));
        rest_values.emplace_back(makeRestValues(static_cast<Int64>(i * sample_rows_per_block), sample_rows_per_block));
    }
    stage0_filters.emplace_back(final_rows, 1);
    filter_values.emplace_back(makeResidualValues(final_rows, final_passed_rows));
    rest_values.emplace_back(makeRestValues(100000, final_rows));

    auto stage0_stream = std::make_shared<DeterministicStage0FilterInputStream>(std::move(stage0_filters));
    auto stage1_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageFilterColumns(),
        filter_values,
        rest_values);
    auto final_rest_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageRestColumns(),
        filter_values,
        rest_values);

    auto bitmap_filter = std::make_shared<BitmapFilter>(4 * sample_rows_per_block + final_rows, 1);
    auto stream = std::make_shared<MultiStageLateMaterializationBlockInputStream>(
        makeMultiStageColumnsToRead(),
        stage0_stream,
        stage1_stream,
        final_rest_stream,
        makeResidualFilterForMultiStageTest(),
        bitmap_filter,
        "test");

    ASSERT_EQ(drainMultiStageStream(stream), 4 * sample_rows_per_block + final_passed_rows);
    ASSERT_EQ(final_rest_stream->getReadWithFilterCount(), 0);
    ASSERT_EQ(final_rest_stream->getReadCount(), 5);
}
CATCH

TEST_F(SkippableBlockInputStreamTest, MultiStageLateMaterializationSparseSampleRowsKeepSampling)
try
{
    constexpr size_t final_rows = DEFAULT_MERGE_BLOCK_SIZE * 6 + 1024;
    constexpr size_t final_passed_rows = DEFAULT_MERGE_BLOCK_SIZE * 4;

    std::vector<IColumn::Filter> stage0_filters;
    std::vector<std::vector<UInt8>> filter_values;
    std::vector<std::vector<Int64>> rest_values;
    for (size_t i = 0; i < 4; ++i)
    {
        stage0_filters.emplace_back(1, 1);
        filter_values.emplace_back(std::vector<UInt8>{0});
        rest_values.emplace_back(std::vector<Int64>{static_cast<Int64>(i)});
    }
    stage0_filters.emplace_back(final_rows, 1);
    filter_values.emplace_back(makeResidualValues(final_rows, final_passed_rows));
    rest_values.emplace_back(makeRestValues(100000, final_rows));

    auto stage0_stream = std::make_shared<DeterministicStage0FilterInputStream>(std::move(stage0_filters));
    auto stage1_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageFilterColumns(),
        filter_values,
        rest_values);
    auto final_rest_stream = std::make_shared<DeterministicSkippableBlockInputStream>(
        makeMultiStageRestColumns(),
        filter_values,
        rest_values);

    auto bitmap_filter = std::make_shared<BitmapFilter>(4 + final_rows, 1);
    auto stream = std::make_shared<MultiStageLateMaterializationBlockInputStream>(
        makeMultiStageColumnsToRead(),
        stage0_stream,
        stage1_stream,
        final_rest_stream,
        makeResidualFilterForMultiStageTest(),
        bitmap_filter,
        "test");

    ASSERT_EQ(drainMultiStageStream(stream), final_passed_rows);
    ASSERT_EQ(final_rest_stream->getSkipCount(), 4);
    ASSERT_EQ(final_rest_stream->getReadWithFilterCount(), 0);
    ASSERT_EQ(final_rest_stream->getReadCount(), 1);
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory2)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem:[0, 1000)", {0});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem:[0, 1000)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem:[0, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory3)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem:[100, 200)", {3, 6, 9});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem:[100, 200)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem:[100, 200)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory4)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem:[-100, 100)", {0, 1, 3, 4, 5, 6, 7, 8});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem:[-100, 100)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem:[-100, 100)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory5)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem_del:[0, 1000)", {4, 5, 6});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem_del:[0, 1000)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem_del:[0, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory6)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem_del:[100, 200)", {});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem_del:[100, 200)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem_del:[100, 200)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, InMemory7)
try
{
    testSkipBlockCase("d_mem:[0, 1000)|d_mem_del:[-100, 100)", {0, 1, 2, 3, 4, 5, 6, 7, 8});
    testReadWithFilterCase("d_mem:[0, 1000)|d_mem_del:[-100, 100)");
    testLateMaterializationCase("d_mem:[0, 1000)|d_mem_del:[-100, 100)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Tiny1)
try
{
    testSkipBlockCase("d_tiny:[100, 500)|d_mem:[200, 1000)", {1, 2, 3, 4, 5, 6});
    testReadWithFilterCase("d_tiny:[100, 500)|d_mem:[200, 1000)");
    testLateMaterializationCase("d_tiny:[100, 500)|d_mem:[200, 1000)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, TinyDel1)
try
{
    testSkipBlockCase("d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)", {7, 8, 9});
    testReadWithFilterCase("d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)");
    testLateMaterializationCase("d_tiny:[100, 500)|d_tiny_del:[200, 300)|d_mem:[0, 100)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, DeleteRange)
try
{
    testSkipBlockCase("d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)", {1, 2, 3, 4, 5, 9});
    testReadWithFilterCase("d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)");
    testLateMaterializationCase("d_tiny:[100, 500)|d_dr:[250, 300)|d_mem:[240, 290)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Big)
try
{
    testSkipBlockCase("d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)", {1, 3, 4, 9});
    testReadWithFilterCase("d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)");
    testLateMaterializationCase("d_tiny:[100, 500)|d_big:[250, 1000)|d_mem:[240, 290)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Stable1)
try
{
    testSkipBlockCase("s:[0, 1024)|d_dr:[0, 1023)", {0});
    testReadWithFilterCase("s:[0, 1024)|d_dr:[0, 1023)");
    testLateMaterializationCase("s:[0, 1024)|d_dr:[0, 1023)");
}
CATCH

TEST_F(SkippableBlockInputStreamTest, Stable2)
try
{
    testSkipBlockCase("s:[0, 102294)|d_dr:[0, 1023)", {2});
    testReadWithFilterCase("s:[0, 102294)|d_dr:[0, 1023)");
    testLateMaterializationCase("s:[0, 102294)|d_dr:[0, 1023)");
}
CATCH


TEST_F(SkippableBlockInputStreamTest, Stable3)
try
{
    testSkipBlockCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)", {0});
    testReadWithFilterCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)");
    testLateMaterializationCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)");
}
CATCH


TEST_F(SkippableBlockInputStreamTest, Mix)
try
{
    testSkipBlockCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)", {1, 2});
    testReadWithFilterCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)");
    testLateMaterializationCase("s:[0, 1024)|d_dr:[128, 256)|d_tiny_del:[300, 310)|d_tiny:[200, 255)|d_mem:[298, 305)");
}
CATCH

} // namespace DB::DM::tests
