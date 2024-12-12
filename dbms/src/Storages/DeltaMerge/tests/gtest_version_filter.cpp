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

#include <Core/Block.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>

namespace DB::DM::tests
{

namespace
{
constexpr const char * str_col_name = "a";

class DebugBlockInputStream : public IProfilingBlockInputStream
{
public:
    static constexpr ColumnID extra_column_id = 100;
    DebugBlockInputStream(const BlocksList & blocks, bool is_common_handle_)
        : begin(blocks.begin())
        , end(blocks.end())
        , it(blocks.begin())
        , is_common_handle(is_common_handle_)
    {}
    String getName() const override { return "Debug"; }
    Block getHeader() const override
    {
        auto cds = DMTestEnv::getDefaultColumns(
            is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        cds->emplace_back(
            extra_column_id,
            str_col_name,
            DataTypeFactory::instance().get(DataTypeString::getDefaultName()));
        return toEmptyBlock(*cds);
    }

protected:
    Block readImpl() override
    {
        if (it == end)
            return Block();
        else
            return *(it++);
    }

private:
    BlocksList::const_iterator begin;
    BlocksList::const_iterator end;
    BlocksList::const_iterator it;
    bool is_common_handle;
};

template <DMVersionFilterMode MODE>
BlockInputStreamPtr getVersionFilterInputStream(
    const BlocksList & blocks,
    const ColumnDefines & columns,
    UInt64 max_version,
    bool is_common_handle)
{
    ColumnDefine handle_define(
        TiDBPkColumnID,
        DMTestEnv::pk_name,
        is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE);
    return std::make_shared<DMVersionFilterBlockInputStream<MODE>>(
        std::make_shared<DebugBlockInputStream>(blocks, is_common_handle),
        columns,
        max_version,
        is_common_handle);
}

template <DMVersionFilterMode MODE, bool is_block_sorted>
BlockInputStreamPtr getVersionFilterInputStreamWithRowKeyFilterStream(
    const BlocksList & blocks,
    const ColumnDefines & columns,
    UInt64 max_version,
    bool is_common_handle,
    const RowKeyRanges & read_ranges)
{
    ColumnDefine handle_define(
        TiDBPkColumnID,
        DMTestEnv::pk_name,
        is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE);

    BlockInputStreamPtr rowkey_filter_stream = std::make_shared<DMRowKeyFilterBlockInputStream<is_block_sorted>>(
        std::make_shared<DebugBlockInputStream>(blocks, is_common_handle),
        read_ranges,
        0);

    return std::make_shared<DMVersionFilterBlockInputStream<MODE>>(
        rowkey_filter_stream,
        columns,
        max_version,
        is_common_handle);
}

} // namespace

TEST(VersionFilterTest, MVCC)
{
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            10,
            0,
            str_col_name,
            "hello",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            20,
            0,
            str_col_name,
            "world",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            30,
            1,
            str_col_name,
            "",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            40,
            0,
            str_col_name,
            "Flash",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
    }

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::MVCC>(blocks, columns, 40, false);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({"Flash"})}));
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::MVCC>(blocks, columns, 30, false);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({})}));
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::MVCC>(blocks, columns, 20, false);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({"world"})}));
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::MVCC>(blocks, columns, 10, false);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({"hello"})}));
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::MVCC>(blocks, columns, 9, false);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({})}));
    }
}

TEST(VersionFilterTest, RangesMVCC)
{
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            10,
            0,
            str_col_name,
            "hello",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            20,
            0,
            str_col_name,
            "world",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            30,
            1,
            str_col_name,
            "",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            40,
            0,
            str_col_name,
            "Flash",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
    }
    {
        Int64 pk_value = 45;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            10,
            0,
            str_col_name,
            "hello",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            20,
            0,
            str_col_name,
            "world",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            30,
            1,
            str_col_name,
            "",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
    }
    {
        Int64 pk_value = 100;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            10,
            0,
            str_col_name,
            "hello",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            20,
            0,
            str_col_name,
            "world",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            30,
            1,
            str_col_name,
            "",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            40,
            0,
            str_col_name,
            "Flash",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
    }

    RowKeyRanges ranges;
    // create ranges unsorted and overlapped with each other
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(20, 50)));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(0, 30)));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(70, 90)));

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, false>(
            blocks,
            columns,
            40,
            false,
            ranges);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({"Flash"})}));
    }
    {
        auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, true>(
            blocks,
            columns,
            40,
            false,
            ranges);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({"Flash"})}));
    }
    {
        auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, false>(
            blocks,
            columns,
            30,
            false,
            ranges);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({})}));
    }
    {
        auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, true>(
            blocks,
            columns,
            30,
            false,
            ranges);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({})}));
    }
    {
        auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, false>(
            blocks,
            columns,
            20,
            false,
            ranges);
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({str_col_name}),
            createColumns({createColumn<String>({"world", "world"})}));
    }
    {
        auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, true>(
            blocks,
            columns,
            20,
            false,
            ranges);
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({str_col_name}),
            createColumns({createColumn<String>({"world", "world"})}));
    }
    {
        auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, false>(
            blocks,
            columns,
            10,
            false,
            ranges);
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({str_col_name}),
            createColumns({createColumn<String>({"hello", "hello"})}));
    }
    {
        auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, true>(
            blocks,
            columns,
            10,
            false,
            ranges);
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({str_col_name}),
            createColumns({createColumn<String>({"hello", "hello"})}));
    }
    {
        auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, false>(
            blocks,
            columns,
            9,
            false,
            ranges);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({})}));
    }
    {
        auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, true>(
            blocks,
            columns,
            9,
            false,
            ranges);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({})}));
    }
    {
        // test merged blocks
        Blocks block_vec{std::make_move_iterator(blocks.begin()), std::make_move_iterator(blocks.end())};
        Block block = DB::vstackBlocks(std::move(block_vec));
        blocks.clear();
        blocks.push_back(std::move(block));
        {
            auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, false>(
                blocks,
                columns,
                40,
                false,
                ranges);
            ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({"Flash"})}));
        }
        {
            auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, true>(
                blocks,
                columns,
                40,
                false,
                ranges);
            ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({"Flash"})}));
        }
        {
            auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, false>(
                blocks,
                columns,
                9,
                false,
                ranges);
            ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({})}));
        }
        {
            auto in = getVersionFilterInputStreamWithRowKeyFilterStream<DMVersionFilterMode::MVCC, true>(
                blocks,
                columns,
                9,
                false,
                ranges);
            ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({})}));
        }
    }
}

TEST(VersionFilterTest, MVCCCommonHandle)
{
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            10,
            0,
            str_col_name,
            "hello",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            20,
            0,
            str_col_name,
            "world",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            30,
            1,
            str_col_name,
            "",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            40,
            0,
            str_col_name,
            "Flash",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
    }

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::MVCC>(blocks, columns, 40, true);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({"Flash"})}));
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::MVCC>(blocks, columns, 30, true);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({})}));
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::MVCC>(blocks, columns, 20, true);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({"world"})}));
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::MVCC>(blocks, columns, 10, true);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({"hello"})}));
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::MVCC>(blocks, columns, 9, true);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({str_col_name}), createColumns({createColumn<String>({})}));
    }
}

TEST(VersionFilterTest, Compact)
{
    // TODO: currently it just test data statistics, add test for data correctness
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            10,
            0,
            str_col_name,
            "hello",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            20,
            0,
            str_col_name,
            "world",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            30,
            1,
            str_col_name,
            "",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            40,
            0,
            str_col_name,
            "Flash",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
    }
    {
        Int64 pk_value = 5;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            10,
            0,
            str_col_name,
            "hello",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            20,
            0,
            str_col_name,
            "world",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            30,
            1,
            str_col_name,
            "",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
    }
    {
        Int64 pk_value = 6;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            10,
            1,
            str_col_name,
            "hello",
            false,
            1,
            DebugBlockInputStream::extra_column_id));
    }

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::COMPACT>(blocks, columns, 40, false);
        const auto * mvcc_stream
            = typeid_cast<const DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = std::numeric_limits<UInt64>::max();
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)1);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)0);
        ASSERT_EQ(gc_hint_version, (size_t)std::numeric_limits<UInt64>::max());
        in->readSuffix();
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::COMPACT>(blocks, columns, 30, false);
        const auto * mvcc_stream
            = typeid_cast<const DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = std::numeric_limits<UInt64>::max();
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)2);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)2);
        ASSERT_EQ(gc_hint_version, (size_t)30);
        in->readSuffix();
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::COMPACT>(blocks, columns, 20, false);
        const auto * mvcc_stream
            = typeid_cast<const DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = std::numeric_limits<UInt64>::max();
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)2);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)4);
        ASSERT_EQ(gc_hint_version, (size_t)30);
        in->readSuffix();
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::COMPACT>(blocks, columns, 10, false);
        const auto * mvcc_stream
            = typeid_cast<const DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = std::numeric_limits<UInt64>::max();
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)3);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)7);
        ASSERT_EQ(gc_hint_version, (size_t)10);
        in->readSuffix();
    }
}

TEST(VersionFilterTest, CompactCommonHandle)
{
    // TODO: currently it just test data statistics, add test for data correctness
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            10,
            0,
            str_col_name,
            "hello",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            20,
            0,
            str_col_name,
            "world",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            30,
            1,
            str_col_name,
            "",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            40,
            0,
            str_col_name,
            "Flash",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
    }
    {
        Int64 pk_value = 5;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            10,
            0,
            str_col_name,
            "hello",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            20,
            0,
            str_col_name,
            "world",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            30,
            1,
            str_col_name,
            "",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
    }
    {
        Int64 pk_value = 6;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(
            pk_value,
            10,
            1,
            str_col_name,
            "hello",
            true,
            2,
            DebugBlockInputStream::extra_column_id));
    }

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::COMPACT>(blocks, columns, 40, true);
        const auto * mvcc_stream
            = typeid_cast<const DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = std::numeric_limits<UInt64>::max();
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)1);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)0);
        ASSERT_EQ(gc_hint_version, (size_t)std::numeric_limits<UInt64>::max());
        in->readSuffix();
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::COMPACT>(blocks, columns, 30, true);
        const auto * mvcc_stream
            = typeid_cast<const DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = std::numeric_limits<UInt64>::max();
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)2);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)2);
        ASSERT_EQ(gc_hint_version, (size_t)30);
        in->readSuffix();
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::COMPACT>(blocks, columns, 20, true);
        const auto * mvcc_stream
            = typeid_cast<const DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = std::numeric_limits<UInt64>::max();
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)2);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)4);
        ASSERT_EQ(gc_hint_version, (size_t)30);
        in->readSuffix();
    }
    {
        auto in = getVersionFilterInputStream<DMVersionFilterMode::COMPACT>(blocks, columns, 10, true);
        const auto * mvcc_stream
            = typeid_cast<const DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT> *>(in.get());
        ASSERT_NE(mvcc_stream, nullptr);
        UInt64 gc_hint_version = std::numeric_limits<UInt64>::max();
        in->readPrefix();
        while (true)
        {
            Block block = in->read();
            if (!block)
                break;
            if (!block.rows())
                continue;
            gc_hint_version = std::min(mvcc_stream->getGCHintVersion(), gc_hint_version);
        }
        ASSERT_EQ(mvcc_stream->getEffectiveNumRows(), (size_t)3);
        ASSERT_EQ(mvcc_stream->getNotCleanRows(), (size_t)7);
        ASSERT_EQ(gc_hint_version, (size_t)10);
        in->readSuffix();
    }
}

} // namespace DB::DM::tests
