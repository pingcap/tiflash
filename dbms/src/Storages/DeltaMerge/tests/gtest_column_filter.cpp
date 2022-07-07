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
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/DMDecoratorStreams.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>

namespace DB
{
namespace DM
{
namespace tests
{
namespace
{
constexpr const char * str_col_name = "col_a";

class DebugBlockInputStream : public IProfilingBlockInputStream
{
public:
    DebugBlockInputStream(const BlocksList & blocks, bool is_common_handle_)
        : begin(blocks.begin())
        , end(blocks.end())
        , it(blocks.begin())
        , is_common_handle(is_common_handle_)
    {
    }
    String getName() const override { return "Debug"; }
    Block getHeader() const override
    {
        auto cds = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        cds->push_back(ColumnDefine(100, str_col_name, DataTypeFactory::instance().get("String")));
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

BlockInputStreamPtr genInputStream(const BlocksList & blocks, const ColumnDefines & columns, bool is_common_handle, bool filter_delete_mark = true)
{
    ColumnDefine handle_define(
        TiDBPkColumnID,
        DMTestEnv::pk_name,
        is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE);
    return std::make_shared<DMColumnFilterBlockInputStream>(
        std::make_shared<DebugBlockInputStream>(blocks, is_common_handle),
        columns,
        filter_delete_mark);
}
} // namespace

TEST(ColumnFilterTest, NormalCaseFilterDeleteMark)
{
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 10, 0, str_col_name, "hello", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 20, 0, str_col_name, "world", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 30, 1, str_col_name, "", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 40, 0, str_col_name, "TiFlash", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 40, 1, str_col_name, "Storage", false, 1));
    }

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto in = genInputStream(blocks, columns, false);
        in->readPrefix();
        Block block = in->read();
        ASSERT_EQ(block.rows(), 1);
        auto col = block.getByName(str_col_name);
        auto val = col.column->getDataAt(0);
        ASSERT_EQ(val, "hello");

        block = in->read();
        ASSERT_EQ(block.rows(), 1);
        col = block.getByName(str_col_name);
        val = col.column->getDataAt(0);
        ASSERT_EQ(val, "world");

        block = in->read();
        ASSERT_EQ(block.rows(), 1);
        col = block.getByName(str_col_name);
        val = col.column->getDataAt(0);
        ASSERT_EQ(val, "TiFlash");

        block = in->read();
        ASSERT_EQ(block.rows(), 0);
        in->readSuffix();
    }
}

TEST(ColumnFilterTest, WithoutFilterDeleteMark)
{
    BlocksList blocks;

    {
        Int64 pk_value = 4;
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 10, 0, str_col_name, "hello", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 20, 0, str_col_name, "world", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 30, 1, str_col_name, "", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 40, 0, str_col_name, "TiFlash", false, 1));
        blocks.push_back(DMTestEnv::prepareOneRowBlock(pk_value, 40, 1, str_col_name, "Storage", false, 1));
    }

    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());

    {
        auto in = genInputStream(blocks, columns, false, false);
        in->readPrefix();
        Block block = in->read();
        ASSERT_EQ(block.rows(), 1);
        auto col = block.getByName(str_col_name);
        auto val = col.column->getDataAt(0);
        ASSERT_EQ(val, "hello");

        block = in->read();
        ASSERT_EQ(block.rows(), 1);
        col = block.getByName(str_col_name);
        val = col.column->getDataAt(0);
        ASSERT_EQ(val, "world");

        block = in->read();
        ASSERT_EQ(block.rows(), 1);
        col = block.getByName(str_col_name);
        val = col.column->getDataAt(0);
        ASSERT_EQ(val, "");


        block = in->read();
        ASSERT_EQ(block.rows(), 1);
        col = block.getByName(str_col_name);
        val = col.column->getDataAt(0);
        ASSERT_EQ(val, "TiFlash");

        block = in->read();
        ASSERT_EQ(block.rows(), 1);
        col = block.getByName(str_col_name);
        val = col.column->getDataAt(0);
        ASSERT_EQ(val, "Storage");

        block = in->read();
        ASSERT_EQ(block.rows(), 0);
        in->readSuffix();
    }
}
} // namespace tests
} // namespace DM
} // namespace DB