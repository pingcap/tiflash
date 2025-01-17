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

#include <DataStreams/BlocksListBlockInputStream.h>
#include <Storages/DeltaMerge/DMDecoratorStreams.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>

namespace DB::DM::tests
{

namespace
{
constexpr const char * str_col_name = "col_a";

class DebugBlockInputStream : public BlocksListBlockInputStream
{
public:
    DebugBlockInputStream(BlocksList & blocks, bool is_common_handle_)
        : BlocksListBlockInputStream(std::move(blocks))
        , is_common_handle(is_common_handle_)
    {}
    String getName() const override { return "Debug"; }
    Block getHeader() const override
    {
        auto cds = DMTestEnv::getDefaultColumns(
            is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        cds->emplace_back(100, str_col_name, DataTypeFactory::instance().get(DataTypeString::getDefaultName()));
        return toEmptyBlock(*cds);
    }

private:
    bool is_common_handle;
};

BlockInputStreamPtr genColumnProjInputStream(BlocksList & blocks, const ColumnDefines & columns, bool is_common_handle)
{
    ColumnDefine handle_define(
        MutSup::extra_handle_id,
        DMTestEnv::pk_name,
        is_common_handle ? MutSup::getExtraHandleColumnStringType() : MutSup::getExtraHandleColumnIntType());

    return std::make_shared<DMColumnProjectionBlockInputStream>(
        std::make_shared<DebugBlockInputStream>(blocks, is_common_handle),
        columns);
}

BlockInputStreamPtr genDeleteFilterInputStream(
    BlocksList & blocks,
    const ColumnDefines & columns,
    bool is_common_handle)
{
    ColumnDefine handle_define(
        MutSup::extra_handle_id,
        DMTestEnv::pk_name,
        is_common_handle ? MutSup::getExtraHandleColumnStringType() : MutSup::getExtraHandleColumnIntType());

    return std::make_shared<DMDeleteFilterBlockInputStream>(
        std::make_shared<DebugBlockInputStream>(blocks, is_common_handle),
        columns);
}
} // namespace

TEST(DeleteFilterTest, NormalCase)
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

    auto in = genDeleteFilterInputStream(blocks, columns, false);
    ASSERT_INPUTSTREAM_COLS_UR(
        in,
        Strings({str_col_name}),
        createColumns({
            createColumn<String>({"hello", "world", "TiFlash"}),
        }));
}

TEST(ColumnProjectionTest, NormalCase)
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

    // Only keep the column `str_col_name`
    ColumnDefines columns = getColumnDefinesFromBlock(blocks.back());
    for (auto iter = columns.begin(); iter != columns.end(); /**/)
    {
        if (iter->name != str_col_name)
            iter = columns.erase(iter);
        else
            iter++;
    }

    ASSERT_INPUTSTREAM_BLOCK_UR(
        genColumnProjInputStream(blocks, columns, false),
        Block({
            createColumn<String>({"hello", "world", "", "TiFlash", "Storage"}, str_col_name),
        }));
}

} // namespace DB::DM::tests
