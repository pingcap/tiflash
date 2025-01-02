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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/MultiSegmentTestUtil.h>
#include <Storages/PathPool.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <ranges>

namespace DB::DM
{
extern DMFilePtr writeIntoNewDMFile(
    DMContext & dm_context,
    const ColumnDefinesPtr & schema_snap,
    const BlockInputStreamPtr & input_stream,
    UInt64 file_id,
    const String & parent_path);
}

namespace DB::DM::tests
{

// Simple test suit for DeltaMergeStore.
class DeltaMergeStoreTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        store = reload();
    }

    DeltaMergeStorePtr reload(
        const ColumnDefinesPtr & pre_define_columns = {},
        bool is_common_handle = false,
        size_t rowkey_column_size = 1)
    {
        TiFlashStorageTestBasic::reload();
        ColumnDefinesPtr cols;
        if (!pre_define_columns)
            cols = DMTestEnv::getDefaultColumns(
                is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        else
            cols = pre_define_columns;

        ColumnDefine handle_column_define = (*cols)[0];

        DeltaMergeStorePtr s = DeltaMergeStore::create(
            *db_context,
            false,
            "test",
            "t_100",
            NullspaceID,
            100,
            /*pk_col_id*/ 0,
            true,
            *cols,
            handle_column_define,
            is_common_handle,
            rowkey_column_size,
            nullptr,
            DeltaMergeStore::Settings());
        return s;
    }

    Strings getAllStorePaths() const
    {
        auto path_delegate = store->path_pool->getStableDiskDelegator();
        return path_delegate.listPaths();
    }

    std::pair<RowKeyRange, std::vector<ExternalDTFileInfo>> genDMFile(DMContext & context, const Block & block)
    {
        auto input_stream = std::make_shared<OneBlockInputStream>(block);
        auto [store_path, file_id] = store->preAllocateIngestFile();

        auto dmfile = writeIntoNewDMFile(
            context,
            std::make_shared<ColumnDefines>(store->getTableColumns()),
            input_stream,
            file_id,
            store_path);

        store->preIngestFile(store_path, file_id, dmfile->getBytesOnDisk());

        const auto & pk_column = block.getByPosition(0).column;
        auto min_pk = pk_column->getInt(0);
        auto max_pk = pk_column->getInt(block.rows() - 1);
        HandleRange range(min_pk, max_pk + 1);
        auto handle_range = RowKeyRange::fromHandleRange(range);
        auto external_file = ExternalDTFileInfo{.id = file_id, .range = handle_range};
        // There are some duplicated info. This is to minimize the change to our test code.
        return {handle_range, {external_file}};
    }

protected:
    DeltaMergeStorePtr store;
};

enum class TestMode
{
    PageStorageV2_MemoryOnly,
    PageStorageV2_DiskOnly,
    PageStorageV2_MemoryAndDisk,
    Current_MemoryOnly,
    Current_DiskOnly,
    Current_MemoryAndDisk
};

// Read write test suit for DeltaMergeStore.
// We will instantiate test cases for different `TestMode`
// to test with different pack types.
class DeltaMergeStoreRWTest
    : public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface<TestMode>
{
public:
    DeltaMergeStoreRWTest()
        : current_version(STORAGE_FORMAT_CURRENT)
    {
        mode = GetParam();

        switch (mode)
        {
        case TestMode::PageStorageV2_MemoryOnly:
        case TestMode::PageStorageV2_DiskOnly:
        case TestMode::PageStorageV2_MemoryAndDisk:
            setStorageFormat(3);
            break;
        case TestMode::Current_MemoryOnly:
        case TestMode::Current_DiskOnly:
        case TestMode::Current_MemoryAndDisk:
            setStorageFormat(STORAGE_FORMAT_CURRENT);
            break;
        }
    }

    ~DeltaMergeStoreRWTest() override { setStorageFormat(current_version); }

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        store = reload();
        dm_context = store->newDMContext(*db_context, db_context->getSettingsRef());
    }

    DeltaMergeStorePtr reload(
        const ColumnDefinesPtr & pre_define_columns = {},
        bool is_common_handle = false,
        size_t rowkey_column_size = 1)
    {
        TiFlashStorageTestBasic::reload();
        ColumnDefinesPtr cols;
        if (!pre_define_columns)
            cols = DMTestEnv::getDefaultColumns(
                is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        else
            cols = pre_define_columns;

        ColumnDefine handle_column_define = (*cols)[0];

        DeltaMergeStorePtr s = DeltaMergeStore::create(
            *db_context,
            false,
            "test",
            "t_101",
            NullspaceID,
            101,
            /*pk_col_id*/ 0,
            true,
            *cols,
            handle_column_define,
            is_common_handle,
            rowkey_column_size,
            nullptr,
            DeltaMergeStore::Settings());
        return s;
    }

    std::pair<RowKeyRange, std::vector<ExternalDTFileInfo>> genDMFile(DMContext & context, const Block & block)
    {
        Blocks blocks{block};
        return genDMFileByBlocks(context, blocks);
    }

    std::pair<RowKeyRange, std::vector<ExternalDTFileInfo>> genDMFileByBlocks(
        DMContext & context,
        const Blocks & blocks)
    {
        BlocksList block_list;

        Int64 min_pk = std::numeric_limits<Int64>::max();
        Int64 max_pk = std::numeric_limits<Int64>::min();
        for (const auto & b : std::ranges::reverse_view(blocks))
        {
            const auto & pk_column = b.getByPosition(0).column;
            min_pk = std::min(min_pk, pk_column->getInt(0));
            max_pk = std::max(max_pk, pk_column->getInt(b.rows() - 1));
            block_list.push_front(b);
        }

        auto input_stream = std::make_shared<BlocksListBlockInputStream>(std::move(block_list));
        auto [store_path, file_id] = store->preAllocateIngestFile();

        auto dmfile = writeIntoNewDMFile(
            context,
            std::make_shared<ColumnDefines>(store->getTableColumns()),
            input_stream,
            file_id,
            store_path);

        store->preIngestFile(store_path, file_id, dmfile->getBytesOnDisk());

        auto handle_range = RowKeyRange::fromHandleRange(HandleRange(min_pk, max_pk + 1));
        auto external_file = ExternalDTFileInfo{.id = file_id, .range = handle_range};
        // There are some duplicated info. This is to minimize the change to our test code.
        return {
            handle_range,
            {external_file},
        };
    }

protected:
    TestMode mode;
    DeltaMergeStorePtr store;
    DMContextPtr dm_context;
    StorageFormatVersion current_version;

    constexpr static const char * TRACING_NAME = "DeltaMergeStoreRWTest";

    void dupHandleVersionAndDeltaIndexAdvancedThanSnapshot();
};

} // namespace DB::DM::tests
