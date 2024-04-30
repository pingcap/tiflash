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

namespace DB
{
namespace DM
{
extern DMFilePtr writeIntoNewDMFile(
    DMContext & dm_context,
    const ColumnDefinesPtr & schema_snap,
    const BlockInputStreamPtr & input_stream,
    UInt64 file_id,
    const String & parent_path);
namespace tests
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

        DeltaMergeStorePtr s = std::make_shared<DeltaMergeStore>(
            *db_context,
            false,
            "test",
            "t_100",
            NullspaceID,
            100,
            true,
            *cols,
            handle_column_define,
            is_common_handle,
            rowkey_column_size,
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

enum TestMode
{
    V1_BlockOnly,
    V2_BlockOnly,
    V2_FileOnly,
    V2_Mix,
    V3_BlockOnly,
    V3_FileOnly,
    V3_Mix
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
    {
        mode = GetParam();

        switch (mode)
        {
        case TestMode::V1_BlockOnly:
            setStorageFormat(1);
            break;
        case TestMode::V2_BlockOnly:
        case TestMode::V2_FileOnly:
        case TestMode::V2_Mix:
            setStorageFormat(2);
            break;
        case TestMode::V3_BlockOnly:
        case TestMode::V3_FileOnly:
        case TestMode::V3_Mix:
            setStorageFormat(3);
            break;
        }
    }

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

        DeltaMergeStorePtr s = std::make_shared<DeltaMergeStore>(
            *db_context,
            false,
            "test",
            "t_101",
            NullspaceID,
            101,
            true,
            *cols,
            handle_column_define,
            is_common_handle,
            rowkey_column_size,
            DeltaMergeStore::Settings());
        return s;
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
        return {
            handle_range,
            {external_file}}; // There are some duplicated info. This is to minimize the change to our test code.
    }

protected:
    TestMode mode;
    DeltaMergeStorePtr store;
    DMContextPtr dm_context;

    constexpr static const char * TRACING_NAME = "DeltaMergeStoreRWTest";

    void dupHandleVersionAndDeltaIndexAdvancedThanSnapshot();
};
} // namespace tests
} // namespace DM
} // namespace DB
