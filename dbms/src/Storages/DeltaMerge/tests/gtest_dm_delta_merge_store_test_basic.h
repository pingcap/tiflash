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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/MultiSegmentTestUtil.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace DM
{


extern DMFilePtr writeIntoNewDMFile(DMContext & dm_context,
                                    const ColumnDefinesPtr & schema_snap,
                                    const BlockInputStreamPtr & input_stream,
                                    UInt64 file_id,
                                    const String & parent_path,
                                    DMFileBlockOutputStream::Flags flags);
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

    DeltaMergeStorePtr
    reload(const ColumnDefinesPtr & pre_define_columns = {}, bool is_common_handle = false, size_t rowkey_column_size = 1)
    {
        TiFlashStorageTestBasic::reload();
        ColumnDefinesPtr cols;
        if (!pre_define_columns)
            cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        else
            cols = pre_define_columns;

        ColumnDefine handle_column_define = (*cols)[0];

        DeltaMergeStorePtr s = std::make_shared<DeltaMergeStore>(*db_context,
                                                                 false,
                                                                 "test",
                                                                 "t_100",
                                                                 100,
                                                                 *cols,
                                                                 handle_column_define,
                                                                 is_common_handle,
                                                                 rowkey_column_size,
                                                                 DeltaMergeStore::Settings());
        return s;
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
        }
    }

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        store = reload();
    }

    DeltaMergeStorePtr
    reload(const ColumnDefinesPtr & pre_define_columns = {}, bool is_common_handle = false, size_t rowkey_column_size = 1)
    {
        TiFlashStorageTestBasic::reload();
        ColumnDefinesPtr cols;
        if (!pre_define_columns)
            cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        else
            cols = pre_define_columns;

        ColumnDefine handle_column_define = (*cols)[0];

        DeltaMergeStorePtr s = std::make_shared<DeltaMergeStore>(*db_context,
                                                                 false,
                                                                 "test",
                                                                 "t_101",
                                                                 101,
                                                                 *cols,
                                                                 handle_column_define,
                                                                 is_common_handle,
                                                                 rowkey_column_size,
                                                                 DeltaMergeStore::Settings());
        return s;
    }

    std::pair<RowKeyRange, PageIds> genDMFile(DMContext & context, const Block & block)
    {
        auto input_stream = std::make_shared<OneBlockInputStream>(block);
        auto [store_path, file_id] = store->preAllocateIngestFile();

        DMFileBlockOutputStream::Flags flags;
        flags.setSingleFile(DMTestEnv::getPseudoRandomNumber() % 2);

        auto dmfile = writeIntoNewDMFile(
            context,
            std::make_shared<ColumnDefines>(store->getTableColumns()),
            input_stream,
            file_id,
            store_path,
            flags);


        store->preIngestFile(store_path, file_id, dmfile->getBytesOnDisk());

        const auto & pk_column = block.getByPosition(0).column;
        auto min_pk = pk_column->getInt(0);
        auto max_pk = pk_column->getInt(block.rows() - 1);
        HandleRange range(min_pk, max_pk + 1);

        return {RowKeyRange::fromHandleRange(range), {file_id}};
    }

protected:
    TestMode mode;
    DeltaMergeStorePtr store;

    constexpr static const char * TRACING_NAME = "DeltaMergeStoreRWTest";
};
} // namespace tests
} // namespace DM
} // namespace DB
