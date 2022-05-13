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

#include <Flash/Management/ManualCompact.h>
#include <Parsers/ASTIdentifier.h>
#include <Poco/Logger.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/tests/MultiSegmentTestUtils.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <common/types.h>
#include <gtest/gtest.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace tests
{


// Test for different kind of handles (Int / Common).
class BasicManualCompactTest
    : public DB::DM::tests::MultiSegmentTest
    , public testing::WithParamInterface<bool>
{
public:
    void SetUp() override
    {
        try
        {
            log = &Poco::Logger::get(DB::base::TiFlashStorageTestBasic::getCurrentFullTestName());
            is_common_handle = GetParam();
            TiFlashStorageTestBasic::SetUp();

            // In tests let's only compact one segment.
            db_context->setSetting("manual_compact_more_until_ms", UInt64(0));

            prepareSegments(50, is_common_handle);
            prepareManagedStorage();
            manager = std::make_unique<DB::Management::ManualCompactManager>(*db_context);
        }
        CATCH
    }

    void TearDown() override
    {
        // TODO: This is more like a hack. Should use storage->drop();
        db_context->getGlobalContext().getTMTContext().getStorages().remove(store->physical_table_id);
    }

    void prepareManagedStorage()
    {
        // TODO: This is more like a hack. Should construct a Storage using column definitions.
        const String table_name = "mytable";
        ASTPtr astptr(new ASTIdentifier(table_name, ASTIdentifier::Kind::Table));
        astptr->children.emplace_back(new ASTIdentifier("col1"));

        NamesAndTypesList columns = {{"col1", std::make_shared<DataTypeInt64>()}};

        TiDB::TableInfo ti;
        ti.id = store->physical_table_id;

        storage = StorageDeltaMerge::create("TiFlash",
                                            /* db_name= */ "default",
                                            table_name,
                                            std::ref(ti),
                                            ColumnsDescription{columns},
                                            astptr,
                                            0,
                                            db_context->getGlobalContext());
        storage->is_common_handle = store->is_common_handle;
        storage->rowkey_column_size = store->rowkey_column_size;
        storage->_store = store;
        storage->store_inited.store(true, std::memory_order_seq_cst);
        storage->startup();
    }

protected:
    bool is_common_handle;

    std::unique_ptr<DB::Management::ManualCompactManager> manager;

    StorageDeltaMergePtr storage;

    [[maybe_unused]] Poco::Logger * log;
};


INSTANTIATE_TEST_CASE_P(
    ByCommonHandle,
    BasicManualCompactTest,
    testing::Values(/* is_common_handle */ true, false));


TEST_P(BasicManualCompactTest, EmptyRequest)
try
{
    auto request = ::kvrpcpb::CompactRequest();
    auto response = ::kvrpcpb::CompactResponse();
    auto status_code = manager->doWork(&request, &response);
    ASSERT_EQ(status_code.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}
CATCH


TEST_P(BasicManualCompactTest, NonExistTableID)
try
{
    auto request = ::kvrpcpb::CompactRequest();
    request.set_physical_table_id(9999);
    auto response = ::kvrpcpb::CompactResponse();
    auto status_code = manager->doWork(&request, &response);
    ASSERT_EQ(status_code.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}
CATCH


TEST_P(BasicManualCompactTest, InvalidStartKey)
try
{
    auto request = ::kvrpcpb::CompactRequest();
    request.set_physical_table_id(store->physical_table_id);
    request.set_start_key("abcd");
    auto response = ::kvrpcpb::CompactResponse();
    auto status_code = manager->doWork(&request, &response);
    ASSERT_EQ(status_code.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}
CATCH


// Start key is not specified. Should compact the first segment.
TEST_P(BasicManualCompactTest, NoStartKey)
try
{
    {
        // Write data to first 3 segments.
        auto newly_written_rows = rows_by_segments[0] + rows_by_segments[1] + rows_by_segments[2];
        Block block = DM::tests::DMTestEnv::prepareSimpleWriteBlock(0, newly_written_rows, false, 5 /* new tso */, is_common_handle);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->flushCache(dm_context, DM::RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

        expected_delta_rows[0] += rows_by_segments[0];
        expected_delta_rows[1] += rows_by_segments[1];
        expected_delta_rows[2] += rows_by_segments[2];
        verifyExpectedRowsForAllSegments();
    }
    {
        auto request = ::kvrpcpb::CompactRequest();
        request.set_physical_table_id(store->physical_table_id);
        auto response = ::kvrpcpb::CompactResponse();
        auto status_code = manager->doWork(&request, &response);
        ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
        ASSERT_FALSE(response.has_error());
    }
    {
        expected_stable_rows[0] += expected_delta_rows[0];
        expected_delta_rows[0] = 0;
        verifyExpectedRowsForAllSegments();
    }
}
CATCH


TEST_P(BasicManualCompactTest, EmptyStartKey)
try
{
    {
        // Write data to first 3 segments.
        auto newly_written_rows = rows_by_segments[0] + rows_by_segments[1] + rows_by_segments[2];
        Block block = DM::tests::DMTestEnv::prepareSimpleWriteBlock(0, newly_written_rows, false, 5 /* new tso */, is_common_handle);
        store->write(*db_context, db_context->getSettingsRef(), block);
        store->flushCache(dm_context, DM::RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize()));

        expected_delta_rows[0] += rows_by_segments[0];
        expected_delta_rows[1] += rows_by_segments[1];
        expected_delta_rows[2] += rows_by_segments[2];
        verifyExpectedRowsForAllSegments();
    }
    {
        auto request = ::kvrpcpb::CompactRequest();
        request.set_physical_table_id(store->physical_table_id);
        request.set_start_key("");
        auto response = ::kvrpcpb::CompactResponse();
        auto status_code = manager->doWork(&request, &response);
        ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
        ASSERT_FALSE(response.has_error());
    }
    {
        expected_stable_rows[0] += expected_delta_rows[0];
        expected_delta_rows[0] = 0;
        verifyExpectedRowsForAllSegments();
    }
}
CATCH


} // namespace tests
} // namespace DB
