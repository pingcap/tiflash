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

#include <Common/SyncPoint/SyncPoint.h>
#include <Flash/Management/ManualCompact.h>
#include <Interpreters/Context.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/MultiSegmentTestUtil.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/StorageDeltaMerge.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <common/types.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>

#include <ext/scope_guard.h>
#include <future>
#include <thread>

namespace DB
{

namespace tests
{
class BasicManualCompactTest
    : public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface<DM::tests::DMTestEnv::PkType>
{
public:
    static constexpr TableID TABLE_ID = 5;

    BasicManualCompactTest() { pk_type = GetParam(); }

    void SetUp() override
    {
        try
        {
            TiFlashStorageTestBasic::SetUp();

            manager = std::make_unique<DB::Management::ManualCompactManager>(*db_context, db_context->getSettingsRef());

            setupStorage();

            // In tests let's only compact one segment.
            db_context->setSetting("manual_compact_more_until_ms", Field(static_cast<UInt64>(0)));

            // Split into 4 segments, and prepare some delta data for first 3 segments.
            helper = std::make_unique<DM::tests::MultiSegmentTestUtil>(*db_context);
            helper->prepareSegments(storage->getAndMaybeInitStore(), 50, pk_type);
            prepareDataForFirstThreeSegments();
        }
        CATCH
    }

    void setupStorage()
    {
        auto columns = DM::tests::DMTestEnv::getDefaultTableColumns(pk_type);
        auto table_info = DM::tests::DMTestEnv::getMinimalTableInfo(TABLE_ID, pk_type);
        auto astptr = DM::tests::DMTestEnv::getPrimaryKeyExpr("test_table", pk_type);

        storage = StorageDeltaMerge::create(
            "TiFlash",
            "default" /* db_name */,
            "test_table" /* table_name */,
            table_info,
            ColumnsDescription{columns},
            astptr,
            0,
            db_context->getGlobalContext());
        storage->startup();
    }

    void prepareDataForFirstThreeSegments()
    {
        // Write data to first 3 segments.
        auto newly_written_rows
            = helper->rows_by_segments[0] + helper->rows_by_segments[1] + helper->rows_by_segments[2];
        Block block
            = DM::tests::DMTestEnv::prepareSimpleWriteBlock(0, newly_written_rows, false, pk_type, 5 /* new tso */);
        storage->write(block, db_context->getSettingsRef());
        storage->flushCache(*db_context);

        helper->expected_delta_rows[0] += helper->rows_by_segments[0];
        helper->expected_delta_rows[1] += helper->rows_by_segments[1];
        helper->expected_delta_rows[2] += helper->rows_by_segments[2];
        helper->verifyExpectedRowsForAllSegments();
    }

    void TearDown() override
    {
        storage->drop();
        db_context->getTMTContext().getStorages().remove(NullspaceID, TABLE_ID);
    }

protected:
    std::unique_ptr<DM::tests::MultiSegmentTestUtil> helper;
    StorageDeltaMergePtr storage;
    std::unique_ptr<DB::Management::ManualCompactManager> manager;

    DM::tests::DMTestEnv::PkType pk_type;
};


INSTANTIATE_TEST_CASE_P(
    ByCommonHandle,
    BasicManualCompactTest,
    testing::Values(
        DM::tests::DMTestEnv::PkType::HiddenTiDBRowID,
        DM::tests::DMTestEnv::PkType::CommonHandle,
        DM::tests::DMTestEnv::PkType::PkIsHandleInt64),
    [](const testing::TestParamInfo<DM::tests::DMTestEnv::PkType> & info) {
        return DM::tests::DMTestEnv::PkTypeToString(info.param);
    });


TEST_P(BasicManualCompactTest, EmptyRequest)
try
{
    auto request = ::kvrpcpb::CompactRequest();
    auto response = ::kvrpcpb::CompactResponse();
    auto status_code = manager->handleRequest(&request, &response);
    ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
    ASSERT_TRUE(response.has_error());
    ASSERT_TRUE(response.error().has_err_physical_table_not_exist());
}
CATCH


TEST_P(BasicManualCompactTest, NonExistTableID)
try
{
    auto request = ::kvrpcpb::CompactRequest();
    request.set_physical_table_id(9999);
    auto response = ::kvrpcpb::CompactResponse();
    auto status_code = manager->handleRequest(&request, &response);
    ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
    ASSERT_TRUE(response.has_error());
    ASSERT_TRUE(response.error().has_err_physical_table_not_exist());
}
CATCH


TEST_P(BasicManualCompactTest, InvalidStartKey)
try
{
    auto request = ::kvrpcpb::CompactRequest();
    request.set_physical_table_id(TABLE_ID);
    request.set_start_key("abcd");
    auto response = ::kvrpcpb::CompactResponse();
    auto status_code = manager->handleRequest(&request, &response);
    ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
    ASSERT_TRUE(response.has_error());
    ASSERT_TRUE(response.error().has_err_invalid_start_key());
}
CATCH


TEST_P(BasicManualCompactTest, MalformedStartKey)
try
{
    // Specify an int key for common handle table, and vise versa.
    DM::RowKeyValue malformed_start_key;
    switch (pk_type)
    {
    case DM::tests::DMTestEnv::PkType::HiddenTiDBRowID:
    case DM::tests::DMTestEnv::PkType::PkIsHandleInt64:
        malformed_start_key = DM::RowKeyValue::COMMON_HANDLE_MIN_KEY;
        break;
    case DM::tests::DMTestEnv::PkType::CommonHandle:
        malformed_start_key = DM::RowKeyValue::INT_HANDLE_MIN_KEY;
        break;
    default:
        throw Exception("Unknown pk type for test");
    }

    auto request = ::kvrpcpb::CompactRequest();
    request.set_physical_table_id(TABLE_ID);
    WriteBufferFromOwnString wb;
    malformed_start_key.serialize(wb);
    request.set_start_key(wb.releaseStr());
    auto response = ::kvrpcpb::CompactResponse();
    auto status_code = manager->handleRequest(&request, &response);
    ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
    ASSERT_TRUE(response.has_error());
    ASSERT_TRUE(response.error().has_err_invalid_start_key());
}
CATCH


// Start key is not specified. Should compact the first segment.
TEST_P(BasicManualCompactTest, NoStartKey)
try
{
    auto request = ::kvrpcpb::CompactRequest();
    request.set_physical_table_id(TABLE_ID);
    auto response = ::kvrpcpb::CompactResponse();
    auto status_code = manager->handleRequest(&request, &response);
    ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
    ASSERT_FALSE(response.has_error());
    ASSERT_TRUE(response.has_remaining());

    helper->expected_stable_rows[0] += helper->expected_delta_rows[0];
    helper->expected_delta_rows[0] = 0;
    helper->verifyExpectedRowsForAllSegments();
}
CATCH


// Start key is empty. Should compact the first segment.
TEST_P(BasicManualCompactTest, EmptyStartKey)
try
{
    auto request = ::kvrpcpb::CompactRequest();
    request.set_physical_table_id(TABLE_ID);
    request.set_start_key("");
    auto response = ::kvrpcpb::CompactResponse();
    auto status_code = manager->handleRequest(&request, &response);
    ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
    ASSERT_FALSE(response.has_error());
    ASSERT_TRUE(response.has_remaining());

    helper->expected_stable_rows[0] += helper->expected_delta_rows[0];
    helper->expected_delta_rows[0] = 0;
    helper->verifyExpectedRowsForAllSegments();
}
CATCH


// Specify a key in segment[1]. Should compact this segment.
TEST_P(BasicManualCompactTest, SpecifiedStartKey)
try
{
    // TODO: This test may be not appropriate. It highly relies on internal implementation:
    // The encoding of the start key should be hidden from the caller.

    auto request = ::kvrpcpb::CompactRequest();
    request.set_physical_table_id(TABLE_ID);
    {
        WriteBufferFromOwnString wb;
        auto seg0 = storage->getAndMaybeInitStore()->segments.begin()->second;
        auto seg1_start_key = seg0->getRowKeyRange().end;
        seg1_start_key.toPrefixNext().serialize(wb);
        request.set_start_key(wb.releaseStr());
    }
    auto response = ::kvrpcpb::CompactResponse();
    auto status_code = manager->handleRequest(&request, &response);
    ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
    ASSERT_FALSE(response.has_error());
    ASSERT_TRUE(response.has_remaining());

    helper->expected_stable_rows[1] += helper->expected_delta_rows[1];
    helper->expected_delta_rows[1] = 0;
    helper->verifyExpectedRowsForAllSegments();
}
CATCH


TEST_P(BasicManualCompactTest, StartKeyFromPreviousResponse)
try
{
    ::kvrpcpb::CompactResponse response;
    {
        // Request 1
        auto request = ::kvrpcpb::CompactRequest();
        request.set_physical_table_id(TABLE_ID);
        response = ::kvrpcpb::CompactResponse();
        auto status_code = manager->handleRequest(&request, &response);
        ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
        ASSERT_FALSE(response.has_error());
        ASSERT_TRUE(response.has_remaining());

        helper->expected_stable_rows[0] += helper->expected_delta_rows[0];
        helper->expected_delta_rows[0] = 0;
        helper->verifyExpectedRowsForAllSegments();
    }
    {
        // Request 2, use the start key from previous response. We should compact both segment 1 and segment 2.
        auto request = ::kvrpcpb::CompactRequest();
        request.set_physical_table_id(TABLE_ID);
        request.set_start_key(response.compacted_end_key());
        response = ::kvrpcpb::CompactResponse();
        auto status_code = manager->handleRequest(&request, &response);
        ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
        ASSERT_FALSE(response.has_error());
        ASSERT_TRUE(response.has_remaining());

        helper->expected_stable_rows[1] += helper->expected_delta_rows[1];
        helper->expected_delta_rows[1] = 0;
        helper->verifyExpectedRowsForAllSegments();
    }
}
CATCH


TEST_P(BasicManualCompactTest, CompactMultiple)
try
{
    db_context->setSetting(
        "manual_compact_more_until_ms",
        Field(static_cast<UInt64>(60 * 1000))); // Hope it's long enough!

    auto request = ::kvrpcpb::CompactRequest();
    request.set_physical_table_id(TABLE_ID);
    request.set_keyspace_id(NullspaceID);
    auto response = ::kvrpcpb::CompactResponse();
    auto status_code = manager->handleRequest(&request, &response);
    ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
    ASSERT_FALSE(response.has_error()) << response.DebugString();
    ASSERT_FALSE(response.has_remaining()) << response.DebugString();

    // All segments should be compacted.
    for (size_t i = 0; i < 4; ++i)
    {
        helper->expected_stable_rows[i] += helper->expected_delta_rows[i];
        helper->expected_delta_rows[i] = 0;
    }
    helper->verifyExpectedRowsForAllSegments();
}
CATCH


// When there are duplicated logical id while processing, the later one should return error immediately.
TEST_P(BasicManualCompactTest, DuplicatedLogicalId)
try
{
    auto sp_req1_merge_delta = SyncPointCtl::enableInScope("before_DeltaMergeStore::mergeDeltaBySegment");
    auto req1 = std::async([&]() {
        auto request = ::kvrpcpb::CompactRequest();
        request.set_physical_table_id(TABLE_ID);
        request.set_logical_table_id(2);
        request.set_keyspace_id(NullspaceID);
        auto response = ::kvrpcpb::CompactResponse();
        auto status_code = manager->handleRequest(&request, &response);
        ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
        ASSERT_FALSE(response.has_error());
        helper->expected_stable_rows[0] += helper->expected_delta_rows[0];
        helper->expected_delta_rows[0] = 0;
        helper->verifyExpectedRowsForAllSegments();
    });

    sp_req1_merge_delta.waitAndPause();

    // req2: Another request with the same logical id.
    // Although worker pool size is 1, this request will be returned immediately with an error,
    // because there is already same logic id working in progress.
    {
        auto request = ::kvrpcpb::CompactRequest();
        request.set_physical_table_id(TABLE_ID);
        request.set_logical_table_id(2);
        request.set_keyspace_id(NullspaceID);
        request.set_api_version(::kvrpcpb::APIVersion::V1);
        auto response = ::kvrpcpb::CompactResponse();
        auto status_code = manager->handleRequest(&request, &response);
        ASSERT_EQ(status_code.error_code(), grpc::StatusCode::OK);
        ASSERT_TRUE(response.has_error());
        ASSERT_TRUE(response.error().has_err_compact_in_progress());
        helper->verifyExpectedRowsForAllSegments();
    }

    // Proceed the execution of req1. Everything should work normally.
    sp_req1_merge_delta.next();
    req1.wait();
}
CATCH


} // namespace tests
} // namespace DB
