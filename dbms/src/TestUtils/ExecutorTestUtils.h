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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/ExecutorSerializer.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/mockExecutor.h>
#include <WindowFunctions/registerWindowFunctions.h>

#include <functional>

namespace DB::tests
{
TiDB::TP dataTypeToTP(const DataTypePtr & type);

ColumnsWithTypeAndName readBlock(BlockInputStreamPtr stream);
ColumnsWithTypeAndName readBlocks(std::vector<BlockInputStreamPtr> streams);

#define WRAP_FOR_TEST_BEGIN                        \
    std::vector<bool> pipeline_bools{false, true}; \
    for (auto enable_pipeline : pipeline_bools)    \
    {                                              \
        enablePipeline(enable_pipeline);

#define WRAP_FOR_TEST_END }

class ExecutorTest : public ::testing::Test
{
protected:
    void SetUp() override;

    void TearDown() override;

public:
    ExecutorTest()
        : context(TiFlashTestEnv::getContext())
    {}

    static void SetUpTestCase();

    virtual void initializeContext();

    void initializeClientInfo() const;

    DAGContext & getDAGContext();

    void enablePipeline(bool is_enable) const;

    static ::testing::AssertionResult dagRequestEqual(
        const char * lhs_expr,
        const char * rhs_expr,
        const String & expected_string,
        const std::shared_ptr<tipb::DAGRequest> & actual);

    void executeInterpreter(
        const String & expected_string,
        const std::shared_ptr<tipb::DAGRequest> & request,
        size_t concurrency);
    void executeInterpreterWithDeltaMerge(
        const String & expected_string,
        const std::shared_ptr<tipb::DAGRequest> & request,
        size_t concurrency);

    ColumnsWithTypeAndName executeRawQuery(const String & query, size_t concurrency = 1);
    void executeAndAssertColumnsEqual(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const ColumnsWithTypeAndName & expect_columns);

    // To check the output column with index = column_index sorted.
    struct SortInfo
    {
        size_t column_index;
        bool desc;
    };
    using SortInfos = std::vector<SortInfo>;

    // check whether the column in each output block sorted.
    void executeAndAssertSortedBlocks(const std::shared_ptr<tipb::DAGRequest> & request, const SortInfos & sort_infos);
    void executeAndAssertRowsEqual(const std::shared_ptr<tipb::DAGRequest> & request, size_t expect_rows);

    enum SourceType
    {
        TableScan,
        ExchangeReceiver
    };

    // for single source query, the source executor name is ${type}_0
    static String getSourceName(SourceType type)
    {
        switch (type)
        {
        case TableScan:
            return "table_scan_0";
        case ExchangeReceiver:
            return "exchange_receiver_0";
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown Executor Source type {}", fmt::underlying(type));
        }
    }

    ColumnsWithTypeAndName executeStreams(DAGContext * dag_context, bool is_internal = true);

    ColumnsWithTypeAndName executeStreams(const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency = 1);

    ColumnsWithTypeAndName executeStreamsWithMemoryTracker(
        const std::shared_ptr<tipb::DAGRequest> & request,
        size_t concurrency = 1);

    Blocks getExecuteStreamsReturnBlocks(const std::shared_ptr<tipb::DAGRequest> & request, size_t concurrency = 1);

    /// test execution summary
    // <rows, concurrency>
    using ProfileInfo = std::pair<int, int>;
    using Expect = std::unordered_map<String, ProfileInfo>;
    static constexpr int not_check_rows = -1;
    static constexpr int not_check_concurrency = -1;


    void testForExecutionSummary(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const Expect & expect,
        size_t concurrency = 10);

private:
    void executeExecutor(
        const std::shared_ptr<tipb::DAGRequest> & request,
        std::function<::testing::AssertionResult(const ColumnsWithTypeAndName &)> assert_func);

    void checkBlockSorted(
        const std::shared_ptr<tipb::DAGRequest> & request,
        const SortInfos & sort_infos,
        std::function<::testing::AssertionResult(const ColumnsWithTypeAndName &, const ColumnsWithTypeAndName &)>
            assert_func);

protected:
    MockDAGRequestContext context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};

#define ASSERT_DAGREQUEST_EQAUL(str, request) ASSERT_PRED_FORMAT2(ExecutorTest::dagRequestEqual, (str), (request));
#define ASSERT_BLOCKINPUTSTREAM_EQAUL(str, request, concurrency) executeInterpreter((str), (request), (concurrency))

// nullable type
using ColStringNullableType = std::optional<typename TypeTraits<String>::FieldType>;
using ColInt8NullableType = std::optional<typename TypeTraits<Int8>::FieldType>;
using ColInt16NullableType = std::optional<typename TypeTraits<Int16>::FieldType>;
using ColInt32NullableType = std::optional<typename TypeTraits<Int32>::FieldType>;
using ColInt64NullableType = std::optional<typename TypeTraits<Int64>::FieldType>;
using ColFloat32NullableType = std::optional<typename TypeTraits<Float32>::FieldType>;
using ColFloat64NullableType = std::optional<typename TypeTraits<Float64>::FieldType>;
using ColMyDateNullableType = std::optional<typename TypeTraits<MyDate>::FieldType>;
using ColMyDateTimeNullableType = std::optional<typename TypeTraits<MyDateTime>::FieldType>;
using ColDecimalNullableType = std::optional<typename TypeTraits<Decimal32>::FieldType>;

// non nullable type
using ColUInt64Type = typename TypeTraits<UInt64>::FieldType;
using ColInt64Type = typename TypeTraits<Int64>::FieldType;
using ColFloat64Type = typename TypeTraits<Float64>::FieldType;
using ColStringType = typename TypeTraits<String>::FieldType;

// nullable column
using ColumnWithNullableString = std::vector<ColStringNullableType>;
using ColumnWithNullableInt8 = std::vector<ColInt8NullableType>;
using ColumnWithNullableInt16 = std::vector<ColInt16NullableType>;
using ColumnWithNullableInt32 = std::vector<ColInt32NullableType>;
using ColumnWithNullableInt64 = std::vector<ColInt64NullableType>;
using ColumnWithNullableFloat32 = std::vector<ColFloat32NullableType>;
using ColumnWithNullableFloat64 = std::vector<ColFloat64NullableType>;
using ColumnWithNullableMyDate = std::vector<ColMyDateNullableType>;
using ColumnWithNullableMyDateTime = std::vector<ColMyDateTimeNullableType>;
using ColumnWithNullableDecimal = std::vector<ColDecimalNullableType>;

// non nullable column
using ColumnWithInt64 = std::vector<ColInt64Type>;
using ColumnWithUInt64 = std::vector<ColUInt64Type>;
using ColumnWithFloat64 = std::vector<ColFloat64Type>;
using ColumnWithString = std::vector<ColStringType>;
} // namespace DB::tests
