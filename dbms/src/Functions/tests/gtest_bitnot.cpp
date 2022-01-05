#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeNothing.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <unordered_map>
#include <vector>



namespace DB
{
namespace tests
{
// bitnot = tidb's bitneg.
class TestFunctionBitNot : public DB::tests::FunctionTest
{
};

#define ASSERT_BITNOT(t1, result) \
ASSERT_COLUMN_EQ(result, executeFunction("bitNot", {t1}))

TEST_F(TestFunctionBitNot, Simple)
try
{
    ASSERT_BITNOT(createColumn<Nullable<Int64>>({-1, 1}), createColumn<Nullable<Int64>>({0, -2}));
}
CATCH

/// Note: Only IntX and UIntX will be received by BitNot, others will be casted by TiDB Planner.
TEST_F(TestFunctionBitNot, TypeTest)
try
{
    ASSERT_BITNOT(createColumn<Nullable<Int8>>({1}), createColumn<Nullable<Int8>>({-2}));
    ASSERT_BITNOT(createColumn<Nullable<Int16>>({1}), createColumn<Nullable<Int16>>({-2}));
    ASSERT_BITNOT(createColumn<Nullable<Int32>>({1}), createColumn<Nullable<Int32>>({-2}));
    ASSERT_BITNOT(createColumn<Nullable<Int64>>({1}), createColumn<Nullable<Int64>>({-2}));

    ASSERT_BITNOT(createColumn<Nullable<UInt8>>({1}), createColumn<Nullable<UInt8>>({UINT8_MAX-1}));
    ASSERT_BITNOT(createColumn<Nullable<UInt16>>({1}), createColumn<Nullable<UInt16>>({UINT16_MAX-1}));
    ASSERT_BITNOT(createColumn<Nullable<UInt32>>({1}), createColumn<Nullable<UInt32>>({UINT32_MAX-1}));
    ASSERT_BITNOT(createColumn<Nullable<UInt64>>({1}), createColumn<Nullable<UInt64>>({UINT64_MAX-1}));

    ASSERT_BITNOT(createColumn<Int8>({0, 0, 1, 1}), createColumn<Int8>({-1, -1, -2, -2}));
    ASSERT_BITNOT(createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}), createColumn<Nullable<Int8>>({-1, -2, std::nullopt, std::nullopt}));
    ASSERT_BITNOT(createConstColumn<Int8>(4, 0), createConstColumn<Int8>(4, -1));
    ASSERT_BITNOT(createConstColumn<Nullable<Int8>>(4, 0), createConstColumn<Nullable<Int8>>(4, -1));
    ASSERT_BITNOT(createConstColumn<Nullable<Int8>>(4, std::nullopt), createConstColumn<Nullable<Int8>>(4, std::nullopt));
}
CATCH

TEST_F(TestFunctionBitNot, Boundary)
try
{
    ASSERT_BITNOT(createColumn<Int64>({0, 1, -1, INT64_MAX, INT64_MIN}),
                  createColumn<Int64>({-1, -2, 0, INT64_MIN, INT64_MAX}));

    ASSERT_BITNOT(createColumn<UInt64>({0, 1, UINT64_MAX}),
                  createColumn<UInt64>({UINT64_MAX, UINT64_MAX-1, 0}));
}

CATCH

}
}
