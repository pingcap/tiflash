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
class TestFunctionBitAnd : public DB::tests::FunctionTest
{
};

#define ASSERT_BITAND(t1, t2, result) \
ASSERT_COLUMN_EQ(result, executeFunction("bitAnd", {t1, t2}))

TEST_F(TestFunctionBitAnd, Simple)
try
{
    ASSERT_BITAND(createColumn<Nullable<Int64>>({-1, 1}), createColumn<Nullable<Int64>>({0, 0}), createColumn<Nullable<Int64>>({0, 0}));
}
CATCH

/// Note: Only IntX and UIntX will be received by BitAnd, others will be casted by TiDB Planner.
TEST_F(TestFunctionBitAnd, TypePromotion)
try
{
    // Type Promotion
    ASSERT_BITAND(createColumn<Nullable<Int8>>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<Int16>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int16>>({1}), createColumn<Nullable<Int32>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int32>>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));

    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<Nullable<UInt16>>({0}), createColumn<Nullable<UInt16>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt16>>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<UInt32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt32>>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({0}));

    // Type Promotion across signed/unsigned
    ASSERT_BITAND(createColumn<Nullable<Int16>>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int64>>({1}), createColumn<Nullable<UInt8>>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt32>>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));
}
CATCH

TEST_F(TestFunctionBitAnd, Nullable)
try
{
    // Non Nullable
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Int16>({0}), createColumn<Int16>({0}));
    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<Int32>({0}), createColumn<Int32>({0}));
    ASSERT_BITAND(createColumn<Int32>({1}), createColumn<Int64>({0}), createColumn<Int64>({0}));
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Int64>({0}), createColumn<Int64>({0}));

    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<UInt16>({0}), createColumn<UInt16>({0}));
    ASSERT_BITAND(createColumn<UInt16>({1}), createColumn<UInt32>({0}), createColumn<UInt32>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<UInt64>({0}), createColumn<UInt64>({0}));

    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<UInt32>({0}), createColumn<Int32>({0}));
    ASSERT_BITAND(createColumn<Int64>({1}), createColumn<UInt8>({0}), createColumn<Int64>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<Int16>({0}), createColumn<Int32>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Int64>({0}), createColumn<Int64>({0}));

    // Across Nullable and non-Nullable
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<Int16>>({0}));
    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<Nullable<Int32>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Int32>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<Int8>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));

    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Nullable<UInt16>>({0}), createColumn<Nullable<UInt16>>({0}));
    ASSERT_BITAND(createColumn<UInt16>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<UInt32>>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Nullable<UInt64>>({0}), createColumn<Nullable<UInt64>>({0}));

    ASSERT_BITAND(createColumn<Int16>({1}), createColumn<Nullable<UInt32>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Int64>({1}), createColumn<Nullable<UInt8>>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<UInt32>({1}), createColumn<Nullable<Int16>>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<UInt8>({1}), createColumn<Nullable<Int64>>({0}), createColumn<Nullable<Int64>>({0}));

    ASSERT_BITAND(createColumn<Nullable<Int8>>({1}), createColumn<Int16>({0}), createColumn<Nullable<Int16>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int16>>({1}), createColumn<Int32>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int32>>({1}), createColumn<Int64>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({1}), createColumn<Int64>({0}), createColumn<Nullable<Int64>>({0}));

    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<UInt16>({0}), createColumn<Nullable<UInt16>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt16>>({1}), createColumn<UInt32>({0}), createColumn<Nullable<UInt32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt32>>({1}), createColumn<UInt64>({0}), createColumn<Nullable<UInt64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<UInt64>({0}), createColumn<Nullable<UInt64>>({0}));

    ASSERT_BITAND(createColumn<Nullable<Int16>>({1}), createColumn<UInt32>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<Int64>>({1}), createColumn<UInt8>({0}), createColumn<Nullable<Int64>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt32>>({1}), createColumn<Int16>({0}), createColumn<Nullable<Int32>>({0}));
    ASSERT_BITAND(createColumn<Nullable<UInt8>>({1}), createColumn<Int64>({0}), createColumn<Nullable<Int64>>({0}));
}
CATCH

TEST_F(TestFunctionBitAnd, TypeCastWithConst)
try
{
    /// need test these kinds of columns:
    /// 1. ColumnVector
    /// 2. ColumnVector<Nullable>
    /// 3. ColumnConst
    /// 4. ColumnConst<Nullable>, value != null
    /// 5. ColumnConst<Nullable>, value = null

    ASSERT_BITAND(createColumn<Int8>({0, 0, 1, 1}), createColumn<UInt64>({0, 1, 0, 1}), createColumn<Int64>({0, 0, 0, 1}));
    ASSERT_BITAND(createColumn<Int8>({0, 0, 1, 1}), createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}), createColumn<Nullable<Int64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createColumn<Int8>({0, 0, 1, 1}), createConstColumn<UInt64>(4, 0), createColumn<Int64>({0, 0, 0, 0}));
    ASSERT_BITAND(createColumn<Int8>({0, 0, 1, 1}), createConstColumn<Nullable<UInt64>>(4, 0), createColumn<Nullable<Int64>>({0, 0, 0, 0}));
    ASSERT_BITAND(createColumn<Int8>({0, 0, 1, 1}), createConstColumn<Nullable<UInt64>>(4, std::nullopt), createConstColumn<Nullable<Int64>>(4, std::nullopt)); // become const in wrapInNullable

    ASSERT_BITAND(createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}), createColumn<UInt64>({0, 1, 0, 1}), createColumn<Nullable<Int64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}), createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}), createColumn<Nullable<Int64>>({0, 1, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}), createConstColumn<UInt64>(4, 0), createColumn<Nullable<Int64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}), createConstColumn<Nullable<UInt64>>(4, 0), createColumn<Nullable<Int64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createColumn<Nullable<Int8>>({0, 1, std::nullopt, std::nullopt}), createConstColumn<Nullable<UInt64>>(4, std::nullopt), createConstColumn<Nullable<Int64>>(4, std::nullopt));

    ASSERT_BITAND(createConstColumn<Int8>(4, 0), createColumn<UInt64>({0, 1, 0, 1}), createColumn<Int64>({0, 0, 0, 0}));
    ASSERT_BITAND(createConstColumn<Int8>(4, 0), createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}), createColumn<Nullable<Int64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createConstColumn<Int8>(4, 0), createConstColumn<UInt64>(4, 0), createConstColumn<Int64>(4, 0));
    ASSERT_BITAND(createConstColumn<Int8>(4, 0), createConstColumn<Nullable<UInt64>>(4, 0), createConstColumn<Nullable<Int64>>(4, 0));
    ASSERT_BITAND(createConstColumn<Int8>(4, 0), createConstColumn<Nullable<UInt64>>(4, std::nullopt), createConstColumn<Nullable<Int64>>(4, std::nullopt));

    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, 0), createColumn<UInt64>({0, 1, 0, 1}), createColumn<Nullable<Int64>>({0, 0, 0, 0}));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, 0), createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}), createColumn<Nullable<Int64>>({0, 0, std::nullopt, std::nullopt}));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, 0), createConstColumn<UInt64>(4, 0), createConstColumn<Nullable<Int64>>(4, 0));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, 0), createConstColumn<Nullable<UInt64>>(4, 0), createConstColumn<Nullable<Int64>>(4, 0));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, 0), createConstColumn<Nullable<UInt64>>(4, std::nullopt), createConstColumn<Nullable<Int64>>(4, std::nullopt));

    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, std::nullopt), createColumn<UInt64>({0, 1, 0, 1}), createConstColumn<Nullable<Int64>>(4, std::nullopt));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, std::nullopt), createColumn<Nullable<UInt64>>({0, 1, std::nullopt, std::nullopt}), createConstColumn<Nullable<Int64>>(4, std::nullopt));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, std::nullopt), createConstColumn<UInt64>(4, 0), createConstColumn<Nullable<Int64>>(4, std::nullopt));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, std::nullopt), createConstColumn<Nullable<UInt64>>(4, 0), createConstColumn<Nullable<Int64>>(4, std::nullopt));
    ASSERT_BITAND(createConstColumn<Nullable<Int8>>(4, std::nullopt), createConstColumn<Nullable<UInt64>>(4, std::nullopt), createConstColumn<Nullable<Int64>>(4, std::nullopt));
}
CATCH

TEST_F(TestFunctionBitAnd, Boundary)
try
{
    ASSERT_BITAND(createColumn<Int8>({127, 127, -128, -128}), createColumn<UInt8>({0, 255, 0, 255}), createColumn<Int8>({0, 127, 0, -128}));
    ASSERT_BITAND(createColumn<Int8>({127, 127, -128, -128}), createColumn<UInt16>({0, 65535, 0, 65535}), createColumn<Int16>({0, 127, 0, -128}));
    ASSERT_BITAND(createColumn<Int16>({32767, 32767, -32768, -32768}), createColumn<UInt8>({0, 255, 0, 255}), createColumn<Int16>({0, 255, 0, 0}));

    ASSERT_BITAND(createColumn<Int64>({0, 0, 1, 1, -1, -1, INT64_MAX, INT64_MAX, INT64_MIN, INT64_MIN}),
                  createColumn<UInt64>({0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX}),
                  createColumn<Int64>({0, 0, 0, 1, 0, -1, 0, INT64_MAX, 0, INT64_MIN}));
}
CATCH

}
}
