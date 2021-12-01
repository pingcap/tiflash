#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <iostream>

#include <string>
#include <vector>
#include "Core/Field.h"
#include "DataTypes/DataTypeNothing.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/IDataType.h"
#include "common/types.h"

namespace DB::tests
{
class LeastTest : public DB::tests::FunctionTest
{
protected:

    using DecimalField32 = DecimalField<Decimal32>;
    using DecimalField64 = DecimalField<Decimal64>;
    using DecimalField128 = DecimalField<Decimal128>;
    using DecimalField256 = DecimalField<Decimal256>;
};

TEST_F(LeastTest, testBasic)
try
{
    const String & func_name = "tidbLeast";

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({1}),
        executeFunction(
            func_name,
            createColumn<Int8>({2}),
            createColumn<Int8>({1}),
            createColumn<Int8>({3}),
            createColumn<Int8>({4}),
            createColumn<Int32>({5})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({7}),
        executeFunction(
            func_name,
            createColumn<Int16>({10}),
            createColumn<Int32>({7}),
            createColumn<Int64>({8})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({7}),
        executeFunction(
            func_name,
            createColumn<Int8>({10}),
            createColumn<Int8>({7}),
            createColumn<Int64>({8})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({1}),
        executeFunction(
            func_name,
            createColumn<Int8>({2}),
            createColumn<Int32>({1}),
            createColumn<Int64>({3}),
            createColumn<Int16>({4}),
            createColumn<Int8>({5})));

    // consider null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int8>>({}),
            createColumn<Nullable<Int16>>({4}),
            createColumn<Nullable<Int32>>({}),
            createColumn<Nullable<Int64>>({})));
    
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(21, 16),
            {"10.0000000000000000"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(20, 16),
                {"10.0000000000000000"}),
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(21, 15),
                {"12.000000000000000"})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(21, 16),
            {"10.0000000000000000"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(21, 16),
                {"10.0000000000000000"}),
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {"12.000"})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(20, 16),
            {DecimalField128(1300, 3), DecimalField128(-3300, 3), DecimalField128(-1300, 3), DecimalField128(-3300, 3), DecimalField128(0, 3), {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(20, 16),
                {DecimalField128(3300, 3), DecimalField128(-3300, 3), DecimalField128(3300, 3), DecimalField128(-3300, 3), DecimalField128(1000, 3), {}, DecimalField128(0, 3), {}}),
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(20, 16),
                {DecimalField128(1300, 3), DecimalField128(1300, 3), DecimalField128(-1300, 3), DecimalField128(-1300, 3), DecimalField128(0, 3), DecimalField128(0, 3), {}, {}})));    

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(7, 3),
            {DecimalField32(1300, 3), DecimalField32(-3300, 3), DecimalField32(-1300, 3), DecimalField32(-3300, 3), DecimalField32(0, 3), {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(3300, 3), DecimalField32(-3300, 3), DecimalField32(3300, 3), DecimalField32(-3300, 3), DecimalField32(1000, 3), {}, DecimalField32(0, 3), {}}),
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(1300, 3), DecimalField32(1300, 3), DecimalField32(-1300, 3), DecimalField32(-1300, 3), DecimalField32(0, 3), DecimalField32(0, 3), {}, {}})));

    // real least
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {1.1, -1.4, -1.1, -1.3, 1.1, -3.3, -1.1, -3.48, -12.34, 0.0, 0.0, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({1.4, -1.4, 1.3, -1.3, 3.3, -3.3, 3.3, -3.3, 12.34, 0.0, 0.0, 0.0, {}, {}}),
            createColumn<Nullable<Float64>>({1.3, -1.3, 1.3, -1.3, 3.3, -3.3, 3.3, -3.48, -12.34, 0.0, 0.0, 0.0, {}, {}}),
            createColumn<Nullable<Float64>>({1.1, 1.1, -1.1, -1.1, 1.1, 1.1, -1.1, -1.1, 0.0, 12.34, 0.0, {}, 0.0, {}})));
    
    
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({-2, 0, -12, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({1.55, 1.55, 0, 0.0, {}}),
            createColumn<Nullable<Int32>>({-2, 3, -12, 0, {}}),
            createColumn<Nullable<Int64>>({-1, 0, 0, {}, {}})));
    
    // decimal least
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({1, DecimalField32(-1250, 3), {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(1250, 3), DecimalField32(-1250, 3), {}, DecimalField32(0, 3), {}}),
            createColumn<Nullable<Float64>>({1.0, 0.0, 0.0, {}, {}})));
    
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({1, DecimalField32(-1250, 3), {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(1250, 3), DecimalField32(-1250, 3), {}, DecimalField32(0, 3), {}}),
            createColumn<Nullable<Float32>>({1.0, 0.0, 0.0, {}, {}})));

    // const-vector least
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({-3, -11, -3, -3, -3, -5, -3}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(7, -2),
            createConstColumn<Nullable<Int64>>(7,-3),
            createColumn<Nullable<Int64>>({0, -11, 2, -3, 4, -5, 6})));

    // vector-const least
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, 1, 2, 3}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({0, 1, 2, 3}),
            createConstColumn<Nullable<Int64>>(4, 3)));

    // const-const least
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(1, -3),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(1, 5),
            createConstColumn<Nullable<Int64>>(1, -3)));
}

CATCH

} // namespace DB::tests
