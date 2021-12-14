#include <Core/Field.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/types.h>

#include <iostream>
#include <string>
#include <vector>

namespace DB::tests
{
class LeastGreatestTest : public DB::tests::FunctionTest
{
protected:
    using DecimalField32 = DecimalField<Decimal32>;
    using DecimalField64 = DecimalField<Decimal64>;
    using DecimalField128 = DecimalField<Decimal128>;
    using DecimalField256 = DecimalField<Decimal256>;
};

TEST_F(LeastGreatestTest, testLeast)
try
{
    const String & func_name = "tidbLeast";

    // ASSERT_COLUMN_EQ(
    //     createColumn<Int64>({1}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Int8>({2}),
    //         createColumn<Int8>({1}),
    //         createColumn<Int8>({3}),
    //         createColumn<Int8>({4}),
    //         createColumn<Int32>({5})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Int64>({7}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Int16>({10}),
    //         createColumn<Int32>({7}),
    //         createColumn<Int64>({8})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Int64>({7}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Int8>({10}),
    //         createColumn<Int8>({7}),
    //         createColumn<Int64>({8})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Int64>({1}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Int8>({2}),
    //         createColumn<Int32>({1}),
    //         createColumn<Int64>({3}),
    //         createColumn<Int16>({4}),
    //         createColumn<Int8>({5})));

    // // consider null
    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Int64>>({}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<Int8>>({}),
    //         createColumn<Nullable<Int16>>({4}),
    //         createColumn<Nullable<Int32>>({}),
    //         createColumn<Nullable<Int64>>({})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Decimal128>>(
    //         std::make_tuple(21, 16),
    //         {"10.0000000000000000"}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<Decimal128>>(
    //             std::make_tuple(20, 16),
    //             {"10.0000000000000000"}),
    //         createColumn<Nullable<Decimal128>>(
    //             std::make_tuple(21, 15),
    //             {"12.000000000000000"})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Decimal128>>(
    //         std::make_tuple(21, 16),
    //         {"5.0000000000000000"}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<Decimal128>>(
    //             std::make_tuple(21, 16),
    //             {"10.0000000000000000"}),
    //         createColumn<Nullable<Decimal32>>(
    //             std::make_tuple(7, 3),
    //             {"12.000"}),
    //         createColumn<Nullable<Decimal64>>(
    //             std::make_tuple(6, 3),
    //             {"5.000"})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Decimal128>>(
    //         std::make_tuple(20, 16),
    //         {DecimalField128(1300, 3), DecimalField128(-3300, 3), DecimalField128(-1300, 3), DecimalField128(-3300, 3), DecimalField128(0, 3), {}, {}, {}}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<Decimal128>>(
    //             std::make_tuple(20, 16),
    //             {DecimalField128(3300, 3), DecimalField128(-3300, 3), DecimalField128(3300, 3), DecimalField128(-3300, 3), DecimalField128(1000, 3), {}, DecimalField128(0, 3), {}}),
    //         createColumn<Nullable<Decimal128>>(
    //             std::make_tuple(20, 16),
    //             {DecimalField128(1300, 3), DecimalField128(1300, 3), DecimalField128(-1300, 3), DecimalField128(-1300, 3), DecimalField128(0, 3), DecimalField128(0, 3), {}, {}})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Decimal32>>(
    //         std::make_tuple(7, 3),
    //         {DecimalField32(1300, 3), DecimalField32(-3300, 3), DecimalField32(-1300, 3), DecimalField32(-3300, 3), DecimalField32(0, 3), {}, {}, {}}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<Decimal32>>(
    //             std::make_tuple(7, 3),
    //             {DecimalField32(3300, 3), DecimalField32(-3300, 3), DecimalField32(3300, 3), DecimalField32(-3300, 3), DecimalField32(1000, 3), {}, DecimalField32(0, 3), {}}),
    //         createColumn<Nullable<Decimal32>>(
    //             std::make_tuple(7, 3),
    //             {DecimalField32(1300, 3), DecimalField32(1300, 3), DecimalField32(-1300, 3), DecimalField32(-1300, 3), DecimalField32(0, 3), DecimalField32(0, 3), {}, {}})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Decimal64>>(
    //         std::make_tuple(15, 5),
    //         {DecimalField64(924400, 5)}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<Decimal32>>(
    //             std::make_tuple(7, 5),
    //             {DecimalField32(3223456, 5)}),
    //         createColumn<Nullable<Decimal64>>(
    //             std::make_tuple(15, 3),
    //             {DecimalField64(9244, 3)})));

    // // real least
    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Float64>>(
    //         {1.1, -1.4, -1.1, -1.3, 1.1, -3.3, -1.1, -3.48, -12.34, 0.0, 0.0, {}, {}, {}}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<Float64>>({1.4, -1.4, 1.3, -1.3, 3.3, -3.3, 3.3, -3.3, 12.34, 0.0, 0.0, 0.0, {}, {}}),
    //         createColumn<Nullable<Float64>>({1.3, -1.3, 1.3, -1.3, 3.3, -3.3, 3.3, -3.48, -12.34, 0.0, 0.0, 0.0, {}, {}}),
    //         createColumn<Nullable<Float64>>({1.1, 1.1, -1.1, -1.1, 1.1, 1.1, -1.1, -1.1, 0.0, 12.34, 0.0, {}, 0.0, {}})));


    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Float64>>({-2, 0, -12, {}, {}}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<Float64>>({1.55, 1.55, 0, 0.0, {}}),
    //         createColumn<Nullable<Int32>>({-2, 3, -12, 0, {}}),
    //         createColumn<Nullable<Int64>>({-1, 0, 0, {}, {}})));

    // // decimal least
    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Float64>>({1, DecimalField32(-1250, 3), {}, {}, {}}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<Decimal32>>(
    //             std::make_tuple(7, 3),
    //             {DecimalField32(1250, 3), DecimalField32(-1250, 3), {}, DecimalField32(0, 3), {}}),
    //         createColumn<Nullable<Float64>>({1.0, 0.0, 0.0, {}, {}})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Float64>>({1, DecimalField32(-1250, 3), {}, {}, {}}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<Decimal32>>(
    //             std::make_tuple(7, 3),
    //             {DecimalField32(1250, 3), DecimalField32(-1250, 3), {}, DecimalField32(0, 3), {}}),
    //         createColumn<Nullable<Float32>>({1.0, 0.0, 0.0, {}, {}})));

    // // const-vector least
    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Int64>>({-3, -11, -3, -3, -3, -5, -3}),
    //     executeFunction(
    //         func_name,
    //         createConstColumn<Nullable<Int64>>(7, -2),
    //         createConstColumn<Nullable<Int64>>(7, -3),
    //         createColumn<Nullable<Int64>>({0, -11, 2, -3, 4, -5, 6})));

    // // vector-const least
    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<Int64>>({0, 1, 2, 3}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<Int64>>({0, 1, 2, 3}),
    //         createConstColumn<Nullable<Int64>>(4, 3)));

    // // const-const least
    // ASSERT_COLUMN_EQ(
    //     createConstColumn<Nullable<Int64>>(1, -3),
    //     executeFunction(
    //         func_name,
    //         createConstColumn<Nullable<Int64>>(1, 5),
    //         createConstColumn<Nullable<Int64>>(1, -3)));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<UInt64>>({9223372036854775818U}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<UInt64>>({9223372036854775818U}),
    //         createColumn<Nullable<UInt64>>({9223372036854775820U})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<String>>({"sahdka"}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<String>>({"sahdka"}),
    //         createColumn<Nullable<String>>({"sahdkb"})));

    // // test cast
    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<String>>({"1"}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<String>>({"sahdka"}),
    //         createColumn<Nullable<UInt64>>({1})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"1"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"sahdka"}),
            createColumn<Nullable<UInt64>>({2}),
            createColumn<Nullable<UInt8>>({1})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<String>({"1"}),
    //     executeFunction(
    //         func_name,
    //         createColumn<String>({"sahdka"}),
    //         createColumn<UInt64>({2}),
    //         createColumn<UInt8>({1})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<String>>({"1"}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<String>>({"sahdka"}),
    //         createColumn<Nullable<UInt64>>({2}),
    //         createColumn<Nullable<UInt8>>({1}),
    //         createColumn<Nullable<Decimal32>>(
    //             std::make_tuple(4, 3),
    //             {DecimalField32(9244, 3)})));

    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<String>>({"10"}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<String>>({"sahdka"}),
    //         createColumn<Nullable<UInt64>>({20}),
    //         createColumn<Nullable<UInt8>>({10}),
    //         createColumn<Nullable<Decimal32>>(
    //             std::make_tuple(4, 3),
    //             {DecimalField32(9244, 3)})));

    // // string_string
    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<String>>({"sahdka", "1"}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<String>>({"sahdka", "1"}),
    //         createColumn<Nullable<String>>({"sahdkb", "2"})));


    // // string_constant
    // ASSERT_COLUMN_EQ(
    //     createColumn<Nullable<String>>({"sahdka", "sahdkb"}),
    //     executeFunction(
    //         func_name,
    //         createColumn<Nullable<String>>({"sahdka", "sahdkc"}),
    //         createConstColumn<Nullable<String>>(2, "sahdkb")));

    // // constant_contant
    // ASSERT_COLUMN_EQ(
    //     createConstColumn<Nullable<String>>(2, "sahdka"),
    //     executeFunction(
    //         func_name,
    //         createConstColumn<Nullable<String>>(2, "sahdka"),
    //         createConstColumn<Nullable<String>>(2, "sahdkb")));
}
CATCH

TEST_F(LeastGreatestTest, testGreatest)
try
{
    const String & func_name = "tidbGreatest";

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({5}),
        executeFunction(
            func_name,
            createColumn<Int8>({2}),
            createColumn<Int8>({1}),
            createColumn<Int8>({3}),
            createColumn<Int8>({4}),
            createColumn<Int32>({5})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({10}),
        executeFunction(
            func_name,
            createColumn<Int16>({10}),
            createColumn<Int32>({7}),
            createColumn<Int64>({8})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({10}),
        executeFunction(
            func_name,
            createColumn<Int8>({10}),
            createColumn<Int8>({7}),
            createColumn<Int64>({8})));

    ASSERT_COLUMN_EQ(
        createColumn<Int64>({5}),
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
            {"12.0000000000000000"}),
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
            {"12.0000000000000000"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal128>>(
                std::make_tuple(21, 16),
                {"10.0000000000000000"}),
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {"12.000"}),
            createColumn<Nullable<Decimal64>>(
                std::make_tuple(6, 3),
                {"5.000"})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(20, 16),
            {DecimalField128(3300, 3), DecimalField128(1300, 3), DecimalField128(3300, 3), DecimalField128(-1300, 3), DecimalField128(1000, 3), {}, {}, {}}),
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
            {DecimalField32(3300, 3), DecimalField32(1300, 3), DecimalField32(3300, 3), DecimalField32(-1300, 3), DecimalField32(1000, 3), {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(3300, 3), DecimalField32(-3300, 3), DecimalField32(3300, 3), DecimalField32(-3300, 3), DecimalField32(1000, 3), {}, DecimalField32(0, 3), {}}),
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(1300, 3), DecimalField32(1300, 3), DecimalField32(-1300, 3), DecimalField32(-1300, 3), DecimalField32(0, 3), DecimalField32(0, 3), {}, {}})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(15, 5),
            {DecimalField64(3223456, 5)}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 5),
                {DecimalField32(3223456, 5)}),
            createColumn<Nullable<Decimal64>>(
                std::make_tuple(15, 3),
                {DecimalField64(9244, 3)})));

    // real Greatest
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {1.4, 1.1, 1.3, -1.1, 3.3, 1.1, 3.3, -1.1, 12.34, 12.34, 0.0, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({1.4, -1.4, 1.3, -1.3, 3.3, -3.3, 3.3, -3.3, 12.34, 0.0, 0.0, 0.0, {}, {}}),
            createColumn<Nullable<Float64>>({1.3, -1.3, 1.3, -1.3, 3.3, -3.3, 3.3, -3.48, -12.34, 0.0, 0.0, 0.0, {}, {}}),
            createColumn<Nullable<Float64>>({1.1, 1.1, -1.1, -1.1, 1.1, 1.1, -1.1, -1.1, 0.0, 12.34, 0.0, {}, 0.0, {}})));


    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({1.55, 3, 0, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Float64>>({1.55, 1.55, 0, 0.0, {}}),
            createColumn<Nullable<Int32>>({-2, 3, -12, 0, {}}),
            createColumn<Nullable<Int64>>({-1, 0, 0, {}, {}})));

    // decimal least
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({DecimalField32(1250, 3), 0, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(1250, 3), DecimalField32(-1250, 3), {}, DecimalField32(0, 3), {}}),
            createColumn<Nullable<Float64>>({1.0, 0.0, 0.0, {}, {}})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({DecimalField32(1250, 3), 0, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(7, 3),
                {DecimalField32(1250, 3), DecimalField32(-1250, 3), {}, DecimalField32(0, 3), {}}),
            createColumn<Nullable<Float32>>({1.0, 0.0, 0.0, {}, {}})));

    // const-vector greatest
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, -2, 2, -2, 4, -2, 6}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(7, -2),
            createConstColumn<Nullable<Int64>>(7, -3),
            createColumn<Nullable<Int64>>({0, -11, 2, -3, 4, -5, 6})));

    // vector-const greatest
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({3, 3, 3, 3}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({0, 1, 2, 3}),
            createConstColumn<Nullable<Int64>>(4, 3)));

    // const-const greatest
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(1, 5),
        executeFunction(
            func_name,
            createConstColumn<Nullable<Int64>>(1, 5),
            createConstColumn<Nullable<Int64>>(1, -3)));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({9223372036854775820U}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt64>>({9223372036854775818U}),
            createColumn<Nullable<UInt64>>({9223372036854775820U})));


    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"sahdkb"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"sahdka"}),
            createColumn<Nullable<String>>({"sahdkb"})));

    // test cast
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"sahdka"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"sahdka"}),
            createColumn<Nullable<UInt64>>({1})));


    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"sahdka"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"sahdka"}),
            createColumn<Nullable<UInt64>>({2}),
            createColumn<Nullable<UInt8>>({1})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"sahdka"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"sahdka"}),
            createColumn<Nullable<UInt64>>({2}),
            createColumn<Nullable<UInt8>>({1}),
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(4, 3),
                {DecimalField32(9244, 3)})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"sahdka"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"sahdka"}),
            createColumn<Nullable<UInt64>>({20}),
            createColumn<Nullable<UInt8>>({10}),
            createColumn<Nullable<Decimal32>>(
                std::make_tuple(4, 3),
                {DecimalField32(9244, 3)})));

    // string_string
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"sahdkb", "2"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"sahdka", "1"}),
            createColumn<Nullable<String>>({"sahdkb", "2"})));

    // string_constant
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"sahdkb", "sahdkc"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"sahdka", "sahdkc"}),
            createConstColumn<Nullable<String>>(2, "sahdkb")));

    // constant_contant
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(2, "sahdkb"),
        executeFunction(
            func_name,
            createConstColumn<Nullable<String>>(2, "sahdka"),
            createConstColumn<Nullable<String>>(2, "sahdkb")));

    // date
    // ASSERT_COLUMN_EQ(
    //     createConstColumn<Nullable<DataTypeDate>>(2, "sahdkb"),
    //     executeFunction(
    //         func_name,
    //         createConstColumn<Nullable<String>>(2, "sahdka"),
    //         createConstColumn<Nullable<String>>(2, "sahdkb")));
}
CATCH


} // namespace DB::tests
