#include <limits>

#include "Columns/ColumnsNumber.h"
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/DataTypeMyDateTime.h"
#include "DataTypes/DataTypeMyDuration.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesNumber.h"
#include "Functions/FunctionHelpers.h"
#include "TestUtils/FunctionTestUtils.h"
#include "common/types.h"
#include "gtest/gtest.h"

namespace DB::tests
{
namespace
{
auto getDatetimeColumn(bool single_field = false)
{
    MyDateTime datetime(2021, 10, 26, 16, 8, 59, 0);
    MyDateTime datetime_frac(2021, 10, 26, 16, 8, 59, 123456);

    auto col_datetime = ColumnUInt64::create();
    col_datetime->insert(Field(datetime.toPackedUInt()));
    if (!single_field)
        col_datetime->insert(Field(datetime_frac.toPackedUInt()));
    return col_datetime;
}

auto createCastTypeConstColumn(String str)
{
    return createConstColumn<String>(1, str);
}

const std::string func_name = "tidb_cast";

const Int8 MAX_INT8 = std::numeric_limits<Int8>::max();
const Int8 MIN_INT8 = std::numeric_limits<Int8>::min();
const Int16 MAX_INT16 = std::numeric_limits<Int16>::max();
const Int16 MIN_INT16 = std::numeric_limits<Int16>::min();
const Int32 MAX_INT32 = std::numeric_limits<Int32>::max();
const Int32 MIN_INT32 = std::numeric_limits<Int32>::min();
const Int64 MAX_INT64 = std::numeric_limits<Int64>::max();
const Int64 MIN_INT64 = std::numeric_limits<Int64>::min();
const UInt8 MAX_UINT8 = std::numeric_limits<UInt8>::max();
const UInt16 MAX_UINT16 = std::numeric_limits<UInt16>::max();
const UInt32 MAX_UINT32 = std::numeric_limits<UInt32>::max();
const UInt64 MAX_UINT64 = std::numeric_limits<UInt64>::max();

class TestTidbConversion : public DB::tests::FunctionTest
{
};

using DecimalField32 = DecimalField<Decimal32>;
using DecimalField64 = DecimalField<Decimal64>;
using DecimalField128 = DecimalField<Decimal128>;
using DecimalField256 = DecimalField<Decimal256>;

TEST_F(TestTidbConversion, castIntAsInt)
try
{
    /// null only cases
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({{}}),
        executeFunction(func_name,
                        {createOnlyNullColumn(1),
                         createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({{}}),
        executeFunction(func_name,
                        {createOnlyNullColumn(1),
                         createCastTypeConstColumn("Nullable(Int64)")}));

    /// const cases
    // uint8/16/32/64 -> uint64, no overflow
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_UINT8),
        executeFunction(func_name,
                        {createConstColumn<UInt8>(1, MAX_UINT8),
                         createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_UINT16),
        executeFunction(func_name,
                        {createConstColumn<UInt16>(1, MAX_UINT16),
                         createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_UINT32),
        executeFunction(func_name,
                        {createConstColumn<UInt32>(1, MAX_UINT32),
                         createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_UINT64),
        executeFunction(func_name,
                        {createConstColumn<UInt64>(1, MAX_UINT64),
                         createCastTypeConstColumn("UInt64")}));
    // int8/16/32/64 -> uint64, no overflow
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_INT8),
        executeFunction(func_name,
                        {createConstColumn<Int8>(1, MAX_INT8),
                         createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_INT16),
        executeFunction(func_name,
                        {createConstColumn<Int16>(1, MAX_INT16),
                         createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_INT32),
        executeFunction(func_name,
                        {createConstColumn<Int32>(1, MAX_INT32),
                         createCastTypeConstColumn("UInt64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt64>(1, MAX_INT64),
        executeFunction(func_name,
                        {createConstColumn<Int64>(1, MAX_INT64),
                         createCastTypeConstColumn("UInt64")}));
    // uint8/16/32 -> int64, no overflow
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_UINT8),
        executeFunction(func_name,
                        {createConstColumn<UInt8>(1, MAX_UINT8),
                         createCastTypeConstColumn("Int64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_UINT16),
        executeFunction(func_name,
                        {createConstColumn<UInt16>(1, MAX_UINT16),
                         createCastTypeConstColumn("Int64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_UINT32),
        executeFunction(func_name,
                        {createConstColumn<UInt32>(1, MAX_UINT32),
                         createCastTypeConstColumn("Int64")}));
    //  uint64 -> int64, will overflow
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, -1),
        executeFunction(func_name,
                        {createConstColumn<UInt64>(1, MAX_UINT64),
                         createCastTypeConstColumn("Int64")}));
    // int8/16/32/64 -> int64, no overflow
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_INT8),
        executeFunction(func_name,
                        {createConstColumn<Int8>(1, MAX_INT8),
                         createCastTypeConstColumn("Int64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_INT16),
        executeFunction(func_name,
                        {createConstColumn<Int16>(1, MAX_INT16),
                         createCastTypeConstColumn("Int64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_INT32),
        executeFunction(func_name,
                        {createConstColumn<Int32>(1, MAX_INT32),
                         createCastTypeConstColumn("Int64")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Int64>(1, MAX_INT64),
        executeFunction(func_name,
                        {createConstColumn<Int64>(1, MAX_INT64),
                         createCastTypeConstColumn("Int64")}));

    /// normal cases
    // uint8/16/32/64 -> uint64, no overflow
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, 1, MAX_UINT8, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt8>>({0, 1, MAX_UINT8, {}}),
                         createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, 1, MAX_UINT16, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt16>>({0, 1, MAX_UINT16, {}}),
                         createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, 1, MAX_UINT32, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt32>>({0, 1, MAX_UINT32, {}}),
                         createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, 1, MAX_UINT64, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt64>>({0, 1, MAX_UINT64, {}}),
                         createCastTypeConstColumn("Nullable(UInt64)")}));
    // int8/16/32/64 -> uint64, no overflow
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, MAX_INT8, MAX_UINT64, MAX_UINT64 - MAX_INT8, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int8>>({0, MAX_INT8, -1, MIN_INT8, {}}),
                         createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, MAX_INT16, MAX_UINT64, MAX_UINT64 - MAX_INT16, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int16>>({0, MAX_INT16, -1, MIN_INT16, {}}),
                         createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, MAX_INT32, MAX_UINT64, MAX_UINT64 - MAX_INT32, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({0, MAX_INT32, -1, MIN_INT32, {}}),
                         createCastTypeConstColumn("Nullable(UInt64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt64>>({0, MAX_INT64, MAX_UINT64, MAX_UINT64 - MAX_INT64, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int64>>({0, MAX_INT64, -1, MIN_INT64, {}}),
                         createCastTypeConstColumn("Nullable(UInt64)")}));
    // uint8/16/32 -> int64, no overflow
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT8, MAX_UINT8, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt8>>({0, MAX_INT8, MAX_UINT8, {}}),
                         createCastTypeConstColumn("Nullable(Int64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT16, MAX_UINT16, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt16>>({0, MAX_INT16, MAX_UINT16, {}}),
                         createCastTypeConstColumn("Nullable(Int64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT32, MAX_UINT32, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt32>>({0, MAX_INT32, MAX_UINT32, {}}),
                         createCastTypeConstColumn("Nullable(Int64)")}));
    //  uint64 -> int64, overflow may happen
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT64, -1, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt64>>({0, MAX_INT64, MAX_UINT64, {}}),
                         createCastTypeConstColumn("Nullable(Int64)")}));
    // int8/16/32/64 -> int64, no overflow
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT8, -1, MIN_INT8, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int8>>({0, MAX_INT8, -1, MIN_INT8, {}}),
                         createCastTypeConstColumn("Nullable(Int64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT16, -1, MIN_INT16, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int16>>({0, MAX_INT16, -1, MIN_INT16, {}}),
                         createCastTypeConstColumn("Nullable(Int64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT32, -1, MIN_INT32, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({0, MAX_INT32, -1, MIN_INT32, {}}),
                         createCastTypeConstColumn("Nullable(Int64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({0, MAX_INT64, -1, MIN_INT64, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int64>>({0, MAX_INT64, -1, MIN_INT64, {}}),
                         createCastTypeConstColumn("Nullable(Int64)")}));
}
CATCH

TEST_F(TestTidbConversion, castIntAsReal)
try
{
    // uint64/int64 -> float64, may be not precise
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {1234567890.0,
             123456789012345680.0,
             0.0,
             {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt64>>(
                             {1234567890, // this is fine
                              123456789012345678, // but this cannot be represented precisely in the IEEE 754 64-bit float format
                              0,
                              {}}),
                         createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {1234567890.0,
             123456789012345680.0,
             0.0,
             {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int64>>(
                             {1234567890, // this is fine
                              123456789012345678, // but this cannot be represented precisely in the IEEE 754 64-bit float format
                              0,
                              {}}),
                         createCastTypeConstColumn("Nullable(Float64)")}));
    // uint32/16/8 and int32/16/8 -> float64, precise
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_UINT32, 0, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt32>>({MAX_UINT32, 0, {}}),
                         createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_UINT16, 0, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt16>>({MAX_UINT16, 0, {}}),
                         createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_UINT8, 0, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt8>>({MAX_UINT8, 0, {}}),
                         createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_INT32, MIN_INT32, 0, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({MAX_INT32, MIN_INT32, 0, {}}),
                         createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_INT16, MIN_INT16, 0, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, 0, {}}),
                         createCastTypeConstColumn("Nullable(Float64)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({MAX_INT8, MIN_INT8, 0, {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, 0, {}}),
                         createCastTypeConstColumn("Nullable(Float64)")}));
}
CATCH

TEST_F(TestTidbConversion, castIntAsString)
try
{
    /// null only cases
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}}),
        executeFunction(func_name,
                        {createOnlyNullColumn(1),
                         createCastTypeConstColumn("Nullable(String)")}));

    /// const cases
    // uint64/32/16/8 -> string
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "18446744073709551615"),
        executeFunction(func_name,
                        {createConstColumn<UInt64>(1, MAX_UINT64),
                         createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "4294967295"),
        executeFunction(func_name,
                        {createConstColumn<UInt32>(1, MAX_UINT32),
                         createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "65535"),
        executeFunction(func_name,
                        {createConstColumn<UInt16>(1, MAX_UINT16),
                         createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "255"),
        executeFunction(func_name,
                        {createConstColumn<UInt8>(1, MAX_UINT8),
                         createCastTypeConstColumn("String")}));
    // int64/32/16/8 -> string
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "9223372036854775807"),
        executeFunction(func_name,
                        {createConstColumn<Int64>(1, MAX_INT64),
                         createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "2147483647"),
        executeFunction(func_name,
                        {createConstColumn<Int32>(1, MAX_INT32),
                         createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "32767"),
        executeFunction(func_name,
                        {createConstColumn<Int16>(1, MAX_INT16),
                         createCastTypeConstColumn("String")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "127"),
        executeFunction(func_name,
                        {createConstColumn<Int8>(1, MAX_INT8),
                         createCastTypeConstColumn("String")}));

    /// normal cases
    // uint64/32/16/8 -> string
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"18446744073709551615", "0", {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt64>>({MAX_UINT64, 0, {}}),
                         createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"4294967295", "0", {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt32>>({MAX_UINT32, 0, {}}),
                         createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"65535", "0", {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt16>>({MAX_UINT16, 0, {}}),
                         createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"255", "0", {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt8>>({MAX_UINT8, 0, {}}),
                         createCastTypeConstColumn("Nullable(String)")}));
    // int64/32/16/8 -> string
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"9223372036854775807", "-9223372036854775808", "0", {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int64>>({MAX_INT64, MIN_INT64, 0, {}}),
                         createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"2147483647", "-2147483648", "0", {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({MAX_INT32, MIN_INT32, 0, {}}),
                         createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"32767", "-32768", "0", {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, 0, {}}),
                         createCastTypeConstColumn("Nullable(String)")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"127", "-128", "0", {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, 0, {}}),
                         createCastTypeConstColumn("Nullable(String)")}));
}
CATCH

TEST_F(TestTidbConversion, castIntAsDecimal)
try
{
    // int8 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(MAX_INT8, 0), DecimalField32(MIN_INT8, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(MAX_INT8, 0), DecimalField64(MIN_INT8, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_INT8, 0), DecimalField128(MIN_INT8, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_INT8), 0), DecimalField256(static_cast<Int256>(MIN_INT8), 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int8>>({MAX_INT8, MIN_INT8, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // int16 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(MAX_INT16, 0), DecimalField32(MIN_INT16, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(MAX_INT16, 0), DecimalField64(MIN_INT16, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_INT16, 0), DecimalField128(MIN_INT16, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_INT16), 0), DecimalField256(static_cast<Int256>(MIN_INT16), 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int16>>({MAX_INT16, MIN_INT16, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // int32 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(999999999, 0), DecimalField32(-999999999, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({999999999, -999999999, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_THROW(executeFunction(func_name,
                                 {createColumn<Nullable<Int32>>({1000000000, -1000000000, {}}),
                                  createCastTypeConstColumn("Nullable(Decimal(9,0))")}),
                 TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(MAX_INT32, 0), DecimalField64(MIN_INT32, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({MAX_INT32, MIN_INT32, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_INT32, 0), DecimalField128(MIN_INT32, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({MAX_INT32, MIN_INT32, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_INT32), 0), DecimalField256(static_cast<Int256>(MIN_INT32), 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({MAX_INT32, MIN_INT32, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // int64 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(999999999, 0), DecimalField32(-999999999, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int64>>({999999999, -999999999, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_THROW(executeFunction(func_name,
                                 {createColumn<Nullable<Int64>>({1000000000, -1000000000, {}}),
                                  createCastTypeConstColumn("Nullable(Decimal(9,0))")}),
                 TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(999999999999999999, 0), DecimalField64(-999999999999999999, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int64>>({999999999999999999, -999999999999999999, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_THROW(executeFunction(func_name,
                                 {createColumn<Nullable<Int64>>({1000000000000000000, -1000000000000000000, {}}),
                                  createCastTypeConstColumn("Nullable(Decimal(18,0))")}),
                 TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_INT64, 0), DecimalField128(MIN_INT64, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int64>>({MAX_INT64, MIN_INT64, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_INT64), 0), DecimalField256(static_cast<Int256>(MIN_INT64), 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int64>>({MAX_INT64, MIN_INT64, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // uint8 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(MAX_UINT8, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt8>>({MAX_UINT8, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(MAX_UINT8, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt8>>({MAX_UINT8, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_UINT8, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt8>>({MAX_UINT8, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_UINT8), 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt8>>({MAX_UINT8, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // uint16 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(MAX_UINT16, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt16>>({MAX_UINT16, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(MAX_UINT16, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt16>>({MAX_UINT16, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_UINT16, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt16>>({MAX_UINT16, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_UINT16), 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt16>>({MAX_UINT16, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // uint32 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(999999999, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt32>>({999999999, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_THROW(executeFunction(func_name,
                                 {createColumn<Nullable<UInt32>>({1000000000, {}}),
                                  createCastTypeConstColumn("Nullable(Decimal(9,0))")}),
                 TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(MAX_UINT32, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt32>>({MAX_UINT32, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_UINT32, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt32>>({MAX_UINT32, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_UINT32), 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt32>>({MAX_UINT32, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(65,0))")}));
    // uint64 -> decimal32/64/128/256
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(9, 0),
            {DecimalField32(999999999, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt64>>({999999999, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(9,0))")}));
    ASSERT_THROW(executeFunction(func_name,
                                 {createColumn<Nullable<UInt64>>({1000000000, {}}),
                                  createCastTypeConstColumn("Nullable(Decimal(9,0))")}),
                 TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal64>>(
            std::make_tuple(18, 0),
            {DecimalField64(999999999999999999, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt64>>({999999999999999999, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(18,0))")}));
    ASSERT_THROW(executeFunction(func_name,
                                 {createColumn<Nullable<UInt64>>({1000000000000000000, {}}),
                                  createCastTypeConstColumn("Nullable(Decimal(18,0))")}),
                 TiFlashException);
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal128>>(
            std::make_tuple(38, 0),
            {DecimalField128(MAX_INT64, 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt64>>({MAX_INT64, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(38,0))")}));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal256>>(
            std::make_tuple(65, 0),
            {DecimalField256(static_cast<Int256>(MAX_INT64), 0), {}}),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt64>>({MAX_INT64, {}}),
                         createCastTypeConstColumn("Nullable(Decimal(65,0))")}));

    ASSERT_THROW(executeFunction(func_name,
                                 {createColumn<Nullable<Int32>>({9999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}),
                 TiFlashException);

    ASSERT_THROW(executeFunction(func_name,
                                 {createColumn<Nullable<Int32>>({-9999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}),
                 TiFlashException);

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(4, 1),
            {DecimalField32(static_cast<Int32>(9990), 1)}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(4, 1),
            {DecimalField32(static_cast<Int32>(-9990), 1)}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({-999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}));
}
CATCH

TEST_F(TestTidbConversion, castIntAsTime)
try
{
    ASSERT_COLUMN_EQ(
        createDateTimeColumnNullable({{}, {{2021, 10, 26, 16, 8, 59, 0}}}, 6),
        executeFunction(func_name,
                        {createColumn<Nullable<Int64>>({{}, 20211026160859}),
                         createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
    ASSERT_COLUMN_EQ(
        createDateTimeColumnNullable({{}, {{2021, 10, 26, 16, 8, 59, 0}}}, 6),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt64>>({{}, 20211026160859}),
                         createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
    ASSERT_THROW(
        executeFunction(func_name,
                        {createColumn<Nullable<UInt8>>({MAX_UINT8}),
                         createCastTypeConstColumn("Nullable(MyDateTime(6))")}),
        TiFlashException);
    ASSERT_THROW(
        executeFunction(func_name,
                        {createColumn<Nullable<UInt16>>({MAX_UINT16}),
                         createCastTypeConstColumn("Nullable(MyDateTime(6))")}),
        TiFlashException);
    ASSERT_THROW(
        executeFunction(func_name,
                        {createColumn<Nullable<UInt32>>({MAX_UINT32}),
                         createCastTypeConstColumn("Nullable(MyDateTime(6))")}),
        TiFlashException);
    ASSERT_COLUMN_EQ(
        createDateTimeColumnNullable({{}}, 6),
        executeFunction(func_name,
                        {createColumn<Nullable<UInt64>>({0}),
                         createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
    ASSERT_THROW(
        executeFunction(func_name,
                        {createColumn<Nullable<Int64>>({{}, -20211026160859}),
                         createCastTypeConstColumn("Nullable(MyDateTime(6))")}),
        TiFlashException);
}
CATCH

TEST_F(TestTidbConversion, castTimeAsReal)
try
{
    const auto data_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    const Float64 datetime_float = 20211026160859;
    const Float64 datetime_frac_float = 20211026160859.125;

    // cast datetime to float
    auto col_datetime1 = getDatetimeColumn();
    auto ctn_datetime1 = ColumnWithTypeAndName(std::move(col_datetime1), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({datetime_float, datetime_frac_float}),
        executeFunction(func_name,
                        {ctn_datetime1,
                         createCastTypeConstColumn("Float64")}));

    // cast datetime to nullable float
    auto col_datetime2 = getDatetimeColumn();
    auto ctn_datetime2 = ColumnWithTypeAndName(std::move(col_datetime2), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({datetime_float, datetime_frac_float}),
        executeFunction(func_name,
                        {ctn_datetime2,
                         createCastTypeConstColumn("Nullable(Float64)")}));

    // cast nullable datetime to nullable float
    auto col_datetime3 = getDatetimeColumn();
    auto datetime3_null_map = ColumnUInt8::create(2, 0);
    datetime3_null_map->getData()[1] = 1;
    auto col_datetime3_nullable = ColumnNullable::create(std::move(col_datetime3), std::move(datetime3_null_map));
    auto ctn_datetime3_nullable = ColumnWithTypeAndName(std::move(col_datetime3_nullable), makeNullable(data_type_ptr), "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({datetime_float, {}}),
        executeFunction(func_name,
                        {ctn_datetime3_nullable,
                         createCastTypeConstColumn("Nullable(Float64)")}));

    // cast const datetime to float
    auto col_datetime4_const = ColumnConst::create(getDatetimeColumn(true), 1);
    auto ctn_datetime4_const = ColumnWithTypeAndName(std::move(col_datetime4_const), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createConstColumn<Float64>(1, datetime_float),
        executeFunction(func_name,
                        {ctn_datetime4_const,
                         createCastTypeConstColumn("Float64")}));

    // cast nullable const datetime to float
    auto col_datetime5 = getDatetimeColumn(true);
    auto datetime5_null_map = ColumnUInt8::create(1, 0);
    auto col_datetime5_nullable = ColumnNullable::create(std::move(col_datetime5), std::move(datetime5_null_map));
    auto col_datetime5_nullable_const = ColumnConst::create(std::move(col_datetime5_nullable), 1);
    auto ctn_datetime5_nullable_const = ColumnWithTypeAndName(std::move(col_datetime5_nullable_const), makeNullable(data_type_ptr), "datetime");
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(1, datetime_float),
        executeFunction(func_name,
                        {ctn_datetime5_nullable_const,
                         createCastTypeConstColumn("Nullable(Float64)")}));
}
CATCH

TEST_F(TestTidbConversion, castDurationAsDuration)
try
{
    const auto from_type = std::make_shared<DataTypeMyDuration>(3);
    const auto to_type_1 = std::make_shared<DataTypeMyDuration>(5); // from_fsp <  to_fsp
    const auto to_type_2 = std::make_shared<DataTypeMyDuration>(3); // from_fsp == to_fsp
    const auto to_type_3 = std::make_shared<DataTypeMyDuration>(2); // from_fsp <  to_fsp

    ColumnWithTypeAndName input(
        createColumn<DataTypeMyDuration::FieldType>({(20 * 3600 + 20 * 60 + 20) * 1000000000L + 555000000L,
                                                     -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 555000000L,
                                                     (20 * 3600 + 20 * 60 + 20) * 1000000000L + 554000000L,
                                                     -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 554000000L,
                                                     (20 * 3600 + 20 * 60 + 20) * 1000000000L + 999000000L,
                                                     -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 999000000L})
            .column,
        from_type,
        "input");

    ColumnWithTypeAndName output1(input.column, to_type_1, "output1");
    ColumnWithTypeAndName output2(input.column, to_type_2, "output2");
    ColumnWithTypeAndName output3(
        createColumn<DataTypeMyDuration::FieldType>({(20 * 3600 + 20 * 60 + 20) * 1000000000L + 560000000L,
                                                     -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 560000000L,
                                                     (20 * 3600 + 20 * 60 + 20) * 1000000000L + 550000000L,
                                                     -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 550000000L,
                                                     (20 * 3600 + 20 * 60 + 21) * 1000000000L + 000000000L,
                                                     -(20 * 3600 + 20 * 60 + 21) * 1000000000L - 000000000L})
            .column,
        to_type_3,
        "output3");

    ASSERT_COLUMN_EQ(output1, executeFunction(func_name, {input, createCastTypeConstColumn(to_type_1->getName())}));
    ASSERT_COLUMN_EQ(output2, executeFunction(func_name, {input, createCastTypeConstColumn(to_type_2->getName())}));
    ASSERT_COLUMN_EQ(output3, executeFunction(func_name, {input, createCastTypeConstColumn(to_type_3->getName())}));

    // Test Nullable
    ColumnWithTypeAndName input_nullable(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({(20 * 3600 + 20 * 60 + 20) * 1000000000L + 555000000L,
                                                               -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 555000000L,
                                                               {},
                                                               (20 * 3600 + 20 * 60 + 20) * 1000000000L + 554000000L,
                                                               -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 554000000L,
                                                               {},
                                                               (20 * 3600 + 20 * 60 + 20) * 1000000000L + 999000000L,
                                                               -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 999000000L})
            .column,
        makeNullable(input.type),
        "input_nullable");
    ColumnWithTypeAndName output1_nullable(input_nullable.column, makeNullable(to_type_1), "output1_nullable");
    ColumnWithTypeAndName output2_nullable(input_nullable.column, makeNullable(to_type_2), "output2_nullable");
    ColumnWithTypeAndName output3_nullable(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({(20 * 3600 + 20 * 60 + 20) * 1000000000L + 560000000L,
                                                               -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 560000000L,
                                                               {},
                                                               (20 * 3600 + 20 * 60 + 20) * 1000000000L + 550000000L,
                                                               -(20 * 3600 + 20 * 60 + 20) * 1000000000L - 550000000L,
                                                               {},
                                                               (20 * 3600 + 20 * 60 + 21) * 1000000000L + 000000000L,
                                                               -(20 * 3600 + 20 * 60 + 21) * 1000000000L - 000000000L})
            .column,
        makeNullable(to_type_3),
        "output3_output");

    ASSERT_COLUMN_EQ(output1_nullable, executeFunction(func_name, {input_nullable, createCastTypeConstColumn(makeNullable(to_type_1)->getName())}));
    ASSERT_COLUMN_EQ(output2_nullable, executeFunction(func_name, {input_nullable, createCastTypeConstColumn(makeNullable(to_type_2)->getName())}));
    ASSERT_COLUMN_EQ(output3_nullable, executeFunction(func_name, {input_nullable, createCastTypeConstColumn(makeNullable(to_type_3)->getName())}));

    // Test Const
    ColumnWithTypeAndName input_const(createConstColumn<DataTypeMyDuration::FieldType>(1, (20 * 3600 + 20 * 60 + 20) * 1000000000L + 999000000L).column, from_type, "input_const");
    ColumnWithTypeAndName output1_const(input_const.column, to_type_1, "output1_const");
    ColumnWithTypeAndName output2_const(input_const.column, to_type_2, "output2_const");
    ColumnWithTypeAndName output3_const(createConstColumn<DataTypeMyDuration::FieldType>(1, (20 * 3600 + 20 * 60 + 21) * 1000000000L + 000000000L).column, to_type_3, "output3_const");

    ASSERT_COLUMN_EQ(output1_const, executeFunction(func_name, {input_const, createCastTypeConstColumn(to_type_1->getName())}));
    ASSERT_COLUMN_EQ(output2_const, executeFunction(func_name, {input_const, createCastTypeConstColumn(to_type_2->getName())}));
    ASSERT_COLUMN_EQ(output3_const, executeFunction(func_name, {input_const, createCastTypeConstColumn(to_type_3->getName())}));
}
CATCH

TEST_F(TestTidbConversion, StrToDateTypeTest)
try
{
    // Arg1 is ColumnVector, Arg2 is ColumnVector
    auto arg1_column = createColumn<Nullable<String>>({{}, "1/12/2020", "00:59:60 ", "1/12/2020"});
    auto arg2_column = createColumn<Nullable<String>>({"%d/%c/%Y", {}, "%H:%i:%S ", "%d/%c/%Y"});
    ColumnWithTypeAndName result_column(
        createColumn<Nullable<DataTypeMyDateTime::FieldType>>({{}, {}, {}, MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt()}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(non-null value)), Arg2 is ColumnVector
    arg1_column = createConstColumn<Nullable<String>>(2, {"1/12/2020"});
    arg2_column = createColumn<Nullable<String>>({"%d/%c/%Y", "%d/%c/%Y"});
    result_column = ColumnWithTypeAndName(
        createColumn<Nullable<DataTypeMyDateTime::FieldType>>({MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt(), MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt()}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(null value)), Arg2 is ColumnVector
    arg1_column = createConstColumn<Nullable<String>>(2, {});
    arg2_column = createColumn<Nullable<String>>({"%d/%c/%Y", "%d/%c/%Y"});
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(2, {}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnVector, Arg2 is ColumnConst(ColumnNullable(non-null value))
    arg1_column = createColumn<Nullable<String>>({"1/12/2020", "1/12/2020"});
    arg2_column = createConstColumn<Nullable<String>>(2, "%d/%c/%Y");
    result_column = ColumnWithTypeAndName(
        createColumn<Nullable<DataTypeMyDateTime::FieldType>>({MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt(), MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt()}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(non-null value)), Arg2 is ColumnConst(ColumnNullable(non-null value))
    arg1_column = createConstColumn<Nullable<String>>(2, "1/12/2020");
    arg2_column = createConstColumn<Nullable<String>>(2, "%d/%c/%Y");
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(2, {MyDateTime{2020, 12, 1, 0, 0, 0, 0}.toPackedUInt()}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(null value)), Arg2 is ColumnConst(ColumnNullable(non-null value))
    arg1_column = createConstColumn<Nullable<String>>(2, {});
    arg2_column = createConstColumn<Nullable<String>>(2, "%d/%c/%Y");
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(2, {}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnVector, Arg2 is ColumnConst(ColumnNullable(null value))
    arg1_column = createColumn<Nullable<String>>({"1/12/2020", "1/12/2020"});
    arg2_column = createConstColumn<Nullable<String>>(2, {});
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(2, {}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(non-null value)), Arg2 is ColumnConst(ColumnNullable(null value))
    arg1_column = createConstColumn<Nullable<String>>(2, {"1/12/2020"});
    arg2_column = createConstColumn<Nullable<String>>(2, {});
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(2, {}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));

    // Arg1 is ColumnConst(ColumnNullable(null value)), Arg2 is ColumnConst(ColumnNullable(null value))
    arg1_column = createConstColumn<Nullable<String>>(2, {});
    arg2_column = createConstColumn<Nullable<String>>(2, {});
    result_column = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(2, {}).column,
        makeNullable(std::make_shared<DataTypeMyDateTime>(0)),
        "result");
    ASSERT_COLUMN_EQ(result_column, executeFunction("strToDateDatetime", arg1_column, arg2_column));
}
CATCH

// for https://github.com/pingcap/tics/issues/4036
TEST_F(TestTidbConversion, castStringAsDateTime)
try
{
    auto input = std::vector<String>{"2012-12-12 12:12:12", "2012-12-12\t12:12:12", "2012-12-12\n12:12:12", "2012-12-12\v12:12:12", "2012-12-12\f12:12:12", "2012-12-12\r12:12:12"};
    auto to_column = createConstColumn<String>(1, "MyDateTime(6)");

    // vector
    auto from_column = createColumn<String>(input);
    UInt64 except_packed = MyDateTime(2012, 12, 12, 12, 12, 12, 0).toPackedUInt();
    auto vector_result = executeFunction("tidb_cast", {from_column, to_column});
    for (size_t i = 0; i < input.size(); i++)
    {
        ASSERT_EQ(except_packed, vector_result.column.get()->get64(i));
    }

    // const
    auto const_from_column = createConstColumn<String>(1, "2012-12-12\n12:12:12");
    auto const_result = executeFunction("tidb_cast", {from_column, to_column});
    ASSERT_EQ(except_packed, const_result.column.get()->get64(0));

    // nullable
    auto nullable_from_column = createColumn<Nullable<String>>({"2012-12-12 12:12:12", "2012-12-12\t12:12:12", "2012-12-12\n12:12:12", "2012-12-12\v12:12:12", "2012-12-12\f12:12:12", "2012-12-12\r12:12:12"});
    auto nullable_result = executeFunction("tidb_cast", {from_column, to_column});
    for (size_t i = 0; i < input.size(); i++)
    {
        ASSERT_EQ(except_packed, nullable_result.column.get()->get64(i));
    }
}
CATCH

} // namespace
} // namespace DB::tests
