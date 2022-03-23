#include "Columns/ColumnsNumber.h"
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/DataTypeMyDateTime.h"
#include "DataTypes/DataTypeMyDuration.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesNumber.h"
#include "Functions/FunctionFactory.h"
#include "Functions/FunctionHelpers.h"
#include "TestUtils/FunctionTestUtils.h"
#include "common/types.h"
#include "gtest/gtest.h"

namespace DB
{
namespace tests
{
class TestTidbConversion : public DB::tests::FunctionTest
{
public:
    static auto getDatetimeColumn(bool single_field = false)
    {
        MyDateTime datetime(2021, 10, 26, 16, 8, 59, 0);
        MyDateTime datetime_frac(2021, 10, 26, 16, 8, 59, 123456);

        auto col_datetime = ColumnUInt64::create();
        col_datetime->insert(Field(datetime.toPackedUInt()));
        if (!single_field)
            col_datetime->insert(Field(datetime_frac.toPackedUInt()));
        return col_datetime;
    }
};

TEST_F(TestTidbConversion, castTimestampAsReal)
try
{
<<<<<<< HEAD
    static const std::string func_name = "tidb_cast";
    static const auto data_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    static const Float64 datetime_float = 20211026160859;
    static const Float64 datetime_frac_float = 20211026160859.125;
=======
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

    DAGContext * dag_context = context.getDAGContext();
    UInt64 ori_flags = dag_context->getFlags();
    dag_context->addFlag(TiDBSQLFlags::OVERFLOW_AS_WARNING);
    dag_context->clearWarnings();

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(4, 1),
            {DecimalField32(static_cast<Int32>(9999), 1)}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({9999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(4, 1),
            {DecimalField32(static_cast<Int32>(-9999), 1)}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({-9999}), createCastTypeConstColumn("Nullable(Decimal(4, 1))")}));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(2, 2),
            {DecimalField32(static_cast<Int32>(99), 2)}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({9999}), createCastTypeConstColumn("Nullable(Decimal(2, 2))")}));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Decimal32>>(
            std::make_tuple(2, 2),
            {DecimalField32(static_cast<Int32>(-99), 2)}),
        executeFunction(func_name,
                        {createColumn<Nullable<Int32>>({-9999}), createCastTypeConstColumn("Nullable(Decimal(2, 2))")}));

    dag_context->setFlags(ori_flags);
}
CATCH

TEST_F(TestTidbConversion, castIntAsTime)
try
{
    ASSERT_COLUMN_EQ(
        createDateTimeColumn({{}, {{2021, 10, 26, 16, 8, 59, 0}}}, 6),
        executeFunction(func_name,
                        {createColumn<Nullable<Int64>>({{}, 20211026160859}),
                         createCastTypeConstColumn("Nullable(MyDateTime(6))")}));
    ASSERT_COLUMN_EQ(
        createDateTimeColumn({{}, {{2021, 10, 26, 16, 8, 59, 0}}}, 6),
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
        createDateTimeColumn({{}}, 6),
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

TEST_F(TestTidbConversion, castRealAsInt)
try
{
    testOnlyNull<Float32, Int64>();
    testOnlyNull<Float32, UInt64>();
    testOnlyNull<Float64, Int64>();
    testOnlyNull<Float64, UInt64>();

    testNotOnlyNull<Float32, Int64>(0, 0);
    testThrowException<Float32, Int64>(MAX_FLOAT32);
    testNotOnlyNull<Float32, Int64>(MIN_FLOAT32, 0);
    testNotOnlyNull<Float32, Int64>(12.213f, 12);
    testNotOnlyNull<Float32, Int64>(-12.213f, -12);
    testNotOnlyNull<Float32, Int64>(12.513f, 13);
    testNotOnlyNull<Float32, Int64>(-12.513f, -13);

    testNotOnlyNull<Float32, UInt64>(0, 0);
    testThrowException<Float32, UInt64>(MAX_FLOAT32);
    testNotOnlyNull<Float32, UInt64>(MIN_FLOAT32, 0);
    testNotOnlyNull<Float32, UInt64>(12.213f, 12);
    testThrowException<Float32, UInt64>(-12.213f);
    testNotOnlyNull<Float32, UInt64>(12.513f, 13);
    testThrowException<Float32, UInt64>(-12.513f);

    testNotOnlyNull<Float64, Int64>(0, 0);
    testThrowException<Float64, Int64>(MAX_FLOAT64);
    testNotOnlyNull<Float64, Int64>(MIN_FLOAT64, 0);
    testNotOnlyNull<Float64, Int64>(12.213, 12);
    testNotOnlyNull<Float64, Int64>(-12.213, -12);
    testNotOnlyNull<Float64, Int64>(12.513, 13);
    testNotOnlyNull<Float64, Int64>(-12.513, -13);

    testNotOnlyNull<Float64, UInt64>(0, 0);
    testThrowException<Float64, UInt64>(MAX_FLOAT64);
    testNotOnlyNull<Float64, UInt64>(MIN_FLOAT64, 0);
    testNotOnlyNull<Float64, UInt64>(12.213, 12);
    testThrowException<Float64, UInt64>(-12.213);
    testNotOnlyNull<Float64, Int64>(12.513, 13);
    testNotOnlyNull<Float64, Int64>(-12.513, -13);
}
CATCH

TEST_F(TestTidbConversion, castRealAsReal)
try
{
    testOnlyNull<Float32, Float64>();
    testOnlyNull<Float64, Float64>();

    testNotOnlyNull<Float32, Float64>(0, 0);
    testNotOnlyNull<Float32, Float64>(12.213, 12.213000297546387);
    testNotOnlyNull<Float32, Float64>(-12.213, -12.213000297546387);
    testNotOnlyNull<Float32, Float64>(MIN_FLOAT32, MIN_FLOAT32);
    testNotOnlyNull<Float32, Float64>(MAX_FLOAT32, MAX_FLOAT32);

    testNotOnlyNull<Float64, Float64>(0, 0);
    testNotOnlyNull<Float64, Float64>(12.213, 12.213);
    testNotOnlyNull<Float64, Float64>(-12.213, -12.213);
    testNotOnlyNull<Float64, Float64>(MIN_FLOAT64, MIN_FLOAT64);
    testNotOnlyNull<Float64, Float64>(MAX_FLOAT64, MAX_FLOAT64);
}
CATCH

TEST_F(TestTidbConversion, castRealAsString)
try
{
    testOnlyNull<Float32, String>();
    testOnlyNull<Float64, String>();

    // TODO add tests after non-expected results fixed

    testNotOnlyNull<Float32, String>(0, "0");
    testNotOnlyNull<Float32, String>(12.213, "12.213");
    testNotOnlyNull<Float32, String>(-12.213, "-12.213");
    // tiflash: 3.4028235e38
    // tidb: 340282350000000000000000000000000000000
    // mysql: 3.40282e38
    // testNotOnlyNull<Float32, String>(MAX_FLOAT32, "3.4028235e38");
    // tiflash: 1.1754944e-38
    // tidb: 0.000000000000000000000000000000000000011754944
    // mysql: 1.17549e-38
    // testNotOnlyNull<Float32, String>(MIN_FLOAT32, "1.1754944e-38");

    testNotOnlyNull<Float64, String>(0, "0");
    testNotOnlyNull<Float64, String>(12.213, "12.213");
    testNotOnlyNull<Float64, String>(-12.213, "-12.213");
    // tiflash: 1.7976931348623157e308
    // tidb: 179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
    // mysql: 1.7976931348623157e308
    // testNotOnlyNull<Float64, String>(MAX_FLOAT64, "1.7976931348623157e308");
    // tiflash: 2.2250738585072014e-308
    // tidb: 0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000022250738585072014
    // mysql: 2.2250738585072014e-308
    // testNotOnlyNull<Float64, String>(MIN_FLOAT64, "2.2250738585072014e-308");
}
CATCH

TEST_F(TestTidbConversion, castRealAsDecimal)
try
{
    testOnlyNull<Float32, Decimal32>();
    testOnlyNull<Float32, Decimal64>();
    testOnlyNull<Float32, Decimal128>();
    testOnlyNull<Float32, Decimal256>();
    testOnlyNull<Float64, Decimal32>();
    testOnlyNull<Float64, Decimal64>();
    testOnlyNull<Float64, Decimal128>();
    testOnlyNull<Float64, Decimal256>();

    // TODO fix:
    // for tidb, cast(12.213f as decimal(x, x)) throw warnings: Truncated incorrect DECIMAL value: '-12.21300029754638.
    // tiflash is same as mysql, don't throw warnings.

    testNotOnlyNull<Float32, Decimal32>(0, DecimalField32(0, 0), std::make_tuple(9, 0));
    testNotOnlyNull<Float32, Decimal32>(12.213f, DecimalField32(12213, 3), std::make_tuple(9, 3));
    testNotOnlyNull<Float32, Decimal32>(-12.213f, DecimalField32(-12213, 3), std::make_tuple(9, 3));
    testThrowException<Float32, Decimal32>(MAX_FLOAT32, std::make_tuple(9, 0));
    testNotOnlyNull<Float32, Decimal32>(MIN_FLOAT64, DecimalField32(0, 9), std::make_tuple(9, 9));

    testNotOnlyNull<Float32, Decimal64>(0, DecimalField64(0, 0), std::make_tuple(18, 0));
    testNotOnlyNull<Float32, Decimal64>(12.213f, DecimalField64(12213, 3), std::make_tuple(18, 3));
    testNotOnlyNull<Float32, Decimal64>(-12.213f, DecimalField64(-12213, 3), std::make_tuple(18, 3));
    testThrowException<Float32, Decimal64>(MAX_FLOAT32, std::make_tuple(18, 0));
    testNotOnlyNull<Float32, Decimal64>(MIN_FLOAT64, DecimalField64(0, 18), std::make_tuple(18, 18));

    testNotOnlyNull<Float32, Decimal128>(0, DecimalField128(0, 0), std::make_tuple(38, 0));
    testNotOnlyNull<Float32, Decimal128>(12.213f, DecimalField128(12213, 3), std::make_tuple(38, 3));
    testNotOnlyNull<Float32, Decimal128>(-12.213f, DecimalField128(-12213, 3), std::make_tuple(38, 3));
    testThrowException<Float32, Decimal128>(MAX_FLOAT32, std::make_tuple(38, 0));
    testNotOnlyNull<Float32, Decimal128>(MIN_FLOAT64, DecimalField128(0, 30), std::make_tuple(38, 30));

    testNotOnlyNull<Float32, Decimal256>(0, DecimalField256(static_cast<Int256>(0), 0), std::make_tuple(65, 0));
    testNotOnlyNull<Float32, Decimal256>(12.213f, DecimalField256(static_cast<Int256>(12213), 3), std::make_tuple(65, 3));
    testNotOnlyNull<Float32, Decimal256>(-12.213f, DecimalField256(static_cast<Int256>(-12213), 3), std::make_tuple(65, 3));
    // TODO add test after bug fixed
    // ERROR 1105 (HY000): other error for mpp stream: Cannot convert a non-finite number to an integer.
    // testNotOnlyNull<Float32, Decimal256>(MAX_FLOAT32, DecimalField256(Int256("340282346638528860000000000000000000000"), 0), std::make_tuple(65, 0));
    testNotOnlyNull<Float32, Decimal256>(MIN_FLOAT64, DecimalField256(static_cast<Int256>(0), 30), std::make_tuple(65, 30));

    testNotOnlyNull<Float64, Decimal32>(0, DecimalField32(0, 0), std::make_tuple(9, 0));
    testNotOnlyNull<Float64, Decimal32>(12.213, DecimalField32(12213, 3), std::make_tuple(9, 3));
    testNotOnlyNull<Float64, Decimal32>(-12.213, DecimalField32(-12213, 3), std::make_tuple(9, 3));
    testThrowException<Float64, Decimal32>(MAX_FLOAT64, std::make_tuple(9, 0));
    testNotOnlyNull<Float64, Decimal32>(MIN_FLOAT64, DecimalField32(0, 9), std::make_tuple(9, 9));

    testNotOnlyNull<Float64, Decimal64>(0, DecimalField64(0, 0), std::make_tuple(18, 0));
    testNotOnlyNull<Float64, Decimal64>(12.213, DecimalField64(12213, 3), std::make_tuple(18, 3));
    testNotOnlyNull<Float64, Decimal64>(-12.213, DecimalField64(-12213, 3), std::make_tuple(18, 3));
    testThrowException<Float64, Decimal64>(MAX_FLOAT64, std::make_tuple(18, 0));
    testNotOnlyNull<Float64, Decimal64>(MIN_FLOAT64, DecimalField64(0, 18), std::make_tuple(18, 18));

    testNotOnlyNull<Float64, Decimal128>(0, DecimalField128(0, 0), std::make_tuple(38, 0));
    testNotOnlyNull<Float64, Decimal128>(12.213, DecimalField128(12213, 3), std::make_tuple(38, 3));
    testNotOnlyNull<Float64, Decimal128>(-12.213, DecimalField128(-12213, 3), std::make_tuple(38, 3));
    testThrowException<Float64, Decimal128>(MAX_FLOAT64, std::make_tuple(38, 0));
    testNotOnlyNull<Float64, Decimal128>(MIN_FLOAT64, DecimalField128(0, 30), std::make_tuple(38, 30));

    testNotOnlyNull<Float64, Decimal256>(0, DecimalField256(static_cast<Int256>(0), 0), std::make_tuple(65, 0));
    testNotOnlyNull<Float64, Decimal256>(12.213, DecimalField256(static_cast<Int256>(12213), 3), std::make_tuple(65, 3));
    testNotOnlyNull<Float64, Decimal256>(-12.213, DecimalField256(static_cast<Int256>(-12213), 3), std::make_tuple(65, 3));
    testThrowException<Float64, Decimal256>(MAX_FLOAT64, std::make_tuple(65, 0));
    testNotOnlyNull<Float64, Decimal256>(MIN_FLOAT64, DecimalField256(static_cast<Int256>(0), 30), std::make_tuple(65, 30));


    // test round
    // TODO fix:
    // in default mode
    // for round test, tidb throw warnings: Truncated incorrect DECIMAL value: xxx
    // tiflash is same as mysql, don't throw warnings.
    DAGContext * dag_context = context.getDAGContext();
    UInt64 ori_flags = dag_context->getFlags();
    dag_context->addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    dag_context->clearWarnings();

    testNotOnlyNull<Float32, Decimal32>(12.213f, DecimalField32(1221, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float32, Decimal32>(-12.213f, DecimalField32(-1221, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float32, Decimal32>(12.215f, DecimalField32(1222, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float32, Decimal32>(-12.215f, DecimalField32(-1222, 2), std::make_tuple(9, 2));

    testNotOnlyNull<Float32, Decimal64>(12.213f, DecimalField64(1221, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float32, Decimal64>(-12.213f, DecimalField64(-1221, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float32, Decimal64>(12.215f, DecimalField64(1222, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float32, Decimal64>(-12.215f, DecimalField64(-1222, 2), std::make_tuple(18, 2));

    testNotOnlyNull<Float32, Decimal128>(12.213f, DecimalField128(1221, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float32, Decimal128>(-12.213f, DecimalField128(-1221, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float32, Decimal128>(12.215f, DecimalField128(1222, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float32, Decimal128>(-12.215f, DecimalField128(-1222, 2), std::make_tuple(38, 2));

    testNotOnlyNull<Float32, Decimal256>(12.213f, DecimalField256(static_cast<Int256>(1221), 2), std::make_tuple(65, 2));
    testNotOnlyNull<Float32, Decimal256>(-12.213f, DecimalField256(static_cast<Int256>(-1221), 2), std::make_tuple(65, 2));
    testNotOnlyNull<Float32, Decimal256>(12.215f, DecimalField256(static_cast<Int256>(1222), 2), std::make_tuple(65, 2));
    testNotOnlyNull<Float32, Decimal256>(-12.215f, DecimalField256(static_cast<Int256>(-1222), 2), std::make_tuple(65, 2));

    testNotOnlyNull<Float64, Decimal32>(12.213, DecimalField32(1221, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float64, Decimal32>(-12.213, DecimalField32(-1221, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float64, Decimal32>(12.215, DecimalField32(1222, 2), std::make_tuple(9, 2));
    testNotOnlyNull<Float64, Decimal32>(-12.215, DecimalField32(-1222, 2), std::make_tuple(9, 2));

    testNotOnlyNull<Float64, Decimal64>(12.213, DecimalField64(1221, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float64, Decimal64>(-12.213, DecimalField64(-1221, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float64, Decimal64>(12.215, DecimalField64(1222, 2), std::make_tuple(18, 2));
    testNotOnlyNull<Float64, Decimal64>(-12.215, DecimalField64(-1222, 2), std::make_tuple(18, 2));

    testNotOnlyNull<Float64, Decimal128>(12.213, DecimalField128(1221, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float64, Decimal128>(-12.213, DecimalField128(-1221, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float64, Decimal128>(12.215, DecimalField128(1222, 2), std::make_tuple(38, 2));
    testNotOnlyNull<Float64, Decimal128>(-12.215, DecimalField128(-1222, 2), std::make_tuple(38, 2));

    testNotOnlyNull<Float64, Decimal256>(12.213, DecimalField256(static_cast<Int256>(1221), 2), std::make_tuple(65, 2));
    testNotOnlyNull<Float64, Decimal256>(-12.213, DecimalField256(static_cast<Int256>(-1221), 2), std::make_tuple(65, 2));
    testNotOnlyNull<Float64, Decimal256>(12.215, DecimalField256(static_cast<Int256>(1222), 2), std::make_tuple(65, 2));
    testNotOnlyNull<Float64, Decimal256>(-12.215, DecimalField256(static_cast<Int256>(-1222), 2), std::make_tuple(65, 2));

    // Not compatible with MySQL/TiDB.
    // MySQL/TiDB: 34028199169636080000000000000000000000.00
    // TiFlash:    34028199169636079590747176440761942016.00
    testNotOnlyNull<Float32, Decimal256>(3.40282e+37f, DecimalField256(Decimal256(Int256("3402819916963607959074717644076194201600")), 2), std::make_tuple(50, 2));
    // MySQL/TiDB: 34028200000000000000000000000000000000.00
    // TiFlash:    34028200000000004441521809130870213181.44
    testNotOnlyNull<Float64, Decimal256>(3.40282e+37, DecimalField256(Decimal256(Int256("3402820000000000444152180913087021318144")), 2), std::make_tuple(50, 2));

    // MySQL/TiDB: 123.12345886230469000000
    // TiFlash:    123.12345886230470197248
    testNotOnlyNull<Float32, Decimal256>(123.123456789123456789f, DecimalField256(Decimal256(Int256("12312345886230470197248")), 20), std::make_tuple(50, 20));
    // MySQL/TiDB: 123.12345886230469000000
    // TiFlash:    123.12345678912344293376
    testNotOnlyNull<Float64, Decimal256>(123.123456789123456789, DecimalField256(Decimal256(Int256("12312345678912344293376")), 20), std::make_tuple(50, 20));

    dag_context->setFlags(ori_flags);
    dag_context->clearWarnings();
}
CATCH

TEST_F(TestTidbConversion, castRealAsTime)
try
{
    testOnlyNull<Float32, MyDateTime>();
    testOnlyNull<Float64, MyDateTime>();

    // TODO add tests after non-expected results fixed

    // mysql: null, warning.
    // tiflash: null, no warning.
    // tidb: 0000-00-00 00:00:00
    // testThrowException<Float32, MyDateTime>(0, 6);
    testThrowException<Float32, MyDateTime>(12.213, 6);
    testThrowException<Float32, MyDateTime>(-12.213, 6);
    testThrowException<Float32, MyDateTime>(MAX_FLOAT32, 6);
    testThrowException<Float32, MyDateTime>(MIN_FLOAT32, 6);
    // mysql: 2000-01-11 00:00:00
    // tiflash / tidb: null, warnings
    // testNotOnlyNull<Float32, MyDateTime>(111, {2000, 1, 11, 0, 0, 0, 0}, 6);
    testThrowException<Float32, MyDateTime>(-111, 6);
    // mysql: 2000-01-11 00:00:00
    // tiflash / tidb: null, warnings
    // testNotOnlyNull<Float32, MyDateTime>(111.1, {2000, 1, 11, 0, 0, 0, 0}, 6);

    // mysql: null, warning.
    // tiflash: null, no warning.
    // tidb: 0000-00-00 00:00:00
    // testThrowException<Float64, MyDateTime>(0, 6);
    testThrowException<Float64, MyDateTime>(12.213, 6);
    testThrowException<Float64, MyDateTime>(-12.213, 6);
    testThrowException<Float64, MyDateTime>(MAX_FLOAT64, 6);
    testThrowException<Float64, MyDateTime>(MIN_FLOAT64, 6);
    // mysql: 2000-01-11 00:00:00
    // tiflash / tidb: null, warnings
    // testNotOnlyNull<Float64, MyDateTime>(111, {2000, 1, 11, 0, 0, 0, 0}, 6);
    testThrowException<Float64, MyDateTime>(-111, 6);
    // mysql: 2000-01-11 00:00:00
    // tiflash / tidb: null, warnings
    // testNotOnlyNull<Float64, MyDateTime>(111.1, {2000, 1, 11, 0, 0, 0, 0}, 6);
    testNotOnlyNull<Float64, MyDateTime>(20210201, {2021, 2, 1, 0, 0, 0, 0}, 6);
    // mysql: 2021-02-01 00:00:00
    // tiflash / tidb: 2021-02-01 01:00:00
    // testNotOnlyNull<Float64, MyDateTime>(20210201.1, {2021, 2, 1, 0, 0, 0, 0}, 6);
}
CATCH

TEST_F(TestTidbConversion, castTimeAsReal)
try
{
    const auto data_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    const Float64 datetime_float = 20211026160859;
    const Float64 datetime_frac_float = 20211026160859.125;
>>>>>>> 68906edbaa (Fix wrong result of cast(float as decimal) when overflow happens (#4380))

    // cast datetime to float
    auto col_datetime1 = getDatetimeColumn();
    auto ctn_datetime1 = ColumnWithTypeAndName(std::move(col_datetime1), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Float64>({datetime_float, datetime_frac_float}),
        executeFunction(func_name,
                        {ctn_datetime1,
                         createConstColumn<String>(1, "Float64")}));

    // cast datetime to nullable float
    auto col_datetime2 = getDatetimeColumn();
    auto ctn_datetime2 = ColumnWithTypeAndName(std::move(col_datetime2), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({datetime_float, datetime_frac_float}),
        executeFunction(func_name,
                        {ctn_datetime2,
                         createConstColumn<String>(1, "Nullable(Float64)")}));

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
                         createConstColumn<String>(1, "Nullable(Float64)")}));

    // cast const datetime to float
    auto col_datetime4_const = ColumnConst::create(getDatetimeColumn(true), 1);
    auto ctn_datetime4_const = ColumnWithTypeAndName(std::move(col_datetime4_const), data_type_ptr, "datetime");
    ASSERT_COLUMN_EQ(
        createConstColumn<Float64>(1, datetime_float),
        executeFunction(func_name,
                        {ctn_datetime4_const,
                         createConstColumn<String>(1, "Float64")}));

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
                         createConstColumn<String>(1, "Nullable(Float64)")}));
}
CATCH

TEST_F(TestTidbConversion, castDurationAsDuration)
try
{
    static const std::string func_name = "tidb_cast";
    static const auto from_type = std::make_shared<DataTypeMyDuration>(3);
    static const auto to_type_1 = std::make_shared<DataTypeMyDuration>(5); // from_fsp <  to_fsp
    static const auto to_type_2 = std::make_shared<DataTypeMyDuration>(3); // from_fsp == to_fsp
    static const auto to_type_3 = std::make_shared<DataTypeMyDuration>(2); // from_fsp <  to_fsp

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

    ASSERT_COLUMN_EQ(output1, executeFunction(func_name, {input, createConstColumn<String>(1, to_type_1->getName())}));
    ASSERT_COLUMN_EQ(output2, executeFunction(func_name, {input, createConstColumn<String>(1, to_type_2->getName())}));
    ASSERT_COLUMN_EQ(output3, executeFunction(func_name, {input, createConstColumn<String>(1, to_type_3->getName())}));

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

    ASSERT_COLUMN_EQ(output1_nullable, executeFunction(func_name, {input_nullable, createConstColumn<String>(1, makeNullable(to_type_1)->getName())}));
    ASSERT_COLUMN_EQ(output2_nullable, executeFunction(func_name, {input_nullable, createConstColumn<String>(1, makeNullable(to_type_2)->getName())}));
    ASSERT_COLUMN_EQ(output3_nullable, executeFunction(func_name, {input_nullable, createConstColumn<String>(1, makeNullable(to_type_3)->getName())}));

    // Test Const
    ColumnWithTypeAndName input_const(createConstColumn<DataTypeMyDuration::FieldType>(1, (20 * 3600 + 20 * 60 + 20) * 1000000000L + 999000000L).column, from_type, "input_const");
    ColumnWithTypeAndName output1_const(input_const.column, to_type_1, "output1_const");
    ColumnWithTypeAndName output2_const(input_const.column, to_type_2, "output2_const");
    ColumnWithTypeAndName output3_const(createConstColumn<DataTypeMyDuration::FieldType>(1, (20 * 3600 + 20 * 60 + 21) * 1000000000L + 000000000L).column, to_type_3, "output3_const");

    ASSERT_COLUMN_EQ(output1_const, executeFunction(func_name, {input_const, createConstColumn<String>(1, to_type_1->getName())}));
    ASSERT_COLUMN_EQ(output2_const, executeFunction(func_name, {input_const, createConstColumn<String>(1, to_type_2->getName())}));
    ASSERT_COLUMN_EQ(output3_const, executeFunction(func_name, {input_const, createConstColumn<String>(1, to_type_3->getName())}));
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

} // namespace tests
} // namespace DB
