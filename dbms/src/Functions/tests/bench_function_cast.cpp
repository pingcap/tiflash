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

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <benchmark/benchmark.h>
#include <boost_wrapper/cpp_int.h>

#include <boost/random.hpp>
#include <random>
#include <vector>

using namespace boost::multiprecision;
using namespace boost::random;

namespace DB
{
namespace tests
{
class CastToDecimalBench : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) override
    {
        initData();
        try
        {
            DB::registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    }
    void initData()
    {
        DataTypePtr from_type_int8 = std::make_shared<DataTypeInt8>();
        DataTypePtr from_type_int16 = std::make_shared<DataTypeInt16>();
        DataTypePtr from_type_int32 = std::make_shared<DataTypeInt32>();
        DataTypePtr from_type_int64 = std::make_shared<DataTypeInt64>();
        DataTypePtr from_type_int64_small = std::make_shared<DataTypeInt64>();
        DataTypePtr from_type_uint8 = std::make_shared<DataTypeUInt8>();
        DataTypePtr from_type_uint16 = std::make_shared<DataTypeUInt16>();
        DataTypePtr from_type_uint32 = std::make_shared<DataTypeUInt32>();
        DataTypePtr from_type_uint64 = std::make_shared<DataTypeUInt64>();
        DataTypePtr from_type_dec_2_1 = std::make_shared<DataTypeDecimal32>(2, 1);
        DataTypePtr from_type_dec_2_1_small = std::make_shared<DataTypeDecimal32>(2, 1);
        DataTypePtr from_type_dec_3_0 = std::make_shared<DataTypeDecimal32>(3, 0);
        DataTypePtr from_type_dec_10_3 = std::make_shared<DataTypeDecimal64>(10, 3);
        DataTypePtr from_type_dec_12_3 = std::make_shared<DataTypeDecimal64>(12, 3);
        DataTypePtr from_type_dec_25_3 = std::make_shared<DataTypeDecimal128>(25, 3);
        DataTypePtr from_type_dec_35_0 = std::make_shared<DataTypeDecimal128>(35, 0);
        DataTypePtr from_type_dec_40_0 = std::make_shared<DataTypeDecimal128>(40, 0);
        DataTypePtr from_type_dec_40_5 = std::make_shared<DataTypeDecimal128>(40, 5);
        DataTypePtr from_type_dec_57_0 = std::make_shared<DataTypeDecimal256>(57, 0);
        DataTypePtr from_type_dec_60_0 = std::make_shared<DataTypeDecimal256>(60, 0);
        DataTypePtr from_type_dec_60_5 = std::make_shared<DataTypeDecimal256>(60, 5);
        DataTypePtr from_type_date = std::make_shared<DataTypeMyDate>();
        DataTypePtr from_type_datetime_fsp5 = std::make_shared<DataTypeMyDateTime>(5);
        DataTypePtr from_type_float32 = std::make_shared<DataTypeFloat32>();
        DataTypePtr from_type_float64 = std::make_shared<DataTypeFloat64>();

        auto tmp_col_int8 = from_type_int8->createColumn();
        auto tmp_col_int16 = from_type_int16->createColumn();
        auto tmp_col_int32 = from_type_int32->createColumn();
        auto tmp_col_int64 = from_type_int64->createColumn();
        auto tmp_col_int64_small = from_type_int64_small->createColumn();
        auto tmp_col_uint8 = from_type_uint8->createColumn();
        auto tmp_col_uint16 = from_type_uint16->createColumn();
        auto tmp_col_uint32 = from_type_uint32->createColumn();
        auto tmp_col_uint64 = from_type_uint64->createColumn();
        auto tmp_col_dec_2_1 = from_type_dec_2_1->createColumn();
        auto tmp_col_dec_2_1_small = from_type_dec_2_1_small->createColumn();
        auto tmp_col_dec_3_0 = from_type_dec_3_0->createColumn();
        auto tmp_col_dec_10_3 = from_type_dec_10_3->createColumn();
        auto tmp_col_dec_12_3 = from_type_dec_12_3->createColumn();
        auto tmp_col_dec_25_3 = from_type_dec_25_3->createColumn();
        auto tmp_col_dec_35_0 = from_type_dec_35_0->createColumn();
        auto tmp_col_dec_40_0 = from_type_dec_40_0->createColumn();
        auto tmp_col_dec_40_5 = from_type_dec_40_5->createColumn();
        auto tmp_col_dec_57_0 = from_type_dec_57_0->createColumn();
        auto tmp_col_dec_60_0 = from_type_dec_60_0->createColumn();
        auto tmp_col_dec_60_5 = from_type_dec_60_5->createColumn();
        auto tmp_col_date = from_type_date->createColumn();
        auto tmp_col_datetime_fsp5 = from_type_date->createColumn();
        auto tmp_col_float32 = ColumnFloat32::create();
        auto tmp_col_float64 = ColumnFloat64::create();

        std::uniform_int_distribution<int64_t> dist64(
            std::numeric_limits<int64_t>::min(),
            std::numeric_limits<int64_t>::max());

        mt19937 mt;
        uniform_int_distribution<cpp_int> dist256(-(cpp_int(1) << 256), cpp_int(1) << 256);
        const Int256 mod_prec_10 = getScaleMultiplier<Decimal256>(10);
        const Int256 mod_prec_12 = getScaleMultiplier<Decimal256>(12);
        const Int256 mod_prec_25 = getScaleMultiplier<Decimal256>(25);
        const Int256 mod_prec_35 = getScaleMultiplier<Decimal256>(35);
        const Int256 mod_prec_40 = getScaleMultiplier<Decimal256>(40);
        const Int256 mod_prec_57 = getScaleMultiplier<Decimal256>(57);
        const Int256 mod_prec_60 = getScaleMultiplier<Decimal256>(60);

        for (int i = 0; i < row_num; ++i)
        {
            tmp_col_int8->insert(Field(static_cast<Int64>(static_cast<Int8>(dist64(mt)))));
            tmp_col_int16->insert(Field(static_cast<Int64>(static_cast<Int16>(dist64(mt)))));
            tmp_col_int32->insert(Field(static_cast<Int64>(static_cast<Int32>(dist64(mt)))));
            tmp_col_int64->insert(Field(static_cast<Int64>(static_cast<Int64>(dist64(mt)))));
            tmp_col_int64_small->insert(Field(static_cast<Int64>(static_cast<Int64>(dist64(mt) % mod_prec_10))));

            tmp_col_uint8->insert(Field(static_cast<Int64>(static_cast<UInt8>(dist64(mt)))));
            tmp_col_uint16->insert(Field(static_cast<Int64>(static_cast<UInt16>(dist64(mt)))));
            tmp_col_uint32->insert(Field(static_cast<Int64>(static_cast<UInt16>(dist64(mt)))));
            tmp_col_uint64->insert(Field(static_cast<Int64>(static_cast<UInt16>(dist64(mt)))));
            tmp_col_float32->insert(static_cast<Float32>(dist64(mt)));
            tmp_col_float64->insert(static_cast<Float64>(dist64(mt)));

            tmp_col_dec_2_1->insert(DecimalField(Decimal(static_cast<Int32>(dist64(mt) % 100)), 1));
            tmp_col_dec_2_1_small->insert(DecimalField(Decimal(static_cast<Int32>(dist64(mt) % 10)), 1));
            tmp_col_dec_3_0->insert(DecimalField(Decimal(static_cast<Int32>(dist64(mt) % 1000)), 0));
            tmp_col_dec_10_3->insert(DecimalField(Decimal(static_cast<Int64>(dist64(mt) % mod_prec_10)), 3));
            tmp_col_dec_12_3->insert(DecimalField(Decimal(static_cast<Int64>(dist64(mt) % mod_prec_12)), 3));
            tmp_col_dec_25_3->insert(DecimalField(Decimal(static_cast<Int128>(dist256(mt) % mod_prec_25)), 3));
            tmp_col_dec_35_0->insert(DecimalField(Decimal(static_cast<Int128>(dist256(mt) % mod_prec_35)), 0));
            tmp_col_dec_40_0->insert(DecimalField(Decimal(static_cast<Int256>(dist256(mt) % mod_prec_40)), 0));
            tmp_col_dec_40_5->insert(DecimalField(Decimal(static_cast<Int256>(dist256(mt) % mod_prec_40)), 5));
            tmp_col_dec_57_0->insert(DecimalField(Decimal(static_cast<Int256>(dist256(mt) % mod_prec_57)), 0));
            tmp_col_dec_60_0->insert(DecimalField(Decimal(static_cast<Int256>(dist256(mt) % mod_prec_60)), 0));
            tmp_col_dec_60_5->insert(DecimalField(Decimal(static_cast<Int256>(dist256(mt) % mod_prec_60)), 5));
            tmp_col_date->insert(Field(static_cast<UInt64>(MyDate(2020, 10, 10).toPackedUInt())));
            tmp_col_datetime_fsp5->insert(
                Field(static_cast<UInt64>(MyDateTime(2020, 1, 10, 0, 0, 0, 0).toPackedUInt())));
        }
        from_col_int8 = ColumnWithTypeAndName(std::move(tmp_col_int8), from_type_int8, "from_col_int8");
        from_col_int16 = ColumnWithTypeAndName(std::move(tmp_col_int16), from_type_int16, "from_col_int16");
        from_col_int32 = ColumnWithTypeAndName(std::move(tmp_col_int32), from_type_int32, "from_col_int32");
        from_col_int64 = ColumnWithTypeAndName(std::move(tmp_col_int64), from_type_int64, "from_col_int64");
        from_col_int64_small
            = ColumnWithTypeAndName(std::move(tmp_col_int64_small), from_type_int64_small, "from_col_int64_small");
        from_col_uint8 = ColumnWithTypeAndName(std::move(tmp_col_uint8), from_type_uint8, "from_col_uint8");
        from_col_uint16 = ColumnWithTypeAndName(std::move(tmp_col_uint16), from_type_uint16, "from_col_uint16");
        from_col_uint32 = ColumnWithTypeAndName(std::move(tmp_col_uint32), from_type_uint32, "from_col_uint32");
        from_col_uint64 = ColumnWithTypeAndName(std::move(tmp_col_uint64), from_type_uint64, "from_col_uint64");
        from_col_float32 = ColumnWithTypeAndName(std::move(tmp_col_float32), from_type_float32, "from_col_float32");
        from_col_float64 = ColumnWithTypeAndName(std::move(tmp_col_float64), from_type_float64, "from_col_float64");

        from_col_dec_2_1 = ColumnWithTypeAndName(std::move(tmp_col_dec_2_1), from_type_dec_2_1, "from_col_dec_2_1");
        from_col_dec_2_1_small = ColumnWithTypeAndName(
            std::move(tmp_col_dec_2_1_small),
            from_type_dec_2_1_small,
            "from_col_dec_2_1_small");
        from_col_dec_3_0 = ColumnWithTypeAndName(std::move(tmp_col_dec_3_0), from_type_dec_3_0, "from_col_dec_3_0");
        from_col_dec_10_3 = ColumnWithTypeAndName(std::move(tmp_col_dec_10_3), from_type_dec_10_3, "from_col_dec_10_3");
        from_col_dec_12_3 = ColumnWithTypeAndName(std::move(tmp_col_dec_12_3), from_type_dec_12_3, "from_col_dec_12_3");
        from_col_dec_25_3 = ColumnWithTypeAndName(std::move(tmp_col_dec_25_3), from_type_dec_25_3, "from_col_dec_25_3");
        from_col_dec_35_0 = ColumnWithTypeAndName(std::move(tmp_col_dec_35_0), from_type_dec_35_0, "from_col_dec_35_0");
        from_col_dec_40_0 = ColumnWithTypeAndName(std::move(tmp_col_dec_40_0), from_type_dec_40_0, "from_col_dec_40_0");
        from_col_dec_40_5 = ColumnWithTypeAndName(std::move(tmp_col_dec_40_5), from_type_dec_40_5, "from_col_dec_40_5");
        from_col_dec_57_0 = ColumnWithTypeAndName(std::move(tmp_col_dec_57_0), from_type_dec_57_0, "from_col_dec_57_0");
        from_col_dec_60_0 = ColumnWithTypeAndName(std::move(tmp_col_dec_60_0), from_type_dec_60_0, "from_col_dec_60_0");
        from_col_dec_60_5 = ColumnWithTypeAndName(std::move(tmp_col_dec_60_5), from_type_dec_60_5, "from_col_dec_60_5");
        from_col_date = ColumnWithTypeAndName(std::move(tmp_col_date), from_type_date, "from_col_date");
        from_col_datetime_fsp5 = ColumnWithTypeAndName(
            std::move(tmp_col_datetime_fsp5),
            from_type_datetime_fsp5,
            "from_col_datetime_fsp5");

        DataTypePtr dest_type_dec_2_1 = createDecimal(2, 1);
        DataTypePtr dest_type_dec_2_2 = createDecimal(2, 2);
        DataTypePtr dest_type_dec_15_4 = createDecimal(15, 4);
        DataTypePtr dest_type_dec_25_4 = createDecimal(25, 4);
        DataTypePtr dest_type_dec_31_30 = createDecimal(31, 30);
        DataTypePtr dest_type_dec_60_5 = createDecimal(60, 5);
        DataTypePtr dest_type_dec_65_30 = createDecimal(65, 30);
        DataTypePtr dest_type_dec_8_0 = createDecimal(8, 0);
        DataTypePtr dest_type_dec_10_0 = createDecimal(10, 0);
        DataTypePtr dest_type_dec_10_5 = createDecimal(10, 5);
        DataTypePtr dest_type_dec_10_8 = createDecimal(10, 8);
        DataTypePtr dest_type_dec_20_0 = createDecimal(20, 0);
        DataTypePtr dest_type_dec_30_0 = createDecimal(30, 0);
        DataTypePtr dest_type_dec_40_0 = createDecimal(40, 0);
        DataTypePtr dest_type_dec_40_5 = createDecimal(40, 5);
        DataTypePtr dest_type_dec_60_0 = createDecimal(60, 0);
        DataTypePtr dest_type_dec_60_4 = createDecimal(60, 4);
        DataTypePtr dest_type_dec_60_30 = createDecimal(60, 30);

        dest_col_dec_2_1 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_2_1->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_2_2 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_2_2->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_15_4 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_15_4->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_25_4 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_25_4->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_31_30 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_31_30->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_60_5 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_60_5->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_65_30 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_65_30->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_8_0 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_8_0->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_10_0 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_10_0->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_10_5 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_10_5->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_10_8 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_10_8->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_20_0 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_20_0->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_30_0 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_30_0->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_40_0 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_40_0->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_40_5 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_40_5->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_60_0 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_60_0->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_60_4 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_60_4->getName()),
            std::make_shared<DataTypeString>(),
            "");
        dest_col_dec_60_30 = ColumnWithTypeAndName(
            DataTypeString().createColumnConst(row_num, dest_type_dec_60_30->getName()),
            std::make_shared<DataTypeString>(),
            "");

        // For test performance of casting Int64/Int128 to Int256.
        from_int64_vec = std::vector<Int64>(row_num);
        from_int128_vec = std::vector<Int128>(row_num);
        from_int256_vec = std::vector<Int256>(row_num);
        from_float32_vec = std::vector<Float32>(row_num);
        from_float64_vec = std::vector<Float64>(row_num);
        dest_int64_vec = std::vector<Int64>(row_num);
        dest_int128_vec = std::vector<Int128>(row_num);
        dest_int256_vec = std::vector<Int256>(row_num);
        dest_float32_vec = std::vector<Float32>(row_num);
        dest_float64_vec = std::vector<Float64>(row_num);
        const Int256 mod_prec_19 = getScaleMultiplier<Decimal256>(19);
        const Int256 mod_prec_38 = getScaleMultiplier<Decimal256>(38);
        for (auto i = 0; i < row_num; ++i)
        {
            from_int64_vec[i] = dist64(mt);
            from_int128_vec[i] = static_cast<Int128>(dist256(mt) % (std::numeric_limits<Int128>::max() % mod_prec_19));
            from_int256_vec[i] = static_cast<Int256>(dist256(mt) % (std::numeric_limits<Int256>::max()) % mod_prec_38);
            from_float32_vec[i] = static_cast<Float32>(from_int64_vec[i]);
            from_float64_vec[i] = static_cast<Float64>(from_int64_vec[i]);
        }
    }

    const int row_num = 2000;

    ColumnWithTypeAndName from_col_int8;
    ColumnWithTypeAndName from_col_int16;
    ColumnWithTypeAndName from_col_int32;
    ColumnWithTypeAndName from_col_int64;
    ColumnWithTypeAndName from_col_int64_small;
    ColumnWithTypeAndName from_col_uint8;
    ColumnWithTypeAndName from_col_uint16;
    ColumnWithTypeAndName from_col_uint32;
    ColumnWithTypeAndName from_col_uint64;
    ColumnWithTypeAndName from_col_float32;
    ColumnWithTypeAndName from_col_float64;
    ColumnWithTypeAndName from_col_dec_2_1;
    ColumnWithTypeAndName from_col_dec_2_1_small;
    ColumnWithTypeAndName from_col_dec_3_0;
    ColumnWithTypeAndName from_col_dec_10_3;
    ColumnWithTypeAndName from_col_dec_12_3;
    ColumnWithTypeAndName from_col_dec_25_3;
    ColumnWithTypeAndName from_col_dec_35_0;
    ColumnWithTypeAndName from_col_dec_40_0;
    ColumnWithTypeAndName from_col_dec_40_5;
    ColumnWithTypeAndName from_col_dec_57_0;
    ColumnWithTypeAndName from_col_dec_60_0;
    ColumnWithTypeAndName from_col_dec_60_5;

    ColumnWithTypeAndName from_col_date;
    ColumnWithTypeAndName from_col_datetime_fsp5;

    // second arg is a const string describing dest type.
    ColumnWithTypeAndName dest_col_dec_2_1;
    ColumnWithTypeAndName dest_col_dec_2_2;
    ColumnWithTypeAndName dest_col_dec_15_4;
    ColumnWithTypeAndName dest_col_dec_25_4;
    ColumnWithTypeAndName dest_col_dec_31_30;
    ColumnWithTypeAndName dest_col_dec_60_5;
    ColumnWithTypeAndName dest_col_dec_65_30;

    ColumnWithTypeAndName dest_col_dec_8_0;
    ColumnWithTypeAndName dest_col_dec_10_0;
    ColumnWithTypeAndName dest_col_dec_10_5;
    ColumnWithTypeAndName dest_col_dec_10_8;
    ColumnWithTypeAndName dest_col_dec_20_0;
    ColumnWithTypeAndName dest_col_dec_30_0;
    ColumnWithTypeAndName dest_col_dec_40_0;
    ColumnWithTypeAndName dest_col_dec_40_5;
    ColumnWithTypeAndName dest_col_dec_60_0;
    ColumnWithTypeAndName dest_col_dec_60_4;
    ColumnWithTypeAndName dest_col_dec_60_30;

    std::vector<Int64> from_int64_vec;
    std::vector<Int128> from_int128_vec;
    std::vector<Int256> from_int256_vec;
    std::vector<Float32> from_float32_vec;
    std::vector<Float64> from_float64_vec;
    std::vector<Int64> dest_int64_vec;
    std::vector<Int128> dest_int128_vec;
    std::vector<Int256> dest_int256_vec;
    std::vector<Float32> dest_float32_vec;
    std::vector<Float64> dest_float64_vec;
};

#define CAST_BENCHMARK(CLASS_NAME, CASE_NAME, FROM_COL, DEST_TYPE)     \
    BENCHMARK_DEFINE_F(CLASS_NAME, CASE_NAME)                          \
    (benchmark::State & state)                                         \
    try                                                                \
    {                                                                  \
        const String func_name = "tidb_cast";                          \
        auto context = DB::tests::TiFlashTestEnv::getContext();        \
        auto dag_context_ptr = std::make_unique<DAGContext>(1024);     \
        UInt64 ori_flags = dag_context_ptr->getFlags();                \
        dag_context_ptr->addFlag(TiDBSQLFlags::OVERFLOW_AS_WARNING);   \
        dag_context_ptr->addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);   \
        dag_context_ptr->clearWarnings();                              \
        context->setDAGContext(dag_context_ptr.get());                 \
        for (auto _ : state)                                           \
        {                                                              \
            executeFunction(*context, func_name, FROM_COL, DEST_TYPE); \
        }                                                              \
        dag_context_ptr->setFlags(ori_flags);                          \
    }                                                                  \
    CATCH                                                              \
    BENCHMARK_REGISTER_F(CLASS_NAME, CASE_NAME)->Iterations(1000);

// NOTE: There are three factors that will affects performance:
// 1. Need overflow check or not. The cost of handleOverflowError() is huge.
// 2. Which CastInternalType used. Because the cost of Int256 * Int256 is huge.
// 3. Does the type of return value is same with CastInternalType. Because there will be a static_cast if they are not same.
// 4. div.

// cast(int as decimal)
// no; Int32; Int32
CAST_BENCHMARK(CastToDecimalBench, int8_to_decimal_8_0, from_col_int8, dest_col_dec_8_0);
// no; Int32; Int64
CAST_BENCHMARK(CastToDecimalBench, int8_to_decimal_10_0, from_col_int8, dest_col_dec_10_0);
// no; Int32; Int128
CAST_BENCHMARK(CastToDecimalBench, int8_to_decimal_30_0, from_col_int8, dest_col_dec_30_0);
// no; Int32; Int256
CAST_BENCHMARK(CastToDecimalBench, int8_to_decimal_60_0, from_col_int8, dest_col_dec_60_0);
// no; Int32; Int256
CAST_BENCHMARK(CastToDecimalBench, int8_to_decimal_60_4, from_col_int8, dest_col_dec_60_4);

// no; Int32; Int32
CAST_BENCHMARK(CastToDecimalBench, int16_to_decimal_8_0, from_col_int16, dest_col_dec_8_0);
// no; Int32; Int64
CAST_BENCHMARK(CastToDecimalBench, int16_to_decimal_10_0, from_col_int16, dest_col_dec_10_0);
// no; Int32; Int128
CAST_BENCHMARK(CastToDecimalBench, int16_to_decimal_30_0, from_col_int16, dest_col_dec_30_0);
// no; Int32; Int256
CAST_BENCHMARK(CastToDecimalBench, int16_to_decimal_60_0, from_col_int16, dest_col_dec_60_0);
// no; Int32; Int256
CAST_BENCHMARK(CastToDecimalBench, int16_to_decimal_60_4, from_col_int16, dest_col_dec_60_4);

// need; Int64; Int32
CAST_BENCHMARK(CastToDecimalBench, int32_to_decimal_8_0, from_col_int32, dest_col_dec_8_0);
// no; Int64; Int64
CAST_BENCHMARK(CastToDecimalBench, int32_to_decimal_10_0, from_col_int32, dest_col_dec_10_0);
// no; Int64; Int128
CAST_BENCHMARK(CastToDecimalBench, int32_to_decimal_30_0, from_col_int32, dest_col_dec_30_0);
// no; Int64; Int256
CAST_BENCHMARK(CastToDecimalBench, int32_to_decimal_60_0, from_col_int32, dest_col_dec_60_0);
// no; Int64; Int256
CAST_BENCHMARK(CastToDecimalBench, int32_to_decimal_60_4, from_col_int32, dest_col_dec_60_4);

CAST_BENCHMARK(CastToDecimalBench, float32_to_decimal_60_30, from_col_float32, dest_col_dec_60_30);
CAST_BENCHMARK(CastToDecimalBench, float64_to_decimal_60_30, from_col_float64, dest_col_dec_60_30);

// need; Int128; Int32
CAST_BENCHMARK(CastToDecimalBench, int64_to_decimal_8_0, from_col_int64, dest_col_dec_8_0);
// need; Int128; Int64
CAST_BENCHMARK(CastToDecimalBench, int64_to_decimal_10_0, from_col_int64, dest_col_dec_10_0);
// no; Int128; Int128
CAST_BENCHMARK(CastToDecimalBench, int64_to_decimal_30_0, from_col_int64, dest_col_dec_30_0);
// no; Int128; Int256
CAST_BENCHMARK(CastToDecimalBench, int64_to_decimal_60_0, from_col_int64, dest_col_dec_60_0);
// no; Int128; Int256
CAST_BENCHMARK(CastToDecimalBench, int64_to_decimal_60_4, from_col_int64, dest_col_dec_60_4);

// need; Int128; Int64; small value, so no overflow really hanppens
CAST_BENCHMARK(CastToDecimalBench, int64_small_to_decimal_10_0, from_col_int64_small, dest_col_dec_10_0);

// cast(decimal as decimal)
// no; Int64; Int64
CAST_BENCHMARK(CastToDecimalBench, decimal_3_0_to_decimal_10_0, from_col_dec_3_0, dest_col_dec_10_0);
// no; Int64; Int64
CAST_BENCHMARK(CastToDecimalBench, decimal_3_0_to_decimal_10_5, from_col_dec_3_0, dest_col_dec_10_5);
// need; Int64; Int64
CAST_BENCHMARK(CastToDecimalBench, decimal_3_0_to_decimal_10_8, from_col_dec_3_0, dest_col_dec_10_8);

// no; Int64; Int128
CAST_BENCHMARK(CastToDecimalBench, decimal_3_0_to_decimal_30_0, from_col_dec_3_0, dest_col_dec_30_0);
// need; Int64; Int128

// no; Int64; Int256
CAST_BENCHMARK(CastToDecimalBench, decimal_3_0_to_decimal_60_0, from_col_dec_3_0, dest_col_dec_60_0);
// need; Int64; Int256

// no; Int128; Int256
CAST_BENCHMARK(CastToDecimalBench, decimal_35_0_to_decimal_60_0, from_col_dec_35_0, dest_col_dec_60_0);
// need; Int128; Int256
CAST_BENCHMARK(CastToDecimalBench, decimal_35_0_to_decimal_60_30, from_col_dec_35_0, dest_col_dec_60_30);

// need; Int256; Int128
CAST_BENCHMARK(CastToDecimalBench, decimal_60_0_to_decimal_30_0, from_col_dec_60_0, dest_col_dec_30_0);

// no; Int128; Int128
CAST_BENCHMARK(CastToDecimalBench, decimal_35_0_to_decimal_40_0, from_col_dec_35_0, dest_col_dec_40_0);
// no; Int256; Int256
CAST_BENCHMARK(CastToDecimalBench, decimal_40_0_to_decimal_60_0, from_col_dec_40_0, dest_col_dec_60_0);

// no; Int128; Int128. Int128 div
CAST_BENCHMARK(CastToDecimalBench, decimal_25_3_to_decimal_30_0, from_col_dec_25_3, dest_col_dec_30_0);
// no; Int128; Int256. Int256 div
CAST_BENCHMARK(CastToDecimalBench, decimal_40_5_to_decimal_60_0, from_col_dec_40_5, dest_col_dec_60_0);

// cast(date as decimal)
CAST_BENCHMARK(CastToDecimalBench, date_to_decimal_10_0, from_col_date, dest_col_dec_10_0);
CAST_BENCHMARK(CastToDecimalBench, datetime_fsp5_to_decimal_10_0, from_col_datetime_fsp5, dest_col_dec_10_0);
CAST_BENCHMARK(CastToDecimalBench, datetime_fsp5_to_decimal_10_8, from_col_datetime_fsp5, dest_col_dec_10_8);
CAST_BENCHMARK(CastToDecimalBench, datetime_fsp5_to_decimal_40_0, from_col_datetime_fsp5, dest_col_dec_40_0);
CAST_BENCHMARK(CastToDecimalBench, datetime_fsp5_to_decimal_40_5, from_col_datetime_fsp5, dest_col_dec_40_5);

#define STATIC_CAST_BENCHMARK(CLASS_NAME, FROM_TYPE, TO_TYPE)                                         \
    BENCHMARK_DEFINE_F(CastToDecimalBench, Int##FROM_TYPE##_static_cast_Int##TO_TYPE)                 \
    (benchmark::State & state)                                                                        \
    try                                                                                               \
    {                                                                                                 \
        for (auto _ : state)                                                                          \
        {                                                                                             \
            for (int i = 0; i < row_num; ++i)                                                         \
            {                                                                                         \
                dest_int##TO_TYPE##_vec[i] = static_cast<Int##TO_TYPE>(from_int##FROM_TYPE##_vec[i]); \
            }                                                                                         \
        }                                                                                             \
    }                                                                                                 \
    CATCH                                                                                             \
    BENCHMARK_REGISTER_F(CastToDecimalBench, Int##FROM_TYPE##_static_cast_Int##TO_TYPE)->Iterations(1000);

STATIC_CAST_BENCHMARK(CastToDecimalBench, 64, 128);
STATIC_CAST_BENCHMARK(CastToDecimalBench, 64, 256);
STATIC_CAST_BENCHMARK(CastToDecimalBench, 128, 128);
STATIC_CAST_BENCHMARK(CastToDecimalBench, 128, 256);

#define DIV_BENCHMARK(CLASS_NAME, TYPE)                                             \
    BENCHMARK_DEFINE_F(CastToDecimalBench, div_##TYPE)                              \
    (benchmark::State & state)                                                      \
    try                                                                             \
    {                                                                               \
        for (auto _ : state)                                                        \
        {                                                                           \
            for (int i = 0; i < row_num; ++i)                                       \
            {                                                                       \
                dest_##TYPE##_vec[i] = from_##TYPE##_vec[i] / from_##TYPE##_vec[0]; \
            }                                                                       \
        }                                                                           \
    }                                                                               \
    CATCH                                                                           \
    BENCHMARK_REGISTER_F(CastToDecimalBench, div_##TYPE)->Iterations(1000);

DIV_BENCHMARK(CastToDecimalBench, int64);
DIV_BENCHMARK(CastToDecimalBench, int128);
DIV_BENCHMARK(CastToDecimalBench, int256);
DIV_BENCHMARK(CastToDecimalBench, float32);
DIV_BENCHMARK(CastToDecimalBench, float64);

#define MUL_BENCHMARK(CLASS_NAME, TYPE)                                             \
    BENCHMARK_DEFINE_F(CastToDecimalBench, mul_##TYPE)                              \
    (benchmark::State & state)                                                      \
    try                                                                             \
    {                                                                               \
        for (auto _ : state)                                                        \
        {                                                                           \
            for (int i = 0; i < row_num; ++i)                                       \
            {                                                                       \
                dest_##TYPE##_vec[i] = from_##TYPE##_vec[i] * from_##TYPE##_vec[0]; \
            }                                                                       \
        }                                                                           \
    }                                                                               \
    CATCH                                                                           \
    BENCHMARK_REGISTER_F(CastToDecimalBench, mul_##TYPE)->Iterations(1000);

MUL_BENCHMARK(CastToDecimalBench, int64);
MUL_BENCHMARK(CastToDecimalBench, int128);
MUL_BENCHMARK(CastToDecimalBench, int256);
MUL_BENCHMARK(CastToDecimalBench, float32);
MUL_BENCHMARK(CastToDecimalBench, float64);

#define ADD_BENCHMARK(CLASS_NAME, TYPE)                                             \
    BENCHMARK_DEFINE_F(CastToDecimalBench, add_##TYPE)                              \
    (benchmark::State & state)                                                      \
    try                                                                             \
    {                                                                               \
        for (auto _ : state)                                                        \
        {                                                                           \
            for (int i = 0; i < row_num; ++i)                                       \
            {                                                                       \
                dest_##TYPE##_vec[i] = from_##TYPE##_vec[i] + from_##TYPE##_vec[0]; \
            }                                                                       \
        }                                                                           \
    }                                                                               \
    CATCH                                                                           \
    BENCHMARK_REGISTER_F(CastToDecimalBench, add_##TYPE)->Iterations(1000);

ADD_BENCHMARK(CastToDecimalBench, int64);
ADD_BENCHMARK(CastToDecimalBench, int128);
ADD_BENCHMARK(CastToDecimalBench, int256);
ADD_BENCHMARK(CastToDecimalBench, float32);
ADD_BENCHMARK(CastToDecimalBench, float64);

#define SUB_BENCHMARK(CLASS_NAME, TYPE)                                             \
    BENCHMARK_DEFINE_F(CastToDecimalBench, sub_##TYPE)                              \
    (benchmark::State & state)                                                      \
    try                                                                             \
    {                                                                               \
        for (auto _ : state)                                                        \
        {                                                                           \
            for (int i = 0; i < row_num; ++i)                                       \
            {                                                                       \
                dest_##TYPE##_vec[i] = from_##TYPE##_vec[i] - from_##TYPE##_vec[0]; \
            }                                                                       \
        }                                                                           \
    }                                                                               \
    CATCH                                                                           \
    BENCHMARK_REGISTER_F(CastToDecimalBench, sub_##TYPE)->Iterations(1000);

SUB_BENCHMARK(CastToDecimalBench, int64);
SUB_BENCHMARK(CastToDecimalBench, int128);
SUB_BENCHMARK(CastToDecimalBench, int256);
SUB_BENCHMARK(CastToDecimalBench, float32);
SUB_BENCHMARK(CastToDecimalBench, float64);
} // namespace tests
} // namespace DB
