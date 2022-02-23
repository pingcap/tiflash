#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <benchmark/benchmark.h>

#include <random>

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
        DataTypePtr from_type_uint8 = std::make_shared<DataTypeInt8>();
        DataTypePtr from_type_uint16 = std::make_shared<DataTypeInt16>();
        DataTypePtr from_type_uint32 = std::make_shared<DataTypeInt32>();
        DataTypePtr from_type_uint64 = std::make_shared<DataTypeInt64>();
        DataTypePtr from_type_dec_2_1 = std::make_shared<DataTypeDecimal32>(2, 1);
        DataTypePtr from_type_dec_10_3 = std::make_shared<DataTypeDecimal64>(10, 3);
        DataTypePtr from_type_dec_12_3 = std::make_shared<DataTypeDecimal64>(12, 3);
        DataTypePtr from_type_dec_25_3 = std::make_shared<DataTypeDecimal128>(25, 3);
        DataTypePtr from_type_dec_57_0 = std::make_shared<DataTypeDecimal256>(57, 0);
        DataTypePtr from_type_dec_60_0 = std::make_shared<DataTypeDecimal256>(60, 0);
        DataTypePtr from_type_dec_60_5 = std::make_shared<DataTypeDecimal256>(60, 5);

        auto tmp_col_int8 = from_type_int8->createColumn();
        auto tmp_col_int16 = from_type_int16->createColumn();
        auto tmp_col_int32 = from_type_int32->createColumn();
        auto tmp_col_int64 = from_type_int64->createColumn();
        auto tmp_col_uint8 = from_type_int8->createColumn();
        auto tmp_col_uint16 = from_type_int16->createColumn();
        auto tmp_col_uint32 = from_type_int32->createColumn();
        auto tmp_col_uint64 = from_type_int64->createColumn();
        auto tmp_col_dec_2_1 = from_type_dec_2_1->createColumn();
        auto tmp_col_dec_10_3 = from_type_dec_10_3->createColumn();
        auto tmp_col_dec_12_3 = from_type_dec_12_3->createColumn();
        auto tmp_col_dec_25_3 = from_type_dec_25_3->createColumn();
        auto tmp_col_dec_57_0 = from_type_dec_57_0->createColumn();
        auto tmp_col_dec_60_0 = from_type_dec_60_0->createColumn();
        auto tmp_col_dec_60_5 = from_type_dec_60_5->createColumn();

        std::default_random_engine randomer;
        for (int i = 0; i < row_num; ++i)
        {
            tmp_col_int8->insert(Field(static_cast<Int64>(randomer())));
            tmp_col_int16->insert(Field(static_cast<Int64>(randomer())));
            tmp_col_int32->insert(Field(static_cast<Int64>(randomer())));
            tmp_col_int64->insert(Field(static_cast<Int64>(randomer())));
            tmp_col_uint8->insert(Field(static_cast<Int64>(randomer())));
            tmp_col_uint16->insert(Field(static_cast<Int64>(randomer())));
            tmp_col_uint32->insert(Field(static_cast<Int64>(randomer())));
            tmp_col_uint64->insert(Field(static_cast<Int64>(randomer())));

            tmp_col_dec_2_1->insert(DecimalField(Decimal(static_cast<Int32>(randomer())), 0));
            tmp_col_dec_10_3->insert(DecimalField(Decimal(static_cast<Int64>(randomer())), 0));
            tmp_col_dec_12_3->insert(DecimalField(Decimal(static_cast<Int64>(randomer())), 0));
            tmp_col_dec_25_3->insert(DecimalField(Decimal(static_cast<Int128>(randomer())), 0));
            tmp_col_dec_57_0->insert(DecimalField(Decimal(static_cast<Int256>(randomer())), 0));
            tmp_col_dec_60_0->insert(DecimalField(Decimal(static_cast<Int256>(randomer())), 0));
            tmp_col_dec_60_5->insert(DecimalField(Decimal(static_cast<Int256>(randomer())), 0));
        }
        from_col_int8 = ColumnWithTypeAndName(std::move(tmp_col_int8), from_type_int8, "from_col_int8");
        from_col_int16 = ColumnWithTypeAndName(std::move(tmp_col_int16), from_type_int16, "from_col_int16");
        from_col_int32 = ColumnWithTypeAndName(std::move(tmp_col_int32), from_type_int32, "from_col_int32");
        from_col_int64 = ColumnWithTypeAndName(std::move(tmp_col_int64), from_type_int64, "from_col_int64");
        from_col_uint8 = ColumnWithTypeAndName(std::move(tmp_col_uint8), from_type_uint8, "from_col_uint8");
        from_col_uint16 = ColumnWithTypeAndName(std::move(tmp_col_uint16), from_type_uint16, "from_col_uint16");
        from_col_uint32 = ColumnWithTypeAndName(std::move(tmp_col_uint32), from_type_uint32, "from_col_uint32");
        from_col_uint64 = ColumnWithTypeAndName(std::move(tmp_col_uint64), from_type_uint64, "from_col_uint64");

        from_col_dec_2_1 = ColumnWithTypeAndName(std::move(tmp_col_dec_2_1), from_type_dec_2_1, "from_col_dec_2_1");
        from_col_dec_10_3 = ColumnWithTypeAndName(std::move(tmp_col_dec_10_3), from_type_dec_10_3, "from_col_dec_10_3");
        from_col_dec_12_3 = ColumnWithTypeAndName(std::move(tmp_col_dec_12_3), from_type_dec_12_3, "from_col_dec_12_3");
        from_col_dec_25_3 = ColumnWithTypeAndName(std::move(tmp_col_dec_25_3), from_type_dec_25_3, "from_col_dec_25_3");
        from_col_dec_57_0 = ColumnWithTypeAndName(std::move(tmp_col_dec_57_0), from_type_dec_57_0, "from_col_dec_57_0");
        from_col_dec_60_0 = ColumnWithTypeAndName(std::move(tmp_col_dec_60_0), from_type_dec_60_0, "from_col_dec_60_0");
        from_col_dec_60_5 = ColumnWithTypeAndName(std::move(tmp_col_dec_60_5), from_type_dec_60_5, "from_col_dec_60_5");

        DataTypePtr dest_type_dec_2_1 = createDecimal(2, 1);
        DataTypePtr dest_type_dec_2_2 = createDecimal(2, 2);
        DataTypePtr dest_type_dec_15_4 = createDecimal(15, 4);
        DataTypePtr dest_type_dec_25_4 = createDecimal(25, 4);
        DataTypePtr dest_type_dec_31_30 = createDecimal(31, 30);
        DataTypePtr dest_type_dec_60_0 = createDecimal(60, 0);
        DataTypePtr dest_type_dec_60_5 = createDecimal(60, 5);
        DataTypePtr dest_type_dec_65_30 = createDecimal(65, 30);

        dest_col_dec_2_1 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_dec_2_1->getName()), std::make_shared<DataTypeString>(), "");
        dest_col_dec_2_2 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_dec_2_2->getName()), std::make_shared<DataTypeString>(), "");
        dest_col_dec_15_4 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_dec_15_4->getName()), std::make_shared<DataTypeString>(), "");
        dest_col_dec_25_4 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_dec_25_4->getName()), std::make_shared<DataTypeString>(), "");
        dest_col_dec_31_30 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_dec_31_30->getName()), std::make_shared<DataTypeString>(), "");
        dest_col_dec_60_0 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_dec_60_0->getName()), std::make_shared<DataTypeString>(), "");
        dest_col_dec_60_5 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_dec_60_5->getName()), std::make_shared<DataTypeString>(), "");
        dest_col_dec_65_30 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_dec_65_30->getName()), std::make_shared<DataTypeString>(), "");
    }

    const int row_num = 2000;

    ColumnWithTypeAndName from_col_int8;
    ColumnWithTypeAndName from_col_int16;
    ColumnWithTypeAndName from_col_int32;
    ColumnWithTypeAndName from_col_int64;
    ColumnWithTypeAndName from_col_uint8;
    ColumnWithTypeAndName from_col_uint16;
    ColumnWithTypeAndName from_col_uint32;
    ColumnWithTypeAndName from_col_uint64;
    ColumnWithTypeAndName from_col_dec_2_1;
    ColumnWithTypeAndName from_col_dec_10_3;
    ColumnWithTypeAndName from_col_dec_12_3;
    ColumnWithTypeAndName from_col_dec_25_3;
    ColumnWithTypeAndName from_col_dec_57_0;
    ColumnWithTypeAndName from_col_dec_60_0;
    ColumnWithTypeAndName from_col_dec_60_5;

    // second arg is a const string describing dest type.
    ColumnWithTypeAndName dest_col_dec_2_1;
    ColumnWithTypeAndName dest_col_dec_2_2;
    ColumnWithTypeAndName dest_col_dec_15_4;
    ColumnWithTypeAndName dest_col_dec_25_4;
    ColumnWithTypeAndName dest_col_dec_31_30;
    ColumnWithTypeAndName dest_col_dec_60_0;
    ColumnWithTypeAndName dest_col_dec_60_5;
    ColumnWithTypeAndName dest_col_dec_65_30;
};

#define CAST_BENCHMARK(CLASS_NAME, CASE_NAME, FROM_COL, DEST_TYPE)    \
    BENCHMARK_DEFINE_F(CLASS_NAME, CASE_NAME)                         \
    (benchmark::State & state)                                        \
    try                                                               \
    {                                                                 \
        const String func_name = "tidb_cast";                         \
        auto context = DB::tests::TiFlashTestEnv::getContext();       \
        auto dag_context_ptr = std::make_unique<DAGContext>(1024);    \
        UInt64 ori_flags = dag_context_ptr->getFlags();               \
        dag_context_ptr->addFlag(TiDBSQLFlags::OVERFLOW_AS_WARNING);  \
        dag_context_ptr->addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);  \
        dag_context_ptr->clearWarnings();                             \
        context.setDAGContext(dag_context_ptr.get());                 \
        for (auto _ : state)                                          \
        {                                                             \
            executeFunction(context, func_name, FROM_COL, DEST_TYPE); \
        }                                                             \
        dag_context_ptr->setFlags(ori_flags);                         \
    }                                                                 \
    CATCH                                                             \
    BENCHMARK_REGISTER_F(CLASS_NAME, CASE_NAME)->Iterations(1000);

// int to decimal
// No check overflow; CastInterType is Int32
CAST_BENCHMARK(CastToDecimalBench, int8_to_decimal_60_0, from_col_int8, dest_col_dec_60_0);
CAST_BENCHMARK(CastToDecimalBench, uint8_to_decimal_60_0, from_col_uint8, dest_col_dec_60_0);
// No check overflow; CastInterType is Int32
CAST_BENCHMARK(CastToDecimalBench, int16_to_decimal_60_0, from_col_int16, dest_col_dec_60_0);
CAST_BENCHMARK(CastToDecimalBench, uint16_to_decimal_60_0, from_col_uint16, dest_col_dec_60_0);
// No check overflow; CastInterType is Int64
CAST_BENCHMARK(CastToDecimalBench, int32_to_decimal_60_0, from_col_int32, dest_col_dec_60_0);
CAST_BENCHMARK(CastToDecimalBench, uint32_to_decimal_60_0, from_col_uint32, dest_col_dec_60_0);
// No check overflow; CastInterType is Int128
CAST_BENCHMARK(CastToDecimalBench, int64_to_decimal_60_0, from_col_int64, dest_col_dec_60_0);
CAST_BENCHMARK(CastToDecimalBench, uint64_to_decimal_60_0, from_col_uint64, dest_col_dec_60_0);
// No check overflow; CastInterType is Int256
CAST_BENCHMARK(CastToDecimalBench, int64_to_decimal_65_30, from_col_int64, dest_col_dec_65_30);
CAST_BENCHMARK(CastToDecimalBench, uint64_to_decimal_65_30, from_col_uint64, dest_col_dec_65_30);

// int to decimal
// Need check overflow; CastInterType is Int32
CAST_BENCHMARK(CastToDecimalBench, int8_to_decimal_2_1, from_col_int8, dest_col_dec_2_1);
CAST_BENCHMARK(CastToDecimalBench, uint8_to_decimal_2_1, from_col_uint8, dest_col_dec_2_1);
// Need check overflow; CastInterType is Int32
CAST_BENCHMARK(CastToDecimalBench, int16_to_decimal_2_1, from_col_int16, dest_col_dec_2_1);
CAST_BENCHMARK(CastToDecimalBench, uint16_to_decimal_2_1, from_col_uint16, dest_col_dec_2_1);
// Need check overflow; CastInterType is Int64
CAST_BENCHMARK(CastToDecimalBench, int32_to_decimal_2_1, from_col_int32, dest_col_dec_2_1);
CAST_BENCHMARK(CastToDecimalBench, uint32_to_decimal_2_1, from_col_uint32, dest_col_dec_2_1);
// Need check overflow; CastInterType is Int128
CAST_BENCHMARK(CastToDecimalBench, int64_to_decimal_2_1, from_col_int64, dest_col_dec_2_1);
CAST_BENCHMARK(CastToDecimalBench, uint64_to_decimal_2_1, from_col_uint64, dest_col_dec_2_1);
// Need check overflow; CastInterType is Int256
CAST_BENCHMARK(CastToDecimalBench, int64_to_decimal_31_30, from_col_int64, dest_col_dec_31_30);
CAST_BENCHMARK(CastToDecimalBench, uint64_to_decimal_31_30, from_col_uint64, dest_col_dec_31_30);

// decimal(2, 1) -> decimal(60, 0): No check overflow; CastInterType is Int32
CAST_BENCHMARK(CastToDecimalBench, decimal_2_1_to_decimal_60_0, from_col_dec_2_1, dest_col_dec_60_0);
// decimal(10, 3) -> decimal(15, 4): No check overflow; CastInterType is Int64
CAST_BENCHMARK(CastToDecimalBench, decimal_10_3_to_decimal_15_4, from_col_dec_10_3, dest_col_dec_15_4);
// decimal(25, 3) -> decimal(60, 5): No check overflow; CastInterType is Int128
CAST_BENCHMARK(CastToDecimalBench, decimal_25_3_to_decimal_60_5, from_col_dec_25_3, dest_col_dec_60_5);
// decimal(57, 0) -> decimal(60, 0): No check overflow; CastInterType is Int256
CAST_BENCHMARK(CastToDecimalBench, decimal_57_0_to_decimal_60_0, from_col_dec_57_0, dest_col_dec_60_0);

// decimal(2, 1) -> decimal(2, 2): No check overflow; CastInterType is Int32
CAST_BENCHMARK(CastToDecimalBench, decimal_2_1_to_decimal_2_2, from_col_dec_2_1, dest_col_dec_2_2);
// decimal(12, 3) -> decimal(15, 4): Need check overflow; CastInterType is Int64
CAST_BENCHMARK(CastToDecimalBench, decimal_12_3_to_decimal_15_4, from_col_dec_12_3, dest_col_dec_15_4);
// decimal(25, 3) -> decimal(25, 4): No check overflow; CastInterType is Int128
CAST_BENCHMARK(CastToDecimalBench, decimal_25_3_to_decimal_25_4, from_col_dec_25_3, dest_col_dec_25_4);
// decimal(60, 0) -> decimal(60, 5): Need check overflow; CastInterType is Int256
CAST_BENCHMARK(CastToDecimalBench, decimal_60_0_to_decimal_60_5, from_col_dec_60_0, dest_col_dec_60_5);

// decimal(60, 5) -> decimal(60, 0): No check overflow; CastInterType is Int256
CAST_BENCHMARK(CastToDecimalBench, decimal_60_5_to_decimal_60_0, from_col_dec_60_5, dest_col_dec_60_0);
} // namespace tests
} // namespace DB

int main(int argc, char * argv[])
{
    benchmark::Initialize(&argc, argv);
    DB::tests::TiFlashTestEnv::setupLogger();
    DB::tests::TiFlashTestEnv::initializeGlobalContext();
    if (::benchmark::ReportUnrecognizedArguments(argc, argv))
        return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    DB::tests::TiFlashTestEnv::shutdown();
    ::benchmark::Shutdown();
    return 0;
}
