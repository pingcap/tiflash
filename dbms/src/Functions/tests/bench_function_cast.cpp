#include <benchmark/benchmark.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <random>

namespace DB
{
namespace tests
{

class FunctionBench : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) override
    {
        try
        {
            DB::registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    }

    template <typename... Args>
    ColumnWithTypeAndName executeFunction(Context & context, const String & func_name, const ColumnWithTypeAndName & first_column, const Args &... columns)
    {
        ColumnsWithTypeAndName vec({first_column, columns...});
        return executeFunction(context, func_name, vec);
    }

    template <typename... Args>
    ColumnWithTypeAndName executeFunction(Context & context, const String & func_name, const ColumnNumbers & argument_column_numbers, const ColumnWithTypeAndName & first_column, const Args &... columns)
    {
        ColumnsWithTypeAndName vec({first_column, columns...});
        return executeFunction(context, func_name, argument_column_numbers, vec);
    }

    static ColumnWithTypeAndName executeFunction(Context & context, const String & func_name, const ColumnsWithTypeAndName & columns)
    {
        auto & factory = FunctionFactory::instance();

        Block block(columns);
        ColumnNumbers cns;
        for (size_t i = 0; i < columns.size(); ++i)
            cns.push_back(i);

        auto bp = factory.tryGet(func_name, context);
        if (!bp)
            throw TiFlashTestException(fmt::format("Function {} not found!", func_name));

        auto func = bp->build(columns);

        block.insert({nullptr, func->getReturnType(), "res"});
        func->execute(block, cns, columns.size());
        return block.getByPosition(columns.size());
    }

    static ColumnWithTypeAndName executeFunction(Context & context, const String & func_name, const ColumnNumbers & argument_column_numbers, const ColumnsWithTypeAndName & columns)
    {
        auto & factory = FunctionFactory::instance();
        Block block(columns);
        ColumnsWithTypeAndName arguments;
        for (size_t i = 0; i < argument_column_numbers.size(); ++i)
            arguments.push_back(columns.at(i));
        auto bp = factory.tryGet(func_name, context);
        if (!bp)
            throw TiFlashTestException(fmt::format("Function {} not found!", func_name));
        auto func = bp->build(arguments);
        block.insert({nullptr, func->getReturnType(), "res"});
        func->execute(block, argument_column_numbers, columns.size());
        return block.getByPosition(columns.size());
    }
};

class CastIntToDecimalBench : public FunctionBench
{
    public:
        void SetUp(const benchmark::State & state) override
        {
            FunctionBench::SetUp(state);
            initData();
        }
        // cast(col_int64 as decimal(20, 10))
        void initData()
        {
            DataTypePtr from_type_int8 = std::make_shared<DataTypeInt8>();
            DataTypePtr from_type_int32 = std::make_shared<DataTypeInt32>();
            auto tmp_col_int8 = from_type_int8->createColumn();
            auto tmp_col_int32 = from_type_int8->createColumn();
            std::default_random_engine randomer;
            for (int i = 0; i < row_num; ++i) {
                tmp_col_int8->insert(Field(static_cast<Int64>(randomer())));
                tmp_col_int32->insert(Field(static_cast<Int64>(randomer())));
            }
            from_col_int8 = ColumnWithTypeAndName(std::move(tmp_col_int8), from_type_int8, "from_col_int8");
            from_col_int32 = ColumnWithTypeAndName(std::move(tmp_col_int32), from_type_int32, "from_col_int32");

            DataTypePtr dest_type_decimal32 = createDecimal(9, 0);
            DataTypePtr dest_type_decimal64 = createDecimal(18, 0);
            DataTypePtr dest_type_decimal128 = createDecimal(38, 0);
            DataTypePtr dest_type_decimal256 = createDecimal(65, 0);

            dest_col_decimal32 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_decimal32->getName()), std::make_shared<DataTypeString>(), "");
            res_col_decimal32 = ColumnWithTypeAndName(nullptr, dest_type_decimal32, "");
            dest_col_decimal64 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_decimal64->getName()), std::make_shared<DataTypeString>(), "");
            res_col_decimal64 = ColumnWithTypeAndName(nullptr, dest_type_decimal64, "");
            dest_col_decimal128 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_decimal128->getName()), std::make_shared<DataTypeString>(), "");
            res_col_decimal128 = ColumnWithTypeAndName(nullptr, dest_type_decimal128, "");
            dest_col_decimal256 = ColumnWithTypeAndName(DataTypeString().createColumnConst(row_num, dest_type_decimal256->getName()), std::make_shared<DataTypeString>(), "");
            res_col_decimal256 = ColumnWithTypeAndName(nullptr, dest_type_decimal256, "");
        }

        const int row_num = 2000;

        ColumnWithTypeAndName from_col_int8;
        ColumnWithTypeAndName from_col_int32;

        // second arg is a const string describing dest type.
        ColumnWithTypeAndName dest_col_decimal32;
        ColumnWithTypeAndName dest_col_decimal64;
        ColumnWithTypeAndName dest_col_decimal128;
        ColumnWithTypeAndName dest_col_decimal256;

        // res_col stores the casted value.
        ColumnWithTypeAndName res_col_decimal32;
        ColumnWithTypeAndName res_col_decimal64;
        ColumnWithTypeAndName res_col_decimal128;
        ColumnWithTypeAndName res_col_decimal256;
};

// We can skip check overflow because int8_prec(3) < decimal32_prec(9).
BENCHMARK_DEFINE_F(CastIntToDecimalBench, int8_to_decimal32)
    (benchmark::State & state)
    try
{
    const String func_name = "tidb_cast";
    auto context = DB::tests::TiFlashTestEnv::getContext();
    auto dag_context_ptr = std::make_unique<DAGContext>(1024);
    context.setDAGContext(dag_context_ptr.get());
    for (auto _ : state)
    {
        executeFunction(context, func_name, from_col_int8, dest_col_decimal32);
    }
}
CATCH
BENCHMARK_REGISTER_F(CastIntToDecimalBench, int8_to_decimal32)->Iterations(1000);

// Cannot skip check overflow because int32_prec(10) > decimal32_prec(9).
BENCHMARK_DEFINE_F(CastIntToDecimalBench, int32_to_decimal32)
    (benchmark::State & state)
    try
{
    const String func_name = "tidb_cast";
    auto context = DB::tests::TiFlashTestEnv::getContext();
    auto dag_context_ptr = std::make_unique<DAGContext>(1024);
    context.setDAGContext(dag_context_ptr.get());
    for (auto _ : state)
    {
        executeFunction(context, func_name, from_col_int32, dest_col_decimal32);
    }
}
CATCH
BENCHMARK_REGISTER_F(CastIntToDecimalBench, int8_to_decimal32)->Iterations(1000);
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
