#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <benchmark/benchmark.h>

#include <vector>


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

    const size_t data_size = 10000000;
    std::vector<DataTypePtr> data_types = {
        makeDataType<Nullable<Int64>>(),
        makeDataType<Nullable<UInt64>>(),
        makeDataType<Nullable<Float64>>(),
        makeDataType<Nullable<Decimal32>>(9, 3),
        makeDataType<Nullable<Decimal64>>(18, 6),
        makeDataType<Nullable<Decimal128>>(38, 10),
        makeDataType<Nullable<Decimal256>>(65, 20),
        makeDataType<Nullable<String>>()};
};


class LeastBench : public FunctionBench
{
};

BENCHMARK_DEFINE_F(LeastBench, benchVec)
(benchmark::State & state)
try
{
    const String & func_name = "tidbLeast";

    auto c1 = data_types[0]->createColumn();
    auto c2 = data_types[0]->createColumn();
    auto c3 = data_types[0]->createColumn();
    for (size_t i = 0; i < data_size; ++i)
    {
        c1->insert(Field(static_cast<Int64>(i)));
        c2->insert(Field(static_cast<Int64>(i + 1)));
        c3->insert(Field(static_cast<Int64>(i + 2)));
    }
    auto col1 = ColumnWithTypeAndName(std::move(c1), data_types[0], "col1");
    auto col2 = ColumnWithTypeAndName(std::move(c2), data_types[0], "col2");
    auto col3 = ColumnWithTypeAndName(std::move(c3), data_types[0], "col3");
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(
            context,
            func_name,
            {col1,
             col2,
             col3});
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchVec)->Iterations(100);

BENCHMARK_DEFINE_F(LeastBench, benchVecWithNullable)
(benchmark::State & state)
try
{
    const String & func_name = "tidbLeast";

    auto c1 = data_types[0]->createColumn();
    auto c2 = data_types[0]->createColumn();
    auto c3 = data_types[0]->createColumn();
    for (size_t i = 0; i < data_size; ++i)
    {
        if (i % 2)
            c1->insert(Null());
        else
            c1->insert(Field(static_cast<Int64>(i)));
        if (i % 2)
            c2->insert(Null());
        else
            c2->insert(Field(static_cast<Int64>(i + 1)));
        if (i % 2)
            c3->insert(Null());
        else
            c3->insert(Field(static_cast<Int64>(i + 2)));
    }
    auto col1 = ColumnWithTypeAndName(std::move(c1), data_types[0], "col1");
    auto col2 = ColumnWithTypeAndName(std::move(c2), data_types[0], "col2");
    auto col3 = ColumnWithTypeAndName(std::move(c3), data_types[0], "col3");
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(
            context,
            func_name,
            {col1,
             col2,
             col3});
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchVecWithNullable)->Iterations(100);

BENCHMARK_DEFINE_F(LeastBench, benchNormal)
(benchmark::State & state)
try
{
    const String & func_name = "tidbGreatest";
    auto c1 = data_types[0]->createColumn();
    auto c2 = data_types[0]->createColumn();
    auto c3 = data_types[0]->createColumn();
    for (size_t i = 0; i < data_size; ++i)
    {
        c1->insert(Field(static_cast<Int64>(i)));
        c2->insert(Field(static_cast<Int64>(i + 1)));
        c3->insert(Field(static_cast<Int64>(i + 2)));
    }
    auto col1 = ColumnWithTypeAndName(std::move(c1), data_types[0], "col1");
    auto col2 = ColumnWithTypeAndName(std::move(c2), data_types[0], "col2");
    auto col3 = ColumnWithTypeAndName(std::move(c3), data_types[0], "col3");
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(
            context,
            func_name,
            {col1,
             col2,
             col3});
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchNormal)->Iterations(100);

BENCHMARK_DEFINE_F(LeastBench, benchNormalWithNullable)
(benchmark::State & state)
try
{
    const String & func_name = "tidbGreatest";

    auto c1 = data_types[0]->createColumn();
    auto c2 = data_types[0]->createColumn();
    auto c3 = data_types[0]->createColumn();
    for (size_t i = 0; i < data_size; ++i)
    {
        if (i % 2)
            c1->insert(Null());
        else
            c1->insert(Field(static_cast<Int64>(i)));
        if (i % 2)
            c2->insert(Null());
        else
            c2->insert(Field(static_cast<Int64>(i + 1)));
        if (i % 2)
            c3->insert(Null());
        else
            c3->insert(Field(static_cast<Int64>(i + 2)));
    }
    auto col1 = ColumnWithTypeAndName(std::move(c1), data_types[0], "col1");
    auto col2 = ColumnWithTypeAndName(std::move(c2), data_types[0], "col2");
    auto col3 = ColumnWithTypeAndName(std::move(c3), data_types[0], "col3");
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(
            context,
            func_name,
            {col1,
             col2,
             col3});
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchNormalWithNullable)->Iterations(100);


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