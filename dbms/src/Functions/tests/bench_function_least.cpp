#include <Core/Field.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <benchmark/benchmark.h>
#include <common/types.h>

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

        auto func = bp->build(columns, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY));

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
        auto func = bp->build(arguments, TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY));
        block.insert({nullptr, func->getReturnType(), "res"});
        func->execute(block, argument_column_numbers, columns.size());
        return block.getByPosition(columns.size());
    }
};


class LeastBench : public FunctionBench
{
};

BENCHMARK_DEFINE_F(LeastBench, bench1)
(benchmark::State & state [[maybe_unused]])
try
{
    const String & func_name = "tidbLeast";
    std::vector<Int64> c1;
    std::vector<Int64> c2;
    std::vector<Int64> c3;
    std::vector<Int64> res;

    for (size_t i = 0; i < 100; ++i)
    {
        c1.push_back(i);
        c2.push_back(i + 1);
        c3.push_back(i + 2);
        res.push_back(i);
    }
    std::vector<Int64> cc1 = c1;
    std::vector<Int64> cc2 = c2;
    std::vector<Int64> cc3 = c3;
    std::vector<Int64> cres = res; // todo figure out how to support nullable test...
    auto context = DB::tests::TiFlashTestEnv::getContext();

    for (auto _ : state)
    {
        executeFunction(
            context,
            func_name,
            createColumn<Int64>(cc1),
            createColumn<Int64>(cc2),
            createColumn<Int64>(cc3));
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, bench1)->Iterations(100);

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