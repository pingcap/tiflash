#include <Core/Field.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <benchmark/benchmark.h>
#include <common/types.h>

#include <cstddef>
#include <vector>

namespace DB
{
namespace tests
{

class FunctionBench : public benchmark::Fixture
{
protected:
    void SetUp(const ::benchmark::State & /*state*/) override
    {
        initializeDAGContext();
        try
        {
            DB::registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
    }

public:
    FunctionBench()
        : context(TiFlashTestEnv::getContext())
    {}

    virtual void initializeDAGContext()
    {
        dag_context_ptr = std::make_unique<DAGContext>(1024);
        context.setDAGContext(dag_context_ptr.get());
    }

    template <typename... Args>
    ColumnWithTypeAndName executeFunction(const String & func_name, const ColumnWithTypeAndName & first_column, const Args &... columns)
    {
        ColumnsWithTypeAndName vec({first_column, columns...});
        return executeFunction(func_name, vec);
    }

    template <typename... Args>
    ColumnWithTypeAndName executeFunction(const String & func_name, const ColumnNumbers & argument_column_numbers, const ColumnWithTypeAndName & first_column, const Args &... columns)
    {
        ColumnsWithTypeAndName vec({first_column, columns...});
        return executeFunction(func_name, argument_column_numbers, vec);
    }

    DAGContext & getDAGContext()
    {
        assert(dag_context_ptr != nullptr);
        return *dag_context_ptr;
    }

    ColumnWithTypeAndName executeFunction(const String & func_name, const ColumnsWithTypeAndName & columns)
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

    ColumnWithTypeAndName executeFunction(const String & func_name, const ColumnNumbers & argument_column_numbers, const ColumnsWithTypeAndName & columns)
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

protected:
    Context context;
    std::unique_ptr<DAGContext> dag_context_ptr;
};


class LeastBench : public FunctionBench {};

BENCHMARK_DEFINE_F(LeastBench, bench1)
(benchmark::State & state [[maybe_unused]])
{
    const String & func_name = "tidbLeast";
    std::vector<Int64> c1;
    std::vector<Int64> c2;
    std::vector<Int64> c3;
    std::vector<Int64> res;

    for (size_t i = 0; i < 10000000; ++i)
    {
        c1.push_back(i);
        c2.push_back(i + 1);
        c3.push_back(i + 2);
        res.push_back(i);
    }
    std::vector<Int64> cc1 = c1;
    std::vector<Int64> cc2 = c2;
    std::vector<Int64> cc3 = c3;
    std::vector<Int64> cres = res;
    for (size_t i = 0; i < 100; ++i)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Int64>(res),
            executeFunction(
                func_name,
                createColumn<Int64>(cc1),
                createColumn<Int64>(cc2),
                createColumn<Int64>(cc3)));
    }
}
BENCHMARK_REGISTER_F(LeastBench, bench1)->Iterations(200);
BENCHMARK_MAIN();
} // namespace tests

} // namespace DB
