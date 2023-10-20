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

#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>
#include <Functions/LeastGreatest.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <benchmark/benchmark.h>

#include <random>

namespace DB
{
template <typename A, typename B>
struct BinaryGreatestBaseImpl<A, B, false>
{
    using ResultType = typename NumberTraits::ResultOfBinaryLeastGreatest<A, B>::Type;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return accurate::greaterOp(a, b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

template <typename A, typename B>
struct BinaryGreatestBaseImpl<A, B, true>
{
    using ResultType = If<std::is_floating_point_v<A> || std::is_floating_point_v<B>, double, Decimal32>;
    using ResultPrecInferer = PlusDecimalInferer;

    template <typename Result = ResultType>
    static Result apply(A a, B b)
    {
        return static_cast<Result>(a) > static_cast<Result>(b) ? static_cast<Result>(a) : static_cast<Result>(b);
    }
    template <typename Result = ResultType>
    static Result apply(A, B, UInt8 &)
    {
        throw Exception("Should not reach here");
    }
};

struct RowbasedGreatestImpl
{
    static constexpr auto name = "tidbRowbasedGreatest";
    static bool apply(bool cmp_result) { return !cmp_result; }
};
namespace tests
{
template <typename Impl, typename SpecializedFunction>
class FunctionRowbasedLeastGreatest : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    explicit FunctionRowbasedLeastGreatest(const Context & context)
        : context(context){};
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionRowbasedLeastGreatest<Impl, SpecializedFunction>>(context);
    }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be at least 2.",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return FunctionVectorizedLeastGreatest<Impl, SpecializedFunction>::create(context)->getReturnTypeImpl(
            arguments);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        size_t num_arguments = arguments.size();
        if (num_arguments < 2)
        {
            throw Exception(
                fmt::format(
                    "Number of arguments for function {} doesn't match: passed {}, should be at least 2.",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypes data_types(num_arguments);
        for (size_t i = 0; i < num_arguments; ++i)
            data_types[i] = block.getByPosition(arguments[i]).type;

        DataTypePtr result_type = getReturnTypeImpl(data_types);

        bool is_types_valid = getColumnType(result_type, [&](const auto & type, bool) {
            using ColumnType = std::decay_t<decltype(type)>;
            using ColumnFieldType = typename ColumnType::FieldType;
            using ColVec = ColumnVector<ColumnFieldType>;
            auto col_to = ColVec::create(block.rows());
            auto & vec_to = col_to->getData();
            size_t num_arguments = arguments.size();
            size_t rows = block.rows();
            std::vector<const ColVec *> columns;

            // TODO need to convert the column
            for (size_t arg = 0; arg < num_arguments; ++arg)
            {
                if (const auto * from = checkAndGetColumn<ColVec>(block.getByPosition(arguments[arg]).column.get());
                    from)
                    columns.push_back(from);
                else
                    throw Exception(
                        fmt::format(
                            "Illegal column type {} of  arguments of function {}",
                            block.getByPosition(arguments[arg]).type->getName(),
                            getName()),
                        ErrorCodes::LOGICAL_ERROR);
            }

            for (size_t row_num = 0; row_num < rows; ++row_num)
            {
                size_t best_arg = arguments[0];
                for (size_t arg = 1; arg < num_arguments; ++arg)
                {
                    const auto & vec_from = columns[arguments[arg]]->getData();
                    const auto & vec_best = columns[arguments[best_arg]]->getData();
                    bool cmp_result = accurate::lessOp(vec_from[row_num], vec_best[row_num]);
                    if (Impl::apply(cmp_result))
                        best_arg = arg;
                }
                const auto & vec_best = columns[arguments[best_arg]]->getData();
                vec_to[row_num] = vec_best[row_num];
            }
            block.getByPosition(result).column = std::move(col_to);
            return true;
        });

        if (!is_types_valid)
            throw Exception(
                fmt::format("Illegal return type of function {}", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    template <typename F>
    static bool getColumnType(DataTypePtr type, F && f)
    {
        return castTypeToEither<DataTypeInt64, DataTypeUInt64, DataTypeFloat64>(type.get(), std::forward<F>(f));
    }

    const Context & context;
};

namespace
{
// clang-format off
struct NameGreatest               { static constexpr auto name = "greatest"; };
// clang-format on

using FunctionBinaryRowbasedGreatest = FunctionBinaryArithmetic<BinaryGreatestBaseImpl_t, NameGreatest>;
using FunctionTiDBRowbasedGreatest
    = FunctionRowbasedLeastGreatest<RowbasedGreatestImpl, FunctionBinaryRowbasedGreatest>;
} // namespace
class LeastBench : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) override
    {
        try
        {
            DB::registerFunctions();
            auto & factory = FunctionFactory::instance();
            factory.registerFunction<FunctionTiDBRowbasedGreatest>();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registered, ignore exception here.
        }
        initCols();
        initCols2();
    }

public:
    const size_t data_size_random = 30000;
    const size_t data_size = 10000000;
    const size_t col_num = 1000;
    std::vector<DataTypePtr> data_types = {makeDataType<Nullable<Int64>>()};

    ColumnWithTypeAndName col1;
    ColumnWithTypeAndName col2;
    ColumnWithTypeAndName col3;
    ColumnWithTypeAndName col_nullable1;
    ColumnWithTypeAndName col_nullable2;
    ColumnWithTypeAndName col_nullable3;
    std::vector<ColumnWithTypeAndName> v_col;

private:
    void initCols()
    {
        std::default_random_engine e;
        auto c1 = data_types[0]->createColumn();
        auto c2 = data_types[0]->createColumn();
        auto c3 = data_types[0]->createColumn();
        auto c_nullable1 = data_types[0]->createColumn();
        auto c_nullable2 = data_types[0]->createColumn();
        auto c_nullable3 = data_types[0]->createColumn();
        for (size_t i = 0; i < data_size; ++i)
        {
            c1->insert(Field(static_cast<Int64>(e())));
            c2->insert(Field(static_cast<Int64>(e())));
            c3->insert(Field(static_cast<Int64>(e())));
            if (i % 2)
                c_nullable1->insert(Null());
            else
                c_nullable1->insert(Field(static_cast<Int64>(e())));
            if (i % 2)
                c_nullable2->insert(Null());
            else
                c_nullable2->insert(Field(static_cast<Int64>(e())));
            if (i % 2)
                c_nullable3->insert(Null());
            else
                c_nullable3->insert(Field(static_cast<Int64>(e())));
        }
        col1 = ColumnWithTypeAndName(std::move(c1), data_types[0], "col1");
        col2 = ColumnWithTypeAndName(std::move(c2), data_types[0], "col2");
        col3 = ColumnWithTypeAndName(std::move(c3), data_types[0], "col3");
        col_nullable1 = ColumnWithTypeAndName(std::move(c_nullable1), data_types[0], "col_nullable1");
        col_nullable2 = ColumnWithTypeAndName(std::move(c_nullable2), data_types[0], "col_nullable2");
        col_nullable3 = ColumnWithTypeAndName(std::move(c_nullable3), data_types[0], "col_nullable3");
    }

    void initCols2()
    {
        std::vector<DataTypePtr> col_types;

        std::default_random_engine e;
        for (size_t i = 0; i < col_num; ++i)
            col_types.push_back(data_types[e() % data_types.size()]);

        std::vector<MutableColumnPtr> cols;
        for (size_t i = 0; i < col_num; ++i)
        {
            cols.push_back(col_types[i]->createColumn());
        }

        for (size_t i = 0; i < data_size_random; ++i)
        {
            for (size_t j = 0; j < col_num; j++)
                cols[j]->insert(static_cast<Int64>(e()));
        }
        for (size_t i = 0; i < col_num; ++i)
            v_col.push_back(ColumnWithTypeAndName(std::move(cols[i]), col_types[i], "col"));
    }
};

BENCHMARK_DEFINE_F(LeastBench, benchVec)
(benchmark::State & state)
try
{
    const String & func_name = "tidbLeast";
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(*context, func_name, col1, col2, col3);
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchVec)->Iterations(100);

BENCHMARK_DEFINE_F(LeastBench, benchVec2Col)
(benchmark::State & state)
try
{
    const String & func_name = "tidbLeast";
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(*context, func_name, col1, col2);
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchVec2Col)->Iterations(100);

BENCHMARK_DEFINE_F(LeastBench, benchVecWithNullable)
(benchmark::State & state)
try
{
    const String & func_name = "tidbLeast";
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(*context, func_name, col_nullable1, col_nullable2, col_nullable3);
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchVecWithNullable)->Iterations(100);

BENCHMARK_DEFINE_F(LeastBench, benchNormal)
(benchmark::State & state)
try
{
    const String & func_name = "tidbRowbasedGreatest";
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(*context, func_name, col1, col2, col3);
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchNormal)->Iterations(100);

BENCHMARK_DEFINE_F(LeastBench, benchNormal2Col)
(benchmark::State & state)
try
{
    const String & func_name = "tidbRowbasedGreatest";
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(*context, func_name, col1, col2);
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchNormal2Col)->Iterations(100);

BENCHMARK_DEFINE_F(LeastBench, benchNormalWithNullable)
(benchmark::State & state)
try
{
    const String & func_name = "tidbRowbasedGreatest";
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(*context, func_name, col_nullable1, col_nullable2, col_nullable3);
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchNormalWithNullable)->Iterations(100);


BENCHMARK_DEFINE_F(LeastBench, benchVecMoreCols)
(benchmark::State & state)
try
{
    const String & func_name = "tidbLeast";
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(*context, func_name, v_col);
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchVecMoreCols)->Iterations(100);

BENCHMARK_DEFINE_F(LeastBench, benchNormalMoreCols)
(benchmark::State & state)
try
{
    const String & func_name = "tidbRowbasedGreatest";
    auto context = DB::tests::TiFlashTestEnv::getContext();
    for (auto _ : state)
    {
        executeFunction(*context, func_name, v_col);
    }
}
CATCH
BENCHMARK_REGISTER_F(LeastBench, benchNormalMoreCols)->Iterations(100);

} // namespace tests
} // namespace DB
