#pragma once
#include <Columns/Collator.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>
#include <Core/AccurateComparison.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <common/types.h>
#include <fmt/core.h>

#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct LeastImpl
{
    static constexpr auto name = "tidbLeast";
    static bool apply(int cmp_result) { return cmp_result < 0; }
};
struct GreatestImpl
{
    static constexpr auto name = "tidbGreatest";
    static bool apply(int cmp_result) { return cmp_result > 0; }
};

template <typename Impl, typename SpecializedFunction>
class FunctionVectorizedLeastGreatest : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    explicit FunctionVectorizedLeastGreatest(const Context & context)
        : context(context){};
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionVectorizedLeastGreatest<Impl, SpecializedFunction>>(context);
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypePtr type_res = arguments[0];
        auto function = SpecializedFunction{context};
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            DataTypes args{type_res, arguments[i]};
            auto res = function.getReturnTypeImpl(args);
            type_res = std::move(res);
        }
        return type_res;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        size_t num_arguments = arguments.size();
        if (num_arguments < 2)
        {
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        DataTypes data_types(num_arguments);
        for (size_t i = 0; i < num_arguments; ++i)
            data_types[i] = block.getByPosition(arguments[i]).type;
        return executeNary(block, arguments, result);
    }

private:
    void executeNary(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        ColumnWithTypeAndName pre_col = block.getByPosition(arguments[0]);
        auto function_builder = DefaultFunctionBuilder(std::make_shared<SpecializedFunction>(context));
        auto function = SpecializedFunction::create(context);
        ColumnNumbers col_nums = {0, 1};
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            Block temp_block{pre_col, block.getByPosition(arguments[i])};
            DataTypePtr res_type = function_builder.getReturnTypeImpl({pre_col.type, block.getByPosition(arguments[i]).type});
            temp_block.insert({nullptr, res_type, "res_col"});
            function->executeImpl(temp_block, col_nums, 2);
            pre_col = std::move(temp_block.getByPosition(2));
        }
        block.getByPosition(result).column = std::move(pre_col.column);
    }
    const Context & context;
};

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
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return FunctionVectorizedLeastGreatest<Impl, SpecializedFunction>::create(context)->getReturnTypeImpl(arguments);
    }

    template <typename T, typename ColVec>
    void dispatch(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        auto col_to = ColVec::create(block.rows());
        auto & vec_to = col_to->getData();
        size_t num_arguments = arguments.size();

        for (size_t row_num = 0; row_num < block.rows(); ++row_num)
        {
            size_t best_arg = 0;
            for (size_t arg = 1; arg < num_arguments; ++arg)
            {
                if (const auto * from = checkAndGetColumn<ColVec>(block.getByPosition(arg).column.get()); from)
                {
                    if (const auto * best = checkAndGetColumn<ColVec>(block.getByPosition(best_arg).column.get()); best)
                    {
                        const auto & vec_from = from->getData();
                        const auto & vec_best = best->getData();
                        int cmp_result = accurate::lessOp(vec_from[row_num], vec_best[row_num]);
                        if (Impl::apply(cmp_result))
                            best_arg = arg;
                    }
                }
            }

            if (const auto * from = checkAndGetColumn<ColVec>(block.getByPosition(best_arg).column.get()); from)
            {
                const auto & vec_from = from->getData();
                vec_to[row_num] = vec_from[row_num];
            }
        }
        block.getByPosition(result).column = std::move(col_to);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result [[maybe_unused]]) const override
    {
        size_t num_arguments = arguments.size();
        if (num_arguments < 2)
        {
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypes data_types(num_arguments);
        for (size_t i = 0; i < num_arguments; ++i)
            data_types[i] = block.getByPosition(arguments[i]).type;

        DataTypePtr result_type = getReturnTypeImpl(data_types);

        if (checkDataType<DataTypeInt64>(result_type.get()))
        {
            dispatch<Int64, ColumnInt64>(block, arguments, result);
        }
        else if (checkDataType<DataTypeUInt64>(result_type.get()))
        {
            dispatch<UInt64, ColumnUInt64>(block, arguments, result);
        }
        else if (checkDataType<DataTypeFloat64>(result_type.get()))
        {
            dispatch<Float64, ColumnFloat64>(block, arguments, result);
        }
    }

private:
    const Context & context;
};

template <typename Impl, typename SpecializedFunction>
class FunctionBuilderTiDBLeastGreatest : public IFunctionBuilder
{
public:
    explicit FunctionBuilderTiDBLeastGreatest(const Context & context_)
        : context(context_)
    {}

    static FunctionBuilderPtr create(const Context & context)
    {
        return std::make_unique<FunctionBuilderTiDBLeastGreatest<Impl, SpecializedFunction>>(context);
    }

    static constexpr auto name = Impl::name;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return FunctionVectorizedLeastGreatest<Impl, SpecializedFunction>::create(context)->getReturnTypeImpl(arguments);
    }

    FunctionBasePtr buildImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const TiDB::TiDBCollatorPtr &) const override
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        if (std::is_same_v<Impl, GreatestImpl>) //YWQ: A hack, make Greatest use non-vectorized implementation. Will remove it when the code review is done.
        {
            auto function = FunctionRowbasedLeastGreatest<Impl, SpecializedFunction>::create(context);
            return std::make_unique<DefaultFunctionBase>(function, data_types, result_type);
        }
        else
        {
            auto function = FunctionVectorizedLeastGreatest<Impl, SpecializedFunction>::create(context);
            return std::make_unique<DefaultFunctionBase>(function, data_types, result_type);
        }
    }

private:
    const Context & context;
};


} // namespace DB
