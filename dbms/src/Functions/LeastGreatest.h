#pragma once
#include <Columns/Collator.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>
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

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

enum class LeastGreatest
{
    Least,
    Greatest
};

template <LeastGreatest kind, typename SpecializedFunction>
class FunctionVectorizedLeastGreatest : public IFunction
{
public:
    static constexpr auto name = kind == LeastGreatest::Least ? "tidbLeast" : "tidbGreatest";
    explicit FunctionVectorizedLeastGreatest(const Context & context)
        : context(context){};
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionVectorizedLeastGreatest<kind, SpecializedFunction>>(context);
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
            Block temp_block{
                pre_col,
                block.getByPosition(arguments[i])};
            DataTypePtr res_type = function_builder.getReturnTypeImpl({pre_col.type, block.getByPosition(arguments[i]).type});
            temp_block.insert({nullptr, res_type, "res_col"});
            function->executeImpl(temp_block, col_nums, 2);
            pre_col = std::move(temp_block.getByPosition(2));
        }
        block.getByPosition(result).column = std::move(pre_col.column);
    }
    const Context & context;
};

template <LeastGreatest kind, typename SpecializedFunction>
class FunctionRowbasedLeastGreatest : public IFunction
{
public:
    static constexpr auto name = kind == LeastGreatest::Least ? "tidbLeast" : "tidbGreatest";
    explicit FunctionRowbasedLeastGreatest(const Context & context)
        : context(context){};
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionRowbasedLeastGreatest<kind, SpecializedFunction>>(context);
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

        return FunctionVectorizedLeastGreatest<kind, SpecializedFunction>::create(context)->getReturnTypeImpl(arguments);
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

        DataTypePtr result_type = getReturnTypeImpl(data_types);

        auto result_column = result_type->createColumn();
        result_column->reserve(block.rows());

        for (size_t row_num = 0; row_num < block.rows(); ++row_num)
        {
            size_t best_arg = 0;
            for (size_t arg = 1; arg < num_arguments; ++arg)
            {
                int cmp_result = block.getByPosition(arguments[arg]).column->compareAt(row_num, row_num, *block.getByPosition(arguments[best_arg]).column, 1);

                if constexpr (kind == LeastGreatest::Least) // YWQ TODO: simplify it
                {
                    if (cmp_result < 0)
                        best_arg = arg;
                }
                else
                {
                    if (cmp_result > 0)
                        best_arg = arg;
                }
            }

            result_column->insertFrom(*block.getByPosition(arguments[best_arg]).column, row_num);
        }
        block.getByPosition(result).column = std::move(result_column);
    }

private:
    const Context & context;
};

template <LeastGreatest kind, typename SpecializedFunction>
class FunctionBuilderTiDBLeastGreatest : public IFunctionBuilder
{
public:
    explicit FunctionBuilderTiDBLeastGreatest(const Context & context_)
        : context(context_)
    {}

    static FunctionBuilderPtr create(const Context & context)
    {
        return std::make_unique<FunctionBuilderTiDBLeastGreatest<kind, SpecializedFunction>>(context);
    }

    static constexpr auto name = kind == LeastGreatest::Least ? "tidbLeast" : "tidbGreatest";
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return FunctionVectorizedLeastGreatest<kind, SpecializedFunction>::create(context)->getReturnTypeImpl(arguments);
    }

    FunctionBasePtr buildImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const TiDB::TiDBCollatorPtr &) const override
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        if (kind == LeastGreatest::Greatest) //YWQ: A hack, make Greatest use non-vectorized implementation. Will remove it when code review is done.
        {
            auto function = FunctionRowbasedLeastGreatest<kind, SpecializedFunction>::create(context);
            return std::make_unique<DefaultFunctionBase>(function, data_types, result_type);
        }
        else
        {
            auto function = FunctionVectorizedLeastGreatest<kind, SpecializedFunction>::create(context);
            return std::make_unique<DefaultFunctionBase>(function, data_types, result_type);
        }
    }

private:
    const Context & context;
};


} // namespace DB
