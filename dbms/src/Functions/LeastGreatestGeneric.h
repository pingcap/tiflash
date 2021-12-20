#pragma once
#include <Columns/Collator.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>
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
class FunctionTiDBLeastGreatest : public IFunction
{
public:
    static constexpr auto name = kind == LeastGreatest::Least ? "tidbLeast" : "tidbGreatest";
    explicit FunctionTiDBLeastGreatest(const Context & context)
        : context(context){};
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionTiDBLeastGreatest<kind, SpecializedFunction>>(context);
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be at least 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypePtr type_res = arguments[0];
        if (type_res->isString())
            return std::make_shared<DataTypeString>();
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            if (arguments[i]->isString())
                return std::make_shared<DataTypeString>();
            DataTypes args{type_res, arguments[i]};
            auto res = SpecializedFunction{context}.getReturnTypeImpl(args);
            type_res = std::move(res);
        }
        return type_res;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        size_t num_arguments = arguments.size();
        if (num_arguments < 2)
        {
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                                + toString(arguments.size()) + ", should be at least 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        else
        {
            bool flag = false;
            DataTypes data_types(num_arguments);
            for (size_t i = 0; i < num_arguments; ++i)
            {
                data_types[i] = block.getByPosition(arguments[i]).type;
                if (data_types[i]->isString())
                    flag = true;
            }
            if (flag) // Need to cast all other column's type into string
            {
                DataTypePtr result_type = getReturnTypeImpl(data_types);
                for (size_t arg = 0; arg < num_arguments; ++arg)
                {
                    auto res_column = TiDBCastColumn(block.getByPosition(arguments[arg]), result_type, context);
                    block.getByPosition(arguments[arg]).column = std::move(res_column);
                    block.getByPosition(arguments[arg]).type = result_type;
                }
            }
            return executeNary(block, arguments, result);
        }
    }

    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }

private:
    const Context & context;
    TiDB::TiDBCollatorPtr collator;

    void executeNary(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        ColumnWithTypeAndName pre_col = block.getByPosition(arguments[0]);
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            DataTypePtr res_type;
            Block temp_block{
                pre_col,
                block.getByPosition(arguments[i])};

            auto least_builder = DefaultFunctionBuilder(std::make_shared<SpecializedFunction>(context));
            res_type = least_builder.getReturnTypeImpl({pre_col.type, block.getByPosition(arguments[i]).type});

            temp_block.insert({nullptr, res_type, "res_col"});
            auto function = SpecializedFunction::create(context);
            function->setCollator(collator);
            function->executeImpl(temp_block, {0, 1}, 2);
            pre_col = std::move(temp_block.getByPosition(2)); // This code could have performance issue.
        }
        block.getByPosition(result).column = std::move(pre_col.column);
    }
};

template <LeastGreatest kind, typename SpecializedFunction>
class FunctionTiDBLeastGreatestGeneric : public IFunction
{
public:
    static constexpr auto name = kind == LeastGreatest::Least ? "tidbLeast" : "tidbGreatest";
    explicit FunctionTiDBLeastGreatestGeneric(const Context & context)
        : context(context){};
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionTiDBLeastGreatestGeneric<kind, SpecializedFunction>>(context);
    }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be at least 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto & argument : arguments)
            if (argument->isString())
                return std::make_shared<DataTypeString>();

        return FunctionTiDBLeastGreatest<kind, SpecializedFunction>::create(context)->getReturnTypeImpl(arguments);
    }


    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        size_t num_arguments = arguments.size();
        if (num_arguments < 2)
        {
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                                + toString(arguments.size()) + ", should be at least 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypes data_types(num_arguments);
        for (size_t i = 0; i < num_arguments; ++i)
            data_types[i] = block.getByPosition(arguments[i]).type;

        DataTypePtr result_type = getReturnTypeImpl(data_types);
        Columns converted_columns(num_arguments);
        for (size_t arg = 0; arg < num_arguments; ++arg)
        {
            converted_columns[arg] = TiDBCastColumn(block.getByPosition(arguments[arg]), result_type, context);
            auto temp = converted_columns[arg]->convertToFullColumnIfConst();
            if (temp != nullptr)
                converted_columns[arg] = std::move(temp);
        }

        auto result_column = result_type->createColumn();
        result_column->reserve(block.rows());

        for (size_t row_num = 0; row_num < block.rows(); ++row_num)
        {
            size_t best_arg = 0;
            for (size_t arg = 1; arg < num_arguments; ++arg)
            {
                int cmp_result;
                if (typeid_cast<const DataTypeString *>(result_type.get()) && collator != nullptr)
                    cmp_result = converted_columns[arg]->compareAtWithCollation(row_num, row_num, *converted_columns[best_arg], 1, *collator.get());
                else
                    cmp_result = converted_columns[arg]->compareAt(row_num, row_num, *converted_columns[best_arg], 1);

                if constexpr (kind == LeastGreatest::Least)
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

            result_column->insertFrom(*converted_columns[best_arg], row_num);
        }
        block.getByPosition(result).column = std::move(result_column);
    }

    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }

private:
    const Context & context;
    TiDB::TiDBCollatorPtr collator;
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
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be at least 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto & argument : arguments)
        {
            if (argument->isString())
                return std::make_shared<DataTypeString>();
        }

        return FunctionTiDBLeastGreatest<kind, SpecializedFunction>::create(context)->getReturnTypeImpl(arguments);
    }

    FunctionBasePtr buildImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const TiDB::TiDBCollatorPtr & collator) const override
    {
        DataTypes data_types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        if (kind == LeastGreatest::Greatest) //YWQ: A hack, make Greatest use non-vectorised implementation.
        {
            auto function = FunctionTiDBLeastGreatestGeneric<kind, SpecializedFunction>::create(context);
            function->setCollator(collator);
            return std::make_unique<DefaultFunctionBase>(function, data_types, result_type);
        }
        else
        {
            auto function = FunctionTiDBLeastGreatest<kind, SpecializedFunction>::create(context);
            function->setCollator(collator);
            return std::make_unique<DefaultFunctionBase>(function, data_types, result_type);
        }
    }

private:
    const Context & context;
};


} // namespace DB
