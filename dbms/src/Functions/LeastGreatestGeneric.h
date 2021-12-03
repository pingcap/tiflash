#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <ext/range.h>
#include <memory>

#include "Columns/IColumn.h"
#include "DataTypes/IDataType.h"
#include "Interpreters/Context.h"

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
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            DataTypes args{type_res, arguments[i]};
            auto res = SpecializedFunction{context}.getReturnTypeImpl(args);
            type_res = std::move(res);
        }
        return type_res;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (arguments.size() < 2)
        {
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                                + toString(arguments.size()) + ", should be at least 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        else
            return executeNary(block, arguments, result);
    }

private:
    const Context & context;

    template <typename T0>
    bool checkType(const DataTypePtr & arg) const
    {
        return static_cast<bool>(typeid_cast<const T0 *>(arg.get()));
    }

    void executeNary(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        ColumnWithTypeAndName pre_col = block.getByPosition(arguments[0]);
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            // first get the return type
            DataTypePtr res_type;
            Block temp_block{
                pre_col,
                block.getByPosition(arguments[i])};

            auto least_builder = DefaultFunctionBuilder(std::make_shared<SpecializedFunction>(context));
            res_type = least_builder.getReturnTypeImpl({pre_col.type, block.getByPosition(arguments[i]).type});

            temp_block.insert({nullptr,
                               res_type,
                               "res_col"});
            DefaultExecutable(std::make_shared<SpecializedFunction>(context)).execute(temp_block, {0, 1}, 2);
            pre_col = temp_block.getByPosition(2);
        }
        block.getByPosition(result).column = std::move(pre_col.column);
    }
};

} // namespace DB
