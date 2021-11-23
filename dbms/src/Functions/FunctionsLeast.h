#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/IFunction.h>

#include "DataTypes/DataTypeEnum.h"
#include "DataTypes/DataTypeNothing.h"
#include "DataTypes/DataTypesNumber.h"
#include "Functions/FunctionHelpers.h"
#include "Functions/FunctionsConditional.h"
#include "ext/range.h"

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionTiDBLeast : public IFunction
{
public:
    static constexpr auto name = "tidbLeast";
    explicit FunctionTiDBLeast(const Context & context)
        : context(context){};
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionTiDBLeast>(context);
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be at least 1.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes new_args;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (arguments[i]->onlyNull())
            {
                return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
            }
            bool is_last = i + 1 == arguments.size();
            if (is_last)
            {
                new_args.push_back(arguments[i]);
            }
            else
            {
                new_args.push_back(std::make_shared<DataTypeUInt8>());
                new_args.push_back(removeNullable(arguments[i]));
            }
        }

        auto res = FunctionMultiIf{context}.getReturnTypeImpl(new_args);
        DataTypePtr type_res;
        if (!(checkType<DataTypeInt8>(res, type_res)
              || checkType<DataTypeInt16>(res, type_res)
              || checkType<DataTypeInt32>(res, type_res)
              || checkType<DataTypeInt64>(res, type_res)
              || checkType<DataTypeFloat32>(res, type_res)
              || checkType<DataTypeFloat64>(res, type_res)))
            throw Exception(
                "Illegal types " + res->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(type_res);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result [[maybe_unused]]) const override
    {
        if (arguments.size() <= 1)
        {
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                                + toString(arguments.size()) + ", should be at least 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        else
        {
            DataTypes types;
            for (const auto i : ext::range(0, arguments.size()))
            {
                auto cur_arg = block.getByPosition(arguments[i]).type;
                types.push_back(cur_arg);
            }
            DataTypePtr result_type = getReturnTypeImpl(types);
            DataTypePtr type_res [[maybe_unused]];

            if (checkType<DataTypeInt8>(result_type, type_res)
                || checkType<DataTypeInt16>(result_type, type_res)
                || checkType<DataTypeInt32>(result_type, type_res)
                || checkType<DataTypeInt64>(result_type, type_res))
            {
                return executeNary<DataTypeInt64>(block, arguments, result);
            }

            if (checkType<DataTypeFloat32>(result_type, type_res)
                || checkType<DataTypeFloat64>(result_type, type_res))
            {
                return executeNary<DataTypeFloat64>(block, arguments, result);
            }

            throw Exception(
                "Illegal types " + result_type->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

private:
    const Context & context;
    template <typename T>
    bool checkType(const DataTypePtr & arg, DataTypePtr & type_res) const
    {
        if (typeid_cast<const T *>(arg.get()))
        {
            type_res = std::make_shared<T>();
            return true;
        }
        return false;
    }

    template <typename T>
    void executeNary(Block & block, const ColumnNumbers & arguments, size_t result [[maybe_unused]]) const
    {
        auto col_left [[maybe_unused]] = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get());
        auto col_right [[maybe_unused]] = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[1]).column.get());
        auto & left_val [[maybe_unused]] = col_left->getData();
        auto & right_val [[maybe_unused]] = col_right->getData();
        size_t size = col_left->size();
        auto out_col = ColumnVector<T>::create(size);
        typename ColumnVector<T>::Container & vec_res = out_col->getData();
        vec_res.resize(col_left->getData().size());

        for (size_t i = 0; i < size; ++i)
        {
            // check null?
            // vec_res[i] = static_cast<T>(left_val[i]) < static_cast<T>(right_val[i])
            // ? static_cast<T>(left_val[i])
            // : static_cast<T>(right_val[i]);
        }

        block.getByPosition(result).column = std::move(out_col);
    }
};

} // namespace DB
