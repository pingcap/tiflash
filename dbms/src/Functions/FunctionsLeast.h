#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConditional.h>
#include <Functions/IFunction.h>
#include <Functions/least.h>
#include <openssl/ec.h>

#include <cstddef>
#include <ext/range.h>
#include "Columns/IColumn.h"
#include "DataTypes/IDataType.h"

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
        
        DataTypePtr type_res = arguments[0];
        for (size_t i = 1; i < arguments.size(); ++i) {
            DataTypes args{type_res, arguments[i]};
            auto res = FunctionLeast{context}.getReturnTypeImpl(args);
            type_res = std::move(res);
        }
        if (!(checkType<DataTypeInt8>(type_res)
              || checkType<DataTypeInt16>(type_res)
              || checkType<DataTypeInt32>(type_res)
              || checkType<DataTypeInt64>(type_res)
              || checkType<DataTypeFloat32>(type_res)
              || checkType<DataTypeFloat64>(type_res)))
              // todo add decimal and unsigned..
            throw Exception(
                "Illegal types " + type_res->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
        ColumnWithTypeAndName res_col;
        res_col = block.getByPosition(arguments[0]); 
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            Block temp_block {
                res_col,
                block.getByPosition(arguments[i])
            };
            auto function_least = DefaultExecutable(std::make_shared<FunctionLeast>(context));
            function_least.execute(temp_block, {0, 1}, 1);
            res_col = temp_block.getByPosition(1);
        }
        block.getByPosition(result).column = std::move(res_col.column);
    }

};

} // namespace DB
