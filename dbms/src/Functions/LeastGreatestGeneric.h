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
#include <Functions/FunctionsComparison.h>

#include <cstddef>
#include <ext/range.h>
#include <memory>

#include "Columns/IColumn.h"
#include "Common/PODArray.h"
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/DataTypeDate.h"
#include "DataTypes/DataTypeDateTime.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/IDataType.h"
#include "Functions/FunctionsComparison.h"
#include "Interpreters/Context.h"
#include "Interpreters/castColumn.h"
#include "common/types.h"

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



template <LeastGreatest kind, typename SpecializedFunction>
class FunctionLeastGreatest : public IFunction
{
public:
    static constexpr auto name = kind == LeastGreatest::Least ? "tidbLeast1" : "tidbGreatest1";
    explicit FunctionLeastGreatest(const Context & context)
        : context(context){};
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionLeastGreatest<kind, SpecializedFunction>>(context);
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
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be at least 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = block.getByPosition(arguments[i]).type;

        DataTypePtr result_type = getReturnTypeImpl(data_types);
        for (auto argument : arguments)
        {
            block.getByPosition(argument).column = castColumn(block.getByPosition(argument), 
                result_type, context)->convertToFullColumnIfConst();
            // copyToResult(res, rhs, cmp_result);
        }
        ColumnWithTypeAndName cmp_result;
        ColumnPtr cmp_result_col = ColumnVector<UInt8>::create(block.rows());
        cmp_result.column = std::move(cmp_result_col);
        cmp_result.name = "cmp_result";
        cmp_result.type = std::make_shared<DataTypeUInt8>();
        ColumnWithTypeAndName res;
        ColumnPtr result_col = block.getByPosition(arguments[0]).column;
        res.column = std::move(result_col);
        res.name = "result";
        res.type = result_type;


        for (size_t i = 1; i < arguments.size(); ++i)
        {
            Block temp_block {
                cmp_result,
                res,
                block.getByPosition(arguments[i])
            };
            
            FunctionLess{}.executeImpl(temp_block, {1, 2}, 0);
        }
        block.getByPosition(result) = std::move(res);
    }

    template <typename T0, typename T1, typename T2>
    void vector_vector(T0 & a, T1 & b, T2 & res) const
    {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i)
        {
            if (a[i] == 0)
                res[i] = b[i];
        }
    }

private:
    const Context & context;

    template <typename T0>
    bool checkType(const DataTypePtr & arg) const
    {
        return static_cast<bool>(typeid_cast<const T0 *>(arg.get()));
    }

    template <typename F>
    bool castType(const IDataType * type, F && f) const
    {
        return castTypeToEither<
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64,
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeFloat32,
            DataTypeFloat64,
            DataTypeDate,
            DataTypeDateTime,
            DataTypeDecimal32,
            DataTypeDecimal64,
            DataTypeDecimal128,
            DataTypeDecimal256>(type, std::forward<F>(f));
    }

    // template <typename F>
    // bool castBothTypes(DataTypePtr left, DataTypePtr right, DataTypePtr result, F && f) const
    // {
    //     return castType(left.get(), [&](auto & left_, bool is_left_nullable_) {
    //         return castType(right.get(), [&](auto & right_, bool is_right_nullable_) {
    //             return castType(result.get(), [&](auto & result_, bool) {
    //                 return f(left_, is_left_nullable_, right_, is_right_nullable_, result_);
    //             });
    //         });
    //     });
    // }

    template <typename F>
    bool castBothTypes(DataTypePtr left, DataTypePtr right, DataTypePtr result, F && f) const
    {
        return castType(left.get(), [&](const auto & left_, bool is_left_nullable_) {
            return castType(right.get(), [&](const auto & right_, bool is_right_nullable_) {
                return castType(result.get(), [&](const auto & result_, bool) {
                    return f(left_, is_left_nullable_, right_, is_right_nullable_, result_);
                });
            });
        });
    }
};

} // namespace DB
