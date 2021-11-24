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
        if (!(checkType<DataTypeInt8>(res)
              || checkType<DataTypeInt16>(res)
              || checkType<DataTypeInt32>(res)
              || checkType<DataTypeInt64>(res)
              || checkType<DataTypeFloat32>(res)
              || checkType<DataTypeFloat64>(res)))
            throw Exception(
                "Illegal types " + res->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (checkType<DataTypeInt8>(res)
           || checkType<DataTypeInt16>(res)
           || checkType<DataTypeInt32>(res)
           || checkType<DataTypeInt64>(res)) 
        {
            return std::make_shared<DataTypeInt64>();
        }

        if (checkType<DataTypeFloat32>(res)
            || checkType<DataTypeFloat64>(res))
        {
            return std::make_shared<DataTypeFloat64>();
        }

        throw Exception(
                "Illegal types " + res->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result [[maybe_unused]]) const override
    {
        if (arguments.size() < 2)
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

            if (checkType<DataTypeInt8>(result_type)
                || checkType<DataTypeInt16>(result_type)
                || checkType<DataTypeInt32>(result_type)
                || checkType<DataTypeInt64>(result_type))
            {
                return executeNary<DataTypeInt64::FieldType>(block, arguments, result, types);
            }

            if (checkType<DataTypeFloat32>(result_type)
                || checkType<DataTypeFloat64>(result_type))
            {
                return executeNary<DataTypeFloat64::FieldType>(block, arguments, result, types);
            }

            throw Exception(
                "Illegal types " + result_type->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

private:
    const Context & context;

    template <typename T0>
    bool checkType(const DataTypePtr & arg) const
    {
        return static_cast<bool>(typeid_cast<const T0 *>(arg.get()));
    }


    template <typename T>
    void executeNary(Block & block, const ColumnNumbers & arguments, size_t result, DataTypes & types) const
    {
        // just a POC.
        const DataTypePtr first_type = types[0];
        if (checkType<DataTypeInt8>(first_type))
        {
            const auto * first_col = checkAndGetColumn<ColumnVector<DataTypeInt8::FieldType>>(
                block.getByPosition(arguments[0]).column.get());
            
            size_t size = first_col->size(); 
            auto out_col = ColumnVector<T>::create(size);
            typename ColumnVector<T>::Container & vec_res = out_col->getData();
            vec_res.resize(size);
            const auto & first_val = first_col->getData();
            for (size_t i = 0; i < size; i++) 
            {
                T cur_val = static_cast<T>(static_cast<unsigned char>(first_val[i]));  // so ugly... and not reuseable
                for (size_t j = 1; j < arguments.size(); j++)
                {
                    const DataTypePtr cur_type = types[j];
                    if (checkType<DataTypeInt8>(cur_type))
                    {
                        const auto * cur_col = checkAndGetColumn<ColumnVector<DataTypeInt8::FieldType>>(
                                                    block.getByPosition(arguments[j]).column.get());
                        const auto & val = cur_col->getData();
                        cur_val = cur_val < static_cast<T>(val[i]) ? cur_val : static_cast<T>(val[i]);
                        vec_res[i] = cur_val;
                    } 
                    else if (checkType<DataTypeInt16>(cur_type))
                    {
                        const auto * cur_col = checkAndGetColumn<ColumnVector<DataTypeInt16::FieldType>>(
                                                    block.getByPosition(arguments[j]).column.get());
                        const auto & val = cur_col->getData();
                        cur_val = cur_val < static_cast<T>(val[i]) ? cur_val : static_cast<T>(val[i]);
                        vec_res[i] = cur_val;
                    } 
                    else if (checkType<DataTypeInt32>(cur_type))
                    {
                        const auto * cur_col = checkAndGetColumn<ColumnVector<DataTypeInt32::FieldType>>(
                                                    block.getByPosition(arguments[j]).column.get());
                        const auto & val = cur_col->getData();
                        cur_val = cur_val < static_cast<T>(val[i]) ? cur_val : static_cast<T>(val[i]);
                        vec_res[i] = cur_val;
                    } 
                    else if (checkType<DataTypeInt64>(cur_type))
                    {
                        const auto * cur_col = checkAndGetColumn<ColumnVector<DataTypeInt64::FieldType>>(
                                                    block.getByPosition(arguments[j]).column.get());
                        const auto & val = cur_col->getData();
                        cur_val = cur_val < static_cast<T>(val[i]) ? cur_val : static_cast<T>(val[i]);
                        vec_res[i] = cur_val;
                    }
                }
            }  
            // todo tidy up code...
            block.getByPosition(result).column = std::move(out_col);
        }
    }
};

} // namespace DB
