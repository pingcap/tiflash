#pragma once


#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/DispatchHelper.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

template <typename T>
struct ToFieldType
{
    using type = typename T::FieldType;
};

template <typename Ptr, typename List, auto on_error>
struct CheckDataType
{
    USING_DISPATCH_HELPER;

    using Candidates = typename List::template Transform<ToFieldType>;

    template <typename Context, typename... Ts>
    static Candidates cast(Types<Ts...>, const Context & ctx)
    {
        return List::find([&](auto v) {
            return checkDataType<decltype(v.unwrap())>(Ptr::value(ctx));
        }).onInvalid([&] {
              on_error(ctx);
          }).transform([](auto type) -> Candidates { return type.index; });
    }

    template <typename Context>
    static Candidates select(const Context & ctx)
    {
        return cast(List(), ctx);
    }
};

template <typename Ptr, typename T, bool can_be_const = true>
struct CheckColumn
{
    USING_DISPATCH_HELPER;

    using ColumnType = std::conditional_t<IsDecimal<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using WithConst = Types<ColumnType, ColumnConst>;
    using Candidates = std::conditional_t<can_be_const, WithConst, Types<ColumnType>>;

    template <typename Context>
    static Candidates select(const Context & ctx)
    {
        if (can_be_const && Ptr::value(ctx)->isColumnConst())
            return Type<ColumnConst>();
        else
            return Type<ColumnType>();
    }
};

using FracDataTypes = DispatchHelper::Types<DataTypeInt64, DataTypeUInt64>;
using DecimalDataTypes = DispatchHelper::Types<DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128, DataTypeDecimal256>;
using NumericDataTypes
    = DispatchHelper::Types<DataTypeFloat32, DataTypeFloat64, DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128, DataTypeDecimal256,
        DataTypeInt8, DataTypeUInt8, DataTypeInt16, DataTypeUInt16, DataTypeInt32, DataTypeUInt32, DataTypeInt64, DataTypeUInt64>;

} // namespace DB
