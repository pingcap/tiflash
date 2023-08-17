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

#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>


#define FOR_UNSIGNED_INTEGER_TYPES(M) \
    M(UInt8)                          \
    M(UInt16)                         \
    M(UInt32)                         \
    M(UInt64)

#define FOR_NUMERIC_TYPES(M) \
    M(UInt8)                 \
    M(UInt16)                \
    M(UInt32)                \
    M(UInt64)                \
    M(Int8)                  \
    M(Int16)                 \
    M(Int32)                 \
    M(Int64)                 \
    M(Float32)               \
    M(Float64)

#define FOR_NUMERIC_TYPES_AND_ENUMS(M) \
    M(UInt8, DataTypeUInt8)            \
    M(UInt16, DataTypeUInt16)          \
    M(UInt32, DataTypeUInt32)          \
    M(UInt64, DataTypeUInt64)          \
    M(Int8, DataTypeInt8)              \
    M(Int16, DataTypeInt16)            \
    M(Int32, DataTypeInt32)            \
    M(Int64, DataTypeInt64)            \
    M(Float32, DataTypeFloat32)        \
    M(Float64, DataTypeFloat64)        \
    M(UInt8, DataTypeEnum8)            \
    M(UInt16, DataTypeEnum16)

#define FOR_DECIMAL_TYPES(M)          \
    M(Decimal32, DataTypeDecimal32)   \
    M(Decimal64, DataTypeDecimal64)   \
    M(Decimal128, DataTypeDecimal128) \
    M(Decimal256, DataTypeDecimal256)

namespace DB
{
template <template <typename, typename> class AggregateFunctionTemplate, typename ResultType, typename... TArgs>
static IAggregateFunction * createWithDecimalType(const IDataType & argument_type, TArgs &&... args)
{
#define DISPATCH(FIELDTYPE, DATATYPE)                  \
    if (typeid_cast<const DATATYPE *>(&argument_type)) \
        return new AggregateFunctionTemplate<FIELDTYPE, ResultType>(std::forward<TArgs>(args)...);
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <
    template <typename, typename, typename>
    class AggregateSumTemplate,
    typename ResultType,
    typename SumName,
    typename... TArgs>
static IAggregateFunction * createSumAggWithDecimalType(const IDataType & argument_type, TArgs &&... args)
{
#define DISPATCH(FIELDTYPE, DATATYPE)                  \
    if (typeid_cast<const DATATYPE *>(&argument_type)) \
        return new AggregateSumTemplate<FIELDTYPE, ResultType, SumName>(std::forward<TArgs>(args)...);
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}

/** Create an aggregate function with a numeric type in the template parameter, depending on the type of the argument.
  */
template <template <typename, typename... TArgs> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs &&... args)
{
#define DISPATCH(FIELDTYPE, DATATYPE)                  \
    if (typeid_cast<const DATATYPE *>(&argument_type)) \
        return new AggregateFunctionTemplate<FIELDTYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES_AND_ENUMS(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename Data, typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs &&... args)
{
#define DISPATCH(FIELDTYPE, DATATYPE)                  \
    if (typeid_cast<const DATATYPE *>(&argument_type)) \
        return new AggregateFunctionTemplate<FIELDTYPE, Data>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES_AND_ENUMS(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <
    template <typename, typename>
    class AggregateFunctionTemplate,
    template <typename>
    class Data,
    typename... TArgs>
static IAggregateFunction * createWithNumericType(const IDataType & argument_type, TArgs &&... args)
{
#define DISPATCH(FIELDTYPE, DATATYPE)                  \
    if (typeid_cast<const DATATYPE *>(&argument_type)) \
        return new AggregateFunctionTemplate<FIELDTYPE, Data<FIELDTYPE>>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES_AND_ENUMS(DISPATCH)
#undef DISPATCH
    return nullptr;
}


template <
    template <typename, typename>
    class AggregateFunctionTemplate,
    template <typename>
    class Data,
    typename... TArgs>
static IAggregateFunction * createWithUnsignedIntegerType(const IDataType & argument_type, TArgs &&... args)
{
#define DISPATCH(TYPE)                                       \
    if (typeid_cast<const DataType##TYPE *>(&argument_type)) \
        return new AggregateFunctionTemplate<TYPE, Data<TYPE>>(std::forward<TArgs>(args)...);
    FOR_UNSIGNED_INTEGER_TYPES(DISPATCH)
#undef DISPATCH
    return nullptr;
}


/** For template with two arguments.
  */
template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoNumericTypesSecond(const IDataType & second_type, TArgs &&... args)
{
#define DISPATCH(FIELDTYPE, DATATYPE)                \
    if (typeid_cast<const DATATYPE *>(&second_type)) \
        return new AggregateFunctionTemplate<FirstType, FIELDTYPE>(std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES_AND_ENUMS(DISPATCH)
#undef DISPATCH
    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
static IAggregateFunction * createWithTwoNumericTypes(
    const IDataType & first_type,
    const IDataType & second_type,
    TArgs &&... args)
{
#define DISPATCH(FIELDTYPE, DATATYPE)                                                 \
    if (typeid_cast<const DATATYPE *>(&first_type))                                   \
        return createWithTwoNumericTypesSecond<FIELDTYPE, AggregateFunctionTemplate>( \
            second_type,                                                              \
            std::forward<TArgs>(args)...);
    FOR_NUMERIC_TYPES_AND_ENUMS(DISPATCH)
#undef DISPATCH
    return nullptr;
}

} // namespace DB
