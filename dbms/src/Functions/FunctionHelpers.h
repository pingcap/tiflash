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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>

#include <memory>


namespace DB
{
/// Methods, that helps dispatching over real column types.

template <typename Type>
const Type * checkAndGetDataType(const IDataType * data_type)
{
    return typeid_cast<const Type *>(data_type);
}

template <typename Type>
bool checkDataType(const IDataType * data_type)
{
    return checkAndGetDataType<Type>(data_type);
}

template <typename InnerType>
bool checkDataTypeArray(const IDataType * data_type)
{
    const auto * array_type = checkAndGetDataType<DataTypeArray>(data_type);
    if unlikely (!array_type)
        return false;

    const DataTypePtr & inner_type = array_type->getNestedType();
    return checkDataType<InnerType>(inner_type.get());
}

template <typename Type>
const Type * checkAndGetColumn(const IColumn * column)
{
    return typeid_cast<const Type *>(column);
}

template <typename Type>
bool checkColumn(const IColumn * column)
{
    return checkAndGetColumn<Type>(column);
}

template <typename Type>
const Type * checkAndGetNestedColumn(const IColumn * column)
{
    if (!column || !column->isColumnNullable())
        return {};

    const auto * data_column = &static_cast<const ColumnNullable *>(column)->getNestedColumn();

    return checkAndGetColumn<Type>(data_column);
}

template <typename Type>
const ColumnConst * checkAndGetColumnConst(const IColumn * column, bool maybe_nullable_column = false)
{
    if (!column || !column->isColumnConst())
        return {};

    const auto * res = static_cast<const ColumnConst *>(column);

    const auto * data_column = &res->getDataColumn();
    if (maybe_nullable_column && data_column->isColumnNullable())
        data_column = &typeid_cast<const ColumnNullable *>(data_column)->getNestedColumn();
    if (!checkColumn<Type>(data_column))
        return {};

    return res;
}

template <typename Type>
const Type * checkAndGetColumnConstData(const IColumn * column)
{
    const ColumnConst * res = checkAndGetColumnConst<Type>(column);

    if (!res)
        return {};

    return static_cast<const Type *>(&res->getDataColumn());
}

template <typename Type>
bool checkColumnConst(const IColumn * column)
{
    return checkAndGetColumnConst<Type>(column);
}


/// Returns non-nullptr if column is ColumnConst with ColumnString or ColumnFixedString inside.
const ColumnConst * checkAndGetColumnConstStringOrFixedString(const IColumn * column);


/// Transform anything to Field.
template <typename T>
inline std::enable_if_t<!IsDecimal<T>, Field> toField(const T & x)
{
    return Field(typename NearestFieldType<T>::Type(x));
}

template <typename T>
inline std::enable_if_t<IsDecimal<T>, Field> toField(const T & x, UInt32 scale)
{
    return DecimalField<T>(x, scale);
}

Columns convertConstTupleToConstantElements(const ColumnConst & column);


/// Returns the copy of a given block in which each column specified in
/// the "arguments" parameter is replaced with its respective nested
/// column if it is nullable.
Block createBlockWithNestedColumns(const Block & block, const ColumnNumbers & args);

/// Similar function as above. Additionally transform the result type if needed.
Block createBlockWithNestedColumns(const Block & block, const ColumnNumbers & args, size_t result);

bool functionIsInOperator(const String & name);

bool functionIsInOrGlobalInOperator(const String & name);


template <typename T>
struct IGetVecHelper
{
    static_assert(std::is_arithmetic_v<T>);
    virtual T get(size_t) const = 0;
    virtual ~IGetVecHelper() = default;
    static std::unique_ptr<IGetVecHelper> getHelper(const ColumnVector<T> * p);
    static std::unique_ptr<IGetVecHelper> getHelper(const ColumnConst * p);
};

template <typename T>
struct GetVecHelper : public IGetVecHelper<T>
{
    explicit GetVecHelper(const ColumnVector<T> * p_)
        : p(p_)
    {}
    T get(size_t i) const override { return p->getElement(i); }

private:
    const ColumnVector<T> * p;
};

template <typename T>
struct GetConstVecHelper : public IGetVecHelper<T>
{
    explicit GetConstVecHelper(const ColumnConst * p_)
        : value(p_->getValue<T>())
    {}
    T get(size_t) const override { return value; }

private:
    T value;
};

template <typename T>
std::unique_ptr<IGetVecHelper<T>> IGetVecHelper<T>::getHelper(const ColumnVector<T> * p)
{
    return std::unique_ptr<IGetVecHelper<T>>{new GetVecHelper<T>{p}};
}

template <typename T>
std::unique_ptr<IGetVecHelper<T>> IGetVecHelper<T>::getHelper(const ColumnConst * p)
{
    return std::unique_ptr<IGetVecHelper<T>>{new GetConstVecHelper<T>{p}};
}

static Field FIELD_NULL = toField(Null{});
static Field FIELD_INT8_1 = toField(Int8(1));
static Field FIELD_INT8_0 = toField(Int8(0));

} // namespace DB
