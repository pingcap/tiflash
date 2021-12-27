#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/toSafeUnsigned.h>
#include <Common/typeid_cast.h>
#include <Core/AccurateComparison.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <common/intExp.h>

#include <boost/integer/common_factor.hpp>
#include <ext/range.h>


namespace DB
{
/// Used to indicate undefined operation
struct InvalidType;

template <typename T>
struct DataTypeFromFieldType
{
    using Type = DataTypeNumber<T>;
};

template <>
struct DataTypeFromFieldType<NumberTraits::Error>
{
    using Type = InvalidType;
};

template <typename T>
struct DataTypeFromFieldType<Decimal<T>>
{
    using Type = DataTypeDecimal<T>;
};

} // namespace DB