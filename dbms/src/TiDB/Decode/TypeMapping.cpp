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

#include <Common/Exception.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Poco/StringTokenizer.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>

#include <magic_enum.hpp>
#include <memory>
#include <type_traits>

namespace DB
{
using TiDB::ColumnInfo;

class TypeMapping : public ext::Singleton<TypeMapping>
{
public:
    using Creator = std::function<DataTypePtr(const ColumnInfo & column_info)>;
    using TypeMap = std::unordered_map<TiDB::TP, Creator>;

    DataTypePtr getDataType(const ColumnInfo & column_info);

private:
    TypeMapping();

    TypeMap type_map;

    friend class ext::Singleton<TypeMapping>;
};

template <typename T>
struct SignedType : public std::false_type
{
    using UnsignedType = T;
};
template <>
struct SignedType<DataTypeInt8> : public std::true_type
{
    using UnsignedType = DataTypeUInt8;
};
template <>
struct SignedType<DataTypeInt16> : public std::true_type
{
    using UnsignedType = DataTypeUInt16;
};
template <>
struct SignedType<DataTypeInt32> : public std::true_type
{
    using UnsignedType = DataTypeUInt32;
};
template <>
struct SignedType<DataTypeInt64> : public std::true_type
{
    using UnsignedType = DataTypeUInt64;
};
template <typename T>
inline constexpr bool IsSignedType = SignedType<T>::value;

template <typename T>
struct DecimalType : public std::false_type
{
};
template <typename T>
struct DecimalType<DataTypeDecimal<T>> : public std::true_type
{
};
template <typename T>
inline constexpr bool IsDecimalType = DecimalType<T>::value;

template <typename T>
struct EnumType : public std::false_type
{
};
template <>
struct EnumType<DataTypeEnum16> : public std::true_type
{
};
template <typename T>
inline constexpr bool IsEnumType = EnumType<T>::value;

template <typename T>
struct ArrayType : public std::false_type
{
};
template <>
struct ArrayType<DataTypeArray> : public std::true_type
{
};
template <typename T>
inline constexpr bool IsArrayType = ArrayType<T>::value;

template <typename T>
std::enable_if_t<
    !IsSignedType<T> && !IsDecimalType<T> && !IsEnumType<T> && !std::is_same_v<T, DataTypeMyDateTime>
        && !IsArrayType<T>,
    DataTypePtr> //
getDataTypeByColumnInfoBase(const ColumnInfo &, const T *)
{
    return std::make_shared<T>();
}

template <typename T>
std::enable_if_t<IsSignedType<T>, DataTypePtr> getDataTypeByColumnInfoBase(const ColumnInfo & column_info, const T *)
{
    DataTypePtr t = nullptr;

    if (column_info.hasUnsignedFlag())
        t = std::make_shared<typename SignedType<T>::UnsignedType>();
    else
        t = std::make_shared<T>();

    return t;
}

template <typename T>
std::enable_if_t<IsDecimalType<T>, DataTypePtr> getDataTypeByColumnInfoBase(const ColumnInfo & column_info, const T *)
{
    return createDecimal(column_info.flen, column_info.decimal);
}

template <typename T>
std::enable_if_t<IsArrayType<T>, DataTypePtr> getDataTypeByColumnInfoBase(const ColumnInfo & column_info, const T *)
{
    RUNTIME_CHECK(column_info.tp == TiDB::TypeTiDBVectorFloat32, magic_enum::enum_name(column_info.tp));
    const auto nested_type = std::make_shared<DataTypeFloat32>();
    return std::make_shared<DataTypeArray>(nested_type);
}

template <typename T>
std::enable_if_t<std::is_same_v<T, DataTypeMyDateTime>, DataTypePtr> //
getDataTypeByColumnInfoBase(const ColumnInfo & column_info, const T *)
{
    // In some cases, TiDB will set the decimal to -1, change -1 to 6 to avoid error
    return std::make_shared<T>(column_info.decimal == -1 ? 6 : column_info.decimal);
}

template <typename T>
std::enable_if_t<IsEnumType<T>, DataTypePtr> getDataTypeByColumnInfoBase(const ColumnInfo & column_info, const T *)
{
    return std::make_shared<T>(column_info.elems);
}

TypeMapping::TypeMapping()
{
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct)                                                                        \
    type_map[TiDB::Type##tt] = [](const ColumnInfo & column_info) {                             \
        return getDataTypeByColumnInfoBase<DataType##ct>(column_info, (DataType##ct *)nullptr); \
    };
    COLUMN_TYPES(M)
#undef M
}

// Get the basic data type according to column_info.
// This method ignores the nullable flag.
DataTypePtr TypeMapping::getDataType(const ColumnInfo & column_info)
{
    auto iter = type_map.find(column_info.tp);
    RUNTIME_CHECK_MSG(
        iter != type_map.end(),
        "Invalid type from column info, column_id={} tp={} flag={}",
        column_info.id,
        fmt::underlying(column_info.tp),
        column_info.flag);
    return (iter->second)(column_info);
}

// Get the data type according to column_info, respecting
// the nullable flag.
// This does not support the "duration" type.
DataTypePtr getDataTypeByColumnInfo(const ColumnInfo & column_info)
{
    DataTypePtr base = TypeMapping::instance().getDataType(column_info);

    if (!column_info.hasNotNullFlag())
    {
        return std::make_shared<DataTypeNullable>(base);
    }
    return base;
}

// Get the data type according to column_info.
// This support the duration type that only will be generated when executing
DataTypePtr getDataTypeByColumnInfoForComputingLayer(const ColumnInfo & column_info)
{
    DataTypePtr base = TypeMapping::instance().getDataType(column_info);

    if (column_info.tp == TiDB::TypeTime)
    {
        base = std::make_shared<DataTypeMyDuration>(column_info.decimal);
    }
    if (!column_info.hasNotNullFlag())
    {
        return std::make_shared<DataTypeNullable>(base);
    }
    return base;
}

DataTypePtr getDataTypeByColumnInfoForDisaggregatedStorageLayer(const ColumnInfo & column_info)
{
    DataTypePtr base = TypeMapping::instance().getDataType(column_info);
    if (!column_info.hasNotNullFlag())
    {
        return std::make_shared<DataTypeNullable>(base);
    }
    return base;
}

DataTypePtr getDataTypeByFieldType(const tipb::FieldType & field_type)
{
    ColumnInfo ci = TiDB::fieldTypeToColumnInfo(field_type);
    return getDataTypeByColumnInfo(ci);
}

DataTypePtr getDataTypeByFieldTypeForComputingLayer(const tipb::FieldType & field_type)
{
    ColumnInfo ci = TiDB::fieldTypeToColumnInfo(field_type);
    return getDataTypeByColumnInfoForComputingLayer(ci);
}

TiDB::CodecFlag getCodecFlagByFieldType(const tipb::FieldType & field_type)
{
    ColumnInfo ci = TiDB::fieldTypeToColumnInfo(field_type);
    return ci.getCodecFlag();
}

template <typename T>
void setDecimalPrecScale(const T * decimal_type, ColumnInfo & column_info)
{
    column_info.flen = decimal_type->getPrec();
    column_info.decimal = decimal_type->getScale();
}

void fillTiDBColumnInfo(const String & family_name, const ASTPtr & parameters, ColumnInfo & column_info);
void fillTiDBColumnInfo(const ASTPtr & type, ColumnInfo & column_info)
{
    const auto * func = typeid_cast<const ASTFunction *>(type.get());
    if (func != nullptr)
        return fillTiDBColumnInfo(func->name, func->arguments, column_info);
    const auto * ident = typeid_cast<const ASTIdentifier *>(type.get());
    if (ident != nullptr)
        return fillTiDBColumnInfo(ident->name, {}, column_info);
    throw Exception("Failed to get TiDB data type");
}

void fillTiDBColumnInfo(const String & family_name, const ASTPtr & parameters, ColumnInfo & column_info)
{
    if (family_name == "Nullable")
    {
        if (!parameters || parameters->children.size() != 1)
            throw Exception("Nullable data type must have exactly one argument - nested type");
        fillTiDBColumnInfo(parameters->children[0], column_info);
        column_info.clearNotNullFlag();
        return;
    }

    const static std::unordered_map<String, TiDB::TP> tidb_type_map({
        {"timestamp", TiDB::TypeTimestamp},
        {"time", TiDB::TypeTime},
        {"set", TiDB::TypeSet},
        {"year", TiDB::TypeYear},
        {"bit", TiDB::TypeBit},
    });

    auto it = tidb_type_map.find(Poco::toLower(family_name));
    if (it == tidb_type_map.end())
        throw Exception("Unknown TiDB data type " + family_name);
    column_info.setNotNullFlag();
    column_info.tp = it->second;
    int val = 1;

    switch (column_info.tp)
    {
    case TiDB::TypeTimestamp:
    case TiDB::TypeTime:
        if (!parameters)
            column_info.decimal = 0;
        else
        {
            if (parameters->children.size() != 1)
                throw Exception("TimeStamp/Time type can optionally have only one argument - fractional");
            column_info.decimal = typeid_cast<const ASTLiteral *>(parameters->children[0].get())->value.get<int>();
        }
        break;
    case TiDB::TypeSet:
        if (!parameters)
            throw Exception("Set type must have arguments");
        for (auto & ele : parameters->children)
        {
            column_info.elems.emplace_back(typeid_cast<const ASTLiteral *>(ele.get())->value.get<String>(), val);
            val++;
        }
        column_info.setUnsignedFlag();
        break;
    case TiDB::TypeBit:
        if (!parameters)
            column_info.flen = 1;
        else
        {
            if (parameters->children.size() != 1)
                throw Exception("Bit type can optionally have only one argument");
            column_info.flen = typeid_cast<const ASTLiteral *>(parameters->children[0].get())->value.get<int>();
        }
        column_info.setUnsignedFlag();
        break;
    default:
        break;
    }
}

// This is a hack to create TiDB type that can not get from a TiFlash type
// the rule is use `col_name default value` to declare a TiFlash column
// and if the default value is a string and starts with "asTiDBType", then
// we extract TiDB type from the default value and in this case the default
// value should be formatted as "asTiDBType|tidbType[|defaultValue]"
bool hijackTiDBTypeForMockTest(const Field & default_value, ColumnInfo & column_info)
{
    if (!default_value.isNull() && default_value.getType() == Field::Types::String)
    {
        const auto & value = default_value.get<String>();
        Poco::StringTokenizer st(value, "|");
        if (st[0] == "asTiDBType")
        {
            ParserIdentifierWithOptionalParameters type_parser;
            Tokens tokens(st[1].data(), st[1].data() + st[1].length());
            TokenIterator pos(tokens);
            Expected expected;
            ASTPtr type;
            if (!type_parser.parse(pos, type, expected))
                throw Exception("Invalid TiDB data type");
            fillTiDBColumnInfo(type, column_info);
            if (st.count() >= 3)
                column_info.origin_default_value = st[2];
            return true;
        }
        else
            return false;
    }
    else
        return false;
}

ColumnInfo reverseGetColumnInfo(const NameAndTypePair & column, ColumnID id, const Field & default_value, bool for_test)
{
    ColumnInfo column_info;
    column_info.id = id;
    column_info.name = column.name;
    if (for_test && hijackTiDBTypeForMockTest(default_value, column_info))
        return column_info;
    const IDataType * nested_type = column.type.get();

    // Fill not null.
    if (!column.type->isNullable())
    {
        column_info.setNotNullFlag();
    }
    else
    {
        const auto * nullable_type = checkAndGetDataType<DataTypeNullable>(nested_type);
        nested_type = nullable_type->getNestedType().get();
    }

    // Fill tp.
    switch (nested_type->getTypeId())
    {
    case TypeIndex::Nothing:
        column_info.tp = TiDB::TypeNull;
        break;
    case TypeIndex::UInt8:
    case TypeIndex::Int8:
        column_info.tp = TiDB::TypeTiny;
        break;
    case TypeIndex::UInt16:
    case TypeIndex::Int16:
        column_info.tp = TiDB::TypeShort;
        break;
    case TypeIndex::UInt32:
    case TypeIndex::Int32:
        column_info.tp = TiDB::TypeLong;
        break;
    case TypeIndex::UInt64:
    case TypeIndex::Int64:
        column_info.tp = TiDB::TypeLongLong;
        break;
    case TypeIndex::Float32:
        column_info.tp = TiDB::TypeFloat;
        break;
    case TypeIndex::Float64:
        column_info.tp = TiDB::TypeDouble;
        break;
    case TypeIndex::Date:
    case TypeIndex::MyDate:
        column_info.tp = TiDB::TypeDate;
        break;
    case TypeIndex::DateTime:
    case TypeIndex::MyDateTime:
        column_info.tp = TiDB::TypeDatetime;
        break;
    case TypeIndex::MyTimeStamp:
        column_info.tp = TiDB::TypeTimestamp;
        break;
    case TypeIndex::MyTime:
        column_info.tp = TiDB::TypeTime;
        break;
    case TypeIndex::String:
    case TypeIndex::FixedString:
        column_info.tp = TiDB::TypeString;
        break;
    case TypeIndex::Decimal32:
    case TypeIndex::Decimal64:
    case TypeIndex::Decimal128:
    case TypeIndex::Decimal256:
        column_info.tp = TiDB::TypeNewDecimal;
        break;
    case TypeIndex::Enum8:
    case TypeIndex::Enum16:
        column_info.tp = TiDB::TypeEnum;
        break;
    case TypeIndex::Array:
        column_info.tp = TiDB::TypeTiDBVectorFloat32;
        break;
    default:
        throw DB::Exception(
            "Unable reverse map TiFlash type " + nested_type->getName() + " to TiDB type",
            ErrorCodes::LOGICAL_ERROR);
    }

    // Fill unsigned flag.
    if (nested_type->isUnsignedInteger())
        column_info.setUnsignedFlag();

    // Fill flen and decimal for decimal.
    if (const auto * decimal_type32 = checkAndGetDataType<DataTypeDecimal<Decimal32>>(nested_type))
        setDecimalPrecScale(decimal_type32, column_info);
    else if (const auto * decimal_type64 = checkAndGetDataType<DataTypeDecimal<Decimal64>>(nested_type))
        setDecimalPrecScale(decimal_type64, column_info);
    else if (const auto * decimal_type128 = checkAndGetDataType<DataTypeDecimal<Decimal128>>(nested_type))
        setDecimalPrecScale(decimal_type128, column_info);
    else if (const auto * decimal_type256 = checkAndGetDataType<DataTypeDecimal<Decimal256>>(nested_type))
        setDecimalPrecScale(decimal_type256, column_info);

    // Fill decimal for date time.
    if (const auto * type = checkAndGetDataType<DataTypeMyDateTime>(nested_type))
        column_info.decimal = type->getFraction();

    // Fill decimal for duration.
    if (const auto * type = checkAndGetDataType<DataTypeMyDuration>(nested_type))
        column_info.decimal = type->getFsp();

    // Fill elems for enum.
    if (checkDataType<DataTypeEnum16>(nested_type))
    {
        const auto * enum16_type = checkAndGetDataType<DataTypeEnum16>(nested_type);
        for (const auto & element : enum16_type->getValues())
        {
            column_info.elems.emplace_back(element.first, element.second);
        }
    }

    // Fill default value, currently we only support int.
    if (!default_value.isNull())
        // convert any type to string , this is TiDB's style.
        column_info.origin_default_value = applyVisitor(FieldVisitorToString(), default_value);
    else
        column_info.setNoDefaultValueFlag();

    return column_info;
}

} // namespace DB
