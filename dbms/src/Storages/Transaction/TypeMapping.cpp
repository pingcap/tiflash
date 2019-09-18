#include <type_traits>

#include <Common/FieldVisitors.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{

class TypeMapping : public ext::singleton<TypeMapping>
{
public:
    using Creator = std::function<DataTypePtr(const ColumnInfo & column_info)>;
    using TypeMap = std::unordered_map<TiDB::TP, Creator>;

    DataTypePtr getDataType(const ColumnInfo & column_info);

private:
    TypeMapping();

    TypeMap type_map;

    friend class ext::singleton<TypeMapping>;
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

template <typename T, bool should_widen>
std::enable_if_t<!IsSignedType<T> && !IsDecimalType<T> && !IsEnumType<T> && !std::is_same_v<T, DataTypeMyDateTime>, DataTypePtr>
getDataTypeByColumnInfoBase(const ColumnInfo &, const T *)
{
    DataTypePtr t = std::make_shared<T>();

    if (should_widen)
    {
        auto widen = t->widen();
        t.swap(widen);
    }

    return t;
}

template <typename T, bool should_widen>
std::enable_if_t<IsSignedType<T>, DataTypePtr> getDataTypeByColumnInfoBase(const ColumnInfo & column_info, const T *)
{
    DataTypePtr t = nullptr;

    if (column_info.hasUnsignedFlag())
        t = std::make_shared<typename SignedType<T>::UnsignedType>();
    else
        t = std::make_shared<T>();

    if (should_widen)
    {
        auto widen = t->widen();
        t.swap(widen);
    }

    return t;
}

template <typename T, bool should_widen>
std::enable_if_t<IsDecimalType<T>, DataTypePtr> getDataTypeByColumnInfoBase(const ColumnInfo & column_info, const T *)
{
    DataTypePtr t = createDecimal(column_info.flen, column_info.decimal);

    if (should_widen)
    {
        auto widen = t->widen();
        t.swap(widen);
    }

    return t;
}


template <typename T, bool should_widen>
std::enable_if_t<std::is_same_v<T, DataTypeMyDateTime>, DataTypePtr> getDataTypeByColumnInfoBase(const ColumnInfo & column_info, const T *)
{
    DataTypePtr t = std::make_shared<T>(column_info.decimal);

    if (should_widen)
    {
        auto widen = t->widen();
        t.swap(widen);
    }

    return t;
}

template <typename T, bool should_widen>
std::enable_if_t<IsEnumType<T>, DataTypePtr> getDataTypeByColumnInfoBase(const ColumnInfo & column_info, const T *)
{
    DataTypePtr t = std::make_shared<T>(column_info.elems);

    if (should_widen)
    {
        auto widen = t->widen();
        t.swap(widen);
    }

    return t;
}

TypeMapping::TypeMapping()
{
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct, w) \
    type_map[TiDB::Type##tt] = std::bind(getDataTypeByColumnInfoBase<DataType##ct, w>, std::placeholders::_1, (DataType##ct *)nullptr);
    COLUMN_TYPES(M)
#undef M
}

DataTypePtr TypeMapping::getDataType(const ColumnInfo & column_info) { return type_map[column_info.tp](column_info); }

DataTypePtr getDataTypeByColumnInfo(const ColumnInfo & column_info)
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
    ColumnInfo ci;
    ci.tp = static_cast<TiDB::TP>(field_type.tp());
    ci.flag = field_type.flag();
    ci.flen = field_type.flen();
    ci.decimal = field_type.decimal();
    // TODO: Enum's elems?
    return getDataTypeByColumnInfo(ci);
}

TiDB::CodecFlag getCodecFlagByFieldType(const tipb::FieldType & field_type)
{
    ColumnInfo ci;
    ci.tp = static_cast<TiDB::TP>(field_type.tp());
    ci.flag = field_type.flag();
    ci.flen = field_type.flen();
    ci.decimal = field_type.decimal();
    return ci.getCodecFlag();
}

template <typename T>
void setDecimalPrecScale(const T * decimal_type, ColumnInfo & column_info)
{
    column_info.flen = decimal_type->getPrec();
    column_info.decimal = decimal_type->getScale();
}

ColumnInfo reverseGetColumnInfo(const NameAndTypePair & column, ColumnID id, const Field & default_value)
{
    ColumnInfo column_info;
    column_info.id = id;
    column_info.name = column.name;
    const IDataType * nested_type = column.type.get();

    // Fill not null.
    if (!column.type->isNullable())
    {
        column_info.setNotNullFlag();
    }
    else
    {
        auto nullable_type = checkAndGetDataType<DataTypeNullable>(nested_type);
        nested_type = nullable_type->getNestedType().get();
    }

    // Fill tp.
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct, w)                       \
    if (checkDataType<DataType##ct>(nested_type)) \
        column_info.tp = TiDB::Type##tt;          \
    else
    COLUMN_TYPES(M)
#undef M
    if (checkDataType<DataTypeUInt8>(nested_type))
        column_info.tp = TiDB::TypeTiny;
    else if (checkDataType<DataTypeUInt16>(nested_type))
        column_info.tp = TiDB::TypeShort;
    else if (checkDataType<DataTypeUInt32>(nested_type))
        column_info.tp = TiDB::TypeLong;
    else if (checkDataType<DataTypeDecimal<Decimal64>>(nested_type))
        column_info.tp = TiDB::TypeNewDecimal;
    else if (checkDataType<DataTypeDecimal<Decimal128>>(nested_type))
        column_info.tp = TiDB::TypeNewDecimal;
    else if (checkDataType<DataTypeDecimal<Decimal256>>(nested_type))
        column_info.tp = TiDB::TypeNewDecimal;
    else
        throw DB::Exception("Unable reverse map TiFlash type " + nested_type->getName() + " to TiDB type", ErrorCodes::LOGICAL_ERROR);
    // UInt64 is hijacked by the macro expansion, we check it again.
    if (checkDataType<DataTypeUInt64>(nested_type))
        column_info.tp = TiDB::TypeLongLong;

    // Fill unsigned flag.
    if (nested_type->isUnsignedInteger())
    {
        column_info.setUnsignedFlag();
    }

    // Fill flen and decimal for decimal.
    {
        if (auto decimal_type = checkAndGetDataType<DataTypeDecimal<Decimal32>>(nested_type))
            setDecimalPrecScale(decimal_type, column_info);
        if (auto decimal_type = checkAndGetDataType<DataTypeDecimal<Decimal64>>(nested_type))
            setDecimalPrecScale(decimal_type, column_info);
        if (auto decimal_type = checkAndGetDataType<DataTypeDecimal<Decimal128>>(nested_type))
            setDecimalPrecScale(decimal_type, column_info);
        if (auto decimal_type = checkAndGetDataType<DataTypeDecimal<Decimal256>>(nested_type))
            setDecimalPrecScale(decimal_type, column_info);
    }

    // Fill decimal for date time.
    if (auto type = checkAndGetDataType<DataTypeMyDateTime>(nested_type))
    {
        column_info.decimal = type->getFraction();
    }

    // Fill elems for enum.
    if (checkDataType<DataTypeEnum16>(nested_type))
    {
        auto enum16_type = checkAndGetDataType<DataTypeEnum16>(nested_type);
        for (auto & element : enum16_type->getValues())
        {
            column_info.elems.emplace_back(element.first, element.second);
        }
    }

    // Fill default value, currently we only support int.
    if (!default_value.isNull())
    {
        // convert any type to string , this is TiDB's style.
        column_info.origin_default_value = applyVisitor(FieldVisitorToString(), default_value);
    }
    else
    {
        column_info.setNoDefaultValueFlag();
    }

    return column_info;
}

} // namespace DB
