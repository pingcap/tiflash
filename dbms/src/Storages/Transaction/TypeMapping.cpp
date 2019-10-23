#include <type_traits>

#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
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
    // In some cases, TiDB will set the decimal to -1, change -1 to 6 to avoid error
    DataTypePtr t = std::make_shared<T>(column_info.decimal == -1 ? 6 : column_info.decimal);

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
    const IDataType *nested_type = column.type.get();

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
        default:
            throw DB::Exception("Unable reverse map TiFlash type " + nested_type->getName() + " to TiDB type",
                                ErrorCodes::LOGICAL_ERROR);
    }

    // Fill unsigned flag.
    if (nested_type->isUnsignedInteger())
        column_info.setUnsignedFlag();

    // Fill flen and decimal for decimal.
    if (auto decimal_type32 = checkAndGetDataType<DataTypeDecimal<Decimal32>>(nested_type))
        setDecimalPrecScale(decimal_type32, column_info);
    else if (auto decimal_type64 = checkAndGetDataType<DataTypeDecimal<Decimal64>>(nested_type))
        setDecimalPrecScale(decimal_type64, column_info);
    else if (auto decimal_type128 = checkAndGetDataType<DataTypeDecimal<Decimal128>>(nested_type))
        setDecimalPrecScale(decimal_type128, column_info);
    else if (auto decimal_type256 = checkAndGetDataType<DataTypeDecimal<Decimal256>>(nested_type))
        setDecimalPrecScale(decimal_type256, column_info);

    // Fill decimal for date time.
    if (auto type = checkAndGetDataType<DataTypeMyDateTime>(nested_type))
        column_info.decimal = type->getFraction();

    // Fill elems for enum.
    if (checkDataType<DataTypeEnum16>(nested_type))
    {
        auto enum16_type = checkAndGetDataType<DataTypeEnum16>(nested_type);
        for (auto &element : enum16_type->getValues())
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
