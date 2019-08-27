#include <type_traits>

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/Transaction/TypeMapping.h>
#include <Functions/FunctionHelpers.h>

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
std::enable_if_t<!IsSignedType<T> && !IsDecimalType<T> && !IsEnumType<T>, DataTypePtr> getDataTypeByColumnInfoBase(
    const ColumnInfo &, const T *)
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


namespace
{

template <typename T>
bool getDecimalInfo(const IDataType * type, ColumnInfo & column_info)
{
    using TypeDec = DataTypeDecimal<T>;
    if (auto decimal_type = checkAndGetDataType<TypeDec>(type); decimal_type != nullptr)
    {
        column_info.flen = decimal_type->getPrec();
        column_info.decimal = decimal_type->getScale();
        column_info.tp = TiDB::TypeNewDecimal;
        return true;
    }
    return false;
}

} // namespace

ColumnInfo getColumnInfoByDataType(const DataTypePtr & type)
{
    ColumnInfo col;
    DataTypePtr not_null_type;
    if (const auto * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        not_null_type = type_nullable->getNestedType();
    }
    else
    {
        col.setNotNullFlag();
        not_null_type = type;
    }

    // Use TypeIndex in this PR:
    switch (not_null_type->getTypeId())
    {
        case TypeIndex::Nothing:
            col.tp = TiDB::TypeNull;
            break;

        // UnSigned
        case TypeIndex::UInt8:
            col.setUnsignedFlag();
            col.tp = TiDB::TypeTiny;
            break;
        case TypeIndex::UInt16:
            col.setUnsignedFlag();
            col.tp = TiDB::TypeShort;
            break;
        case TypeIndex::UInt32:
            col.setUnsignedFlag();
            col.tp = TiDB::TypeLong;
            break;
        case TypeIndex::UInt64:
            col.setUnsignedFlag();
            col.tp = TiDB::TypeLongLong;
            break;

        // Signed
        case TypeIndex::Int8:
            col.tp = TiDB::TypeTiny;
            break;
        case TypeIndex::Int16:
            col.tp = TiDB::TypeShort;
            break;
        case TypeIndex::Int32:
            col.tp = TiDB::TypeLong;
            break;
        case TypeIndex::Int64:
            col.tp = TiDB::TypeLongLong;
            break;

        // Floating point types
        case TypeIndex::Float32:
            col.tp = TiDB::TypeFloat;
            break;
        case TypeIndex::Float64:
            col.tp = TiDB::TypeDouble;
            break;

        case TypeIndex::Date:
            col.tp = TiDB::TypeDate;
            break;
        case TypeIndex::DateTime:
            col.tp = TiDB::TypeDatetime;
            break;

        case TypeIndex::String:
            col.tp = TiDB::TypeString;
            break;
        case TypeIndex::FixedString:
            col.tp = TiDB::TypeString;
            break;

        // Decimal
        case TypeIndex::Decimal32:
            getDecimalInfo<Decimal32>(type.get(), col);
            break;
        case TypeIndex::Decimal64:
            getDecimalInfo<Decimal64>(type.get(), col);
            break;
        case TypeIndex::Decimal128:
            getDecimalInfo<Decimal128>(type.get(), col);
            break;
        case TypeIndex::Decimal256:
            getDecimalInfo<Decimal256>(type.get(), col);
            break;

        // Unknown numeric in TiDB
        case TypeIndex::UInt128:
            break;
        case TypeIndex::Int128:
            break;
        case TypeIndex::Int256:
            break;

        // Unkonwn
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
        case TypeIndex::UUID:
        case TypeIndex::Array:
        case TypeIndex::Tuple:
        case TypeIndex::Set:
        case TypeIndex::Interval:
        case TypeIndex::Nullable:
        case TypeIndex::Function:
        case TypeIndex::AggregateFunction:
        case TypeIndex::LowCardinality:
            throw Exception("Unknown TiDB type from " + type->getName(), ErrorCodes::NOT_IMPLEMENTED);
    }
    return col;
}

} // namespace DB
