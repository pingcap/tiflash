#include <type_traits>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
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
template <>
struct DecimalType<DataTypeDecimal> : public std::true_type
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
    DataTypePtr t = std::make_shared<T>(column_info.flen, column_info.decimal);

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

} // namespace DB
