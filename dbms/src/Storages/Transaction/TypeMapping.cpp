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
template <typename T>
DataTypePtr getDataTypeByColumnInfoBase(const ColumnInfo & /*column_info*/)
{
    return std::make_shared<T>();
}


template <>
DataTypePtr getDataTypeByColumnInfoBase<DataTypeDecimal>(const ColumnInfo & column_info)
{
    return std::make_shared<DataTypeDecimal>(column_info.flen, column_info.decimal);
}


template <>
DataTypePtr getDataTypeByColumnInfoBase<DataTypeEnum16>(const ColumnInfo & column_info)
{
    return std::make_shared<DataTypeEnum16>(column_info.elems);
}


class TypeMapping : public ext::singleton<TypeMapping>
{
public:
    using Creator = std::function<DataTypePtr(const ColumnInfo & column_info)>;
    using TypeMap = std::unordered_map<TiDB::TP, Creator>;
    using CodecFlagMap = std::unordered_map<String, TiDB::CodecFlag>;

    DataTypePtr getSigned(const ColumnInfo & column_info);

    DataTypePtr getUnsigned(const ColumnInfo & column_info);

    TiDB::CodecFlag getCodecFlag(const DataTypePtr & dataTypePtr);

private:
    TypeMapping();

    TypeMap signed_type_map;

    TypeMap unsigned_type_map;

    CodecFlagMap codec_flag_map;

    friend class ext::singleton<TypeMapping>;
};


TypeMapping::TypeMapping()
{
#ifdef M
#error "Please undefine macro M first."
#endif

#define M(tt, v, cf, cfu, ct, ctu)                                                        \
    signed_type_map[TiDB::Type##tt] = getDataTypeByColumnInfoBase<DataType##ct>; \
    unsigned_type_map[TiDB::Type##tt] = getDataTypeByColumnInfoBase<DataType##ctu>; \
    codec_flag_map[#ctu] = TiDB::CodecFlag##cfu; \
    codec_flag_map[#ct] = TiDB::CodecFlag##cf;
    COLUMN_TYPES(M)
#undef M
}


DataTypePtr TypeMapping::getSigned(const ColumnInfo & column_info)
{
    return signed_type_map[column_info.tp](column_info);
}


DataTypePtr TypeMapping::getUnsigned(const ColumnInfo & column_info)
{
    return unsigned_type_map[column_info.tp](column_info);
}

TiDB::CodecFlag TypeMapping::getCodecFlag(const DB::DataTypePtr & dataTypePtr) {
    // fixme: String's CodecFlag will be CodecFlagCompactBytes, which is wrong for Json type
    return codec_flag_map[dataTypePtr->getFamilyName()];
}

TiDB::CodecFlag getCodecFlagByDataType(const DataTypePtr & dataTypePtr) {
    return TypeMapping::instance().getCodecFlag(dataTypePtr);
}

DataTypePtr getDataTypeByColumnInfo(const ColumnInfo & column_info)
{
    DataTypePtr base;

    if (column_info.hasUnsignedFlag())
    {
        base = TypeMapping::instance().getUnsigned(column_info);
    }
    else
    {
        base = TypeMapping::instance().getSigned(column_info);
    }

    if (!column_info.hasNotNullFlag())
    {
        return std::make_shared<DataTypeNullable>(base);
    }

    return base;
}

}
