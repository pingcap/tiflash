#include <Storages/Transaction/RowCodec.h>

namespace DB
{

enum struct RowCodecVer : UInt8
{
    ROW_V2 = 128,
};

bool hasMissingColumns(const DecodedFields & decoded_fields, const TableInfo & table_info, const ColumnIdToIndex & column_lut)
{
    for (const auto & id_to_idx : column_lut)
    {
        const auto & column_info = table_info.columns[id_to_idx.second];
        if (auto it = findByColumnID(column_info.id, decoded_fields); it != decoded_fields.end())
            continue;
        // We consider a missing column could be safely filled with NULL, unless it has not default value and is NOT NULL.
        // This could saves lots of unnecessary schema syncs for old data with a schema that has newly added columns.
        if (column_info.hasNoDefaultValueFlag() && column_info.hasNotNullFlag())
            return true;
    }
    return false;
}

DecodedRow * decodeRowV1(const TiKVValue::Base & raw_value, const TableInfo & table_info, const ColumnIdToIndex & column_lut)
{
    /// Decode fields based on codec flag, fill into decoded_fields.
    DecodedFields decoded_fields;
    {
        size_t cursor = 0;

        while (cursor < raw_value.size())
        {
            Field f = DecodeDatum(cursor, raw_value);
            if (f.isNull())
                break;
            ColumnID col_id = f.get<ColumnID>();
            decoded_fields.emplace_back(col_id, DecodeDatum(cursor, raw_value));
        }

        if (cursor != raw_value.size())
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": cursor is not end, remaining: " + raw_value.substr(cursor),
                ErrorCodes::LOGICAL_ERROR);

        {
            // must be sorted, for binary search.
            ::std::sort(decoded_fields.begin(), decoded_fields.end());
        }
    }

    /// Analyze schema, fill unknown_fields and has_missing_columns.
    DecodedFields unknown_fields;
    bool has_missing_columns = false;
    {
        bool schema_match = decoded_fields.size() == column_lut.size()
            && std::all_of(decoded_fields.cbegin(), decoded_fields.cend(), [&column_lut](const auto & field) {
                   return column_lut.count(field.col_id);
               });

        if (!schema_match)
        {
            DecodedFields tmp;
            tmp.reserve(decoded_fields.size());
            for (auto && item : decoded_fields)
            {
                if (column_lut.count(item.col_id))
                    tmp.emplace_back(std::move(item));
                else
                    unknown_fields.emplace_back(std::move(item));
            }
            tmp.swap(decoded_fields);
        }

        has_missing_columns = hasMissingColumns(decoded_fields, table_info, column_lut);
    }

    /// Pack them all and return.
    return new DecodedRow(has_missing_columns, std::move(unknown_fields), true, std::move(decoded_fields));
}

template <typename T>
static T decodeUInt(size_t & cursor, const TiKVValue::Base & raw_value)
{
    T res = readLittleEndian<T>(&raw_value[cursor]);
    cursor += sizeof(T);
    return res;
}

template <typename Target, typename Source>
static void decodeUInts(size_t & cursor, const TiKVValue::Base & raw_value, size_t n, std::vector<Target> & target)
{
    target.reserve(n);
    for (size_t i = 0; i < n; i++)
        target.emplace_back(decodeUInt<Source>(cursor, raw_value));
}

template <typename Sign, typename Source>
static Sign castIntWithSign(Source i)
{
    if constexpr (std::is_signed_v<Sign>)
        return static_cast<Sign>(static_cast<std::make_signed_t<Source>>(i));
    else
        return static_cast<Sign>(static_cast<std::make_unsigned_t<Source>>(i));
}

template <typename T>
static T decodeIntWithLength(const TiKVValue::Base & raw_value, size_t pos, size_t length)
{
    switch (length)
    {
        case sizeof(UInt8):
            return castIntWithSign<T>(decodeUInt<UInt8>(pos, raw_value));
        case sizeof(UInt16):
            return castIntWithSign<T>(decodeUInt<UInt16>(pos, raw_value));
        case sizeof(UInt32):
            return castIntWithSign<T>(decodeUInt<UInt32>(pos, raw_value));
        case sizeof(UInt64):
            return castIntWithSign<T>(decodeUInt<UInt64>(pos, raw_value));
        default:
            throw Exception(std::string("Invalid integer length ") + std::to_string(length) + " at position " + std::to_string(pos)
                + " in value: " + raw_value);
    }
}

struct RowV2
{
    static constexpr UInt8 BigRowMask = 0x1;

    RowV2(const TiKVValue::Base & raw_value_, const TableInfo & table_info_, const ColumnIdToIndex & column_lut_)
        : raw_value(raw_value_), table_info(table_info_), column_lut(column_lut_)
    {
        size_t cursor = 1; // Skip the initial codec ver.
        UInt8 row_flag = decodeUInt<UInt8>(cursor, raw_value);
        bool is_big_row = row_flag & RowV2::BigRowMask;
        num_not_null_columns = decodeUInt<UInt16>(cursor, raw_value);
        num_null_columns = decodeUInt<UInt16>(cursor, raw_value);
        if (is_big_row)
        {
            decodeUInts<ColumnID, UInt32>(cursor, raw_value, num_not_null_columns, not_null_column_ids);
            decodeUInts<ColumnID, UInt32>(cursor, raw_value, num_null_columns, null_column_ids);
            decodeUInts<size_t, UInt32>(cursor, raw_value, num_not_null_columns, value_offsets);
        }
        else
        {
            decodeUInts<ColumnID, UInt8>(cursor, raw_value, num_not_null_columns, not_null_column_ids);
            decodeUInts<ColumnID, UInt8>(cursor, raw_value, num_null_columns, null_column_ids);
            decodeUInts<size_t, UInt16>(cursor, raw_value, num_not_null_columns, value_offsets);
        }
        // Values starts from current cursor.
        values_start_pos = cursor;
    }

    DecodedRow * decode()
    {
        DecodedFields decoded_fields, unknown_fields;
        size_t id_not_null = 0, id_null = 0;
        // Merge ordered not null/null columns to keep order.
        while (id_not_null < not_null_column_ids.size() || id_null < null_column_ids.size())
        {
            bool is_null;
            if (id_not_null < not_null_column_ids.size() && id_null < null_column_ids.size())
                is_null = not_null_column_ids[id_not_null] > null_column_ids[id_null];
            else
                is_null = id_null < null_column_ids.size();

            auto res = is_null ? decodeNullColumn(id_null++) : decodeNotNullColumn(id_not_null++);
            if (std::get<1>(res))
                unknown_fields.emplace_back(std::move(std::get<0>(res)));
            else
                decoded_fields.emplace_back(std::move(std::get<0>(res)));
        }

        bool has_missing_columns = hasMissingColumns(decoded_fields, table_info, column_lut);
        return new DecodedRow(has_missing_columns, std::move(unknown_fields), false, std::move(decoded_fields));
    }

private:
    std::tuple<DecodedField, bool> decodeNullColumn(size_t id)
    {
        return std::make_tuple(DecodedField{null_column_ids[id], Field()}, column_lut.count(null_column_ids[id]));
    }

    std::tuple<DecodedField, bool> decodeNotNullColumn(size_t id)
    {
        ColumnID column_id = not_null_column_ids[id];
        size_t start = id ? value_offsets[id - 1] : 0;
        size_t length = value_offsets[id] - start;

        if (!column_lut.count(column_id))
            // Unknown column, decode as string.
            return std::make_tuple(DecodedField{column_id, raw_value.substr(values_start_pos + start, length)}, true);

        const auto & column_info = table_info.columns[column_lut.find(column_id)->second];
        size_t cursor = values_start_pos + start;
        switch (column_info.tp)
        {
            case TiDB::TypeLongLong:
            case TiDB::TypeLong:
            case TiDB::TypeInt24:
            case TiDB::TypeShort:
            case TiDB::TypeTiny:
            case TiDB::TypeYear:
                if (column_info.hasUnsignedFlag())
                    return std::make_tuple(DecodedField{column_id, decodeIntWithLength<UInt64>(raw_value, cursor, length)}, false);
                else
                    return std::make_tuple(DecodedField{column_id, decodeIntWithLength<Int64>(raw_value, cursor, length)}, false);
            case TiDB::TypeFloat:
            case TiDB::TypeDouble:
                return std::make_tuple(DecodedField{column_id, DecodeFloat64(cursor, raw_value)}, false);
            case TiDB::TypeVarString:
            case TiDB::TypeVarchar:
            case TiDB::TypeString:
            case TiDB::TypeBlob:
            case TiDB::TypeTinyBlob:
            case TiDB::TypeMediumBlob:
            case TiDB::TypeLongBlob:
            case TiDB::TypeJSON: // JSON portion could be quickly navigated by length, instead of doing real decoding.
                return std::make_tuple(DecodedField{column_id, raw_value.substr(cursor, length)}, false);
            case TiDB::TypeNewDecimal:
                return std::make_tuple(DecodedField{column_id, DecodeDecimal(cursor, raw_value)}, false);
            case TiDB::TypeTimestamp:
            case TiDB::TypeDate:
            case TiDB::TypeDatetime:
            case TiDB::TypeBit:
            case TiDB::TypeSet:
            case TiDB::TypeEnum:
                return std::make_tuple(DecodedField{column_id, decodeIntWithLength<UInt64>(raw_value, cursor, length)}, false);
            case TiDB::TypeTime:
                return std::make_tuple(DecodedField{column_id, decodeIntWithLength<Int64>(raw_value, cursor, length)}, false);
            default:
                throw Exception(std::string("Invalid TP ") + std::to_string(column_info.tp) + " of column " + column_info.name);
        }
    }

private:
    const TiKVValue::Base & raw_value;
    const TableInfo & table_info;
    const ColumnIdToIndex & column_lut;

    size_t num_not_null_columns;
    size_t num_null_columns;
    std::vector<ColumnID> not_null_column_ids;
    std::vector<ColumnID> null_column_ids;
    std::vector<size_t> value_offsets;
    size_t values_start_pos = 0;
};

DecodedRow * decodeRowV2(const TiKVValue::Base & raw_value, const TableInfo & table_info, const ColumnIdToIndex & column_lut)
{
    return RowV2(raw_value, table_info, column_lut).decode();
}

DecodedRow * decodeRow(const TiKVValue::Base & raw_value, const TableInfo & table_info, const ColumnIdToIndex & column_lut)
{
    switch (static_cast<UInt8>(raw_value[0]))
    {
        case static_cast<UInt8>(RowCodecVer::ROW_V2):
            return decodeRowV2(raw_value, table_info, column_lut);
        default:
            return decodeRowV1(raw_value, table_info, column_lut);
    }
}

} // namespace DB
