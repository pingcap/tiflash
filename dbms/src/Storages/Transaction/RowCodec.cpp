#include <Storages/Transaction/RowCodec.h>

namespace DB
{

static constexpr UInt8 CodecVer = 128;

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

    /// Analyze schema, fill has_missing_columns and unknown_fields.
    bool has_missing_columns = false;
    DecodedFields unknown_fields;
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

            // must be sorted, for binary search.
            ::std::sort(tmp.begin(), tmp.end());
            ::std::sort(unknown_fields.begin(), unknown_fields.end());
            tmp.swap(decoded_fields);
        }

        for (const auto & id_to_idx : column_lut)
        {
            const auto & column_info = table_info.columns[id_to_idx.second];
            if (auto it = findByColumnID(column_info.id, decoded_fields); it != decoded_fields.end())
                continue;
            if (column_info.hasNoDefaultValueFlag() && column_info.hasNotNullFlag())
            {
                has_missing_columns = true;
                break;
            }
        }
    }

    /// Pack them all and return.
    return new DecodedRow(has_missing_columns, std::move(unknown_fields), true, std::move(decoded_fields));
}

DecodedRow * decodeRowV2(const TiKVValue::Base & /*raw_value*/, const TableInfo & /*table_info*/, const ColumnIdToIndex & /*column_lut*/)
{
    return nullptr;
}

DecodedRow * decodeRow(const TiKVValue::Base & raw_value, const TableInfo & table_info, const ColumnIdToIndex & column_lut)
{
    switch (raw_value[0])
    {
        case CodecVer:
            return decodeRowV1(raw_value, table_info, column_lut);
        default:
            return decodeRowV2(raw_value, table_info, column_lut);
    }
}

} // namespace DB
