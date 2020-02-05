#pragma once

#include <Storages/Transaction/DatumCodec.h>

namespace DB
{
class Field;
struct DecodedField;
using DecodedFields = std::vector<DecodedField>;

struct DecodedField : boost::noncopyable
{
    Int64 col_id;
    Field field;

    DecodedField & operator=(DecodedField && e)
    {
        if (this == &e)
            return *this;
        col_id = e.col_id;
        field = std::move(e.field);
        return *this;
    }
    DecodedField(DecodedField && e) : col_id(e.col_id), field(std::move(e.field)) {}
    DecodedField(const Int64 col_id_, Field && field_) : col_id(col_id_), field(std::move(field_)) {}

    bool operator<(const DecodedField & e) const { return col_id < e.col_id; }
};

/// force decode TiKV value into row by a specific schema, if there is data can't be decoded, store it in unknown_fields.
struct DecodedRow : boost::noncopyable
{
    // In old way, tidb encode each record like: (codec-flag, column-data), (codec-flag, column-data), ...
    // we can use codec-flag to tell type of column. But, in new way, https://github.com/pingcap/tidb/pull/7597,
    // there is no codec-flag, and we should find type in schema by column id.
    struct UnknownFields
    {
        // should be sorted by column id.
        const DecodedFields fields;
        // if there is no codec-flag (in tidb fast codec), field are all string and with_codec_flag is false.
        const bool with_codec_flag;
    };

    DecodedRow(bool has_missing_columns_, DecodedFields && unknown_, bool has_codec_flag, DecodedFields && decoded_fields_)
        : has_missing_columns(has_missing_columns_),
          unknown_fields{std::move(unknown_), has_codec_flag},
          decoded_fields(std::move(decoded_fields_))
    {
        if (!isSortedByColumnID(decoded_fields) || !isSortedByColumnID(unknown_fields.fields))
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": should be sorted by column id", ErrorCodes::LOGICAL_ERROR);
    }

private:
    static bool isSortedByColumnID(const DecodedFields & decoded_fields)
    {
        for (size_t i = 1; i < decoded_fields.size(); ++i)
        {
            if (decoded_fields[i - 1].col_id >= decoded_fields[i].col_id)
                return false;
        }
        return true;
    }

public:
    // if decoded row doesn't contain column in schema.
    const bool has_missing_columns;
    // decoded column not in schema
    const UnknownFields unknown_fields;
    // decoded column in schema and default/null column. should be sorted by column id.
    const DecodedFields decoded_fields;
};

} // namespace DB
