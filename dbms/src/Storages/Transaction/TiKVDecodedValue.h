#pragma once

#include <Storages/Transaction/Codec.h>

namespace DB
{
class Field;
struct DecodedRowElement;
using DecodedRow = std::vector<DecodedRowElement>;

struct DecodedRowElement : boost::noncopyable
{
    Int64 col_id;
    Field field;

    DecodedRowElement & operator=(DecodedRowElement && e)
    {
        if (this == &e)
            return *this;
        col_id = e.col_id;
        field = std::move(e.field);
        return *this;
    }
    DecodedRowElement(DecodedRowElement && e) : col_id(e.col_id), field(std::move(e.field)) {}
    DecodedRowElement(const Int64 col_id_, Field && field_) : col_id(col_id_), field(std::move(field_)) {}

    bool operator<(const DecodedRowElement & e) const { return col_id < e.col_id; }
    DecodedRow::const_iterator findByColumnID(const DecodedRow & row) const
    {
        auto it = std::lower_bound(row.cbegin(), row.cend(), *this);
        if (it != row.cend() && it->col_id == col_id)
            return it;
        return row.cend();
    }
};

/// force decode tikv value into row by a specific schema, if there is data can't be decoded, store it in extra.
struct DecodedRowBySchema : boost::noncopyable
{
    struct UnknownData
    {
        // for new way that TiDB encode column, there is no codec flag
        // if type is unknown, field is string.
        // should be sorted by column id.
        const DecodedRow row;
        const bool known_type;
    };

    DecodedRowBySchema(Int64 decode_schema_version_, bool has_dropped_column_, DecodedRow && row_, DecodedRow && extra_, bool known_type)
        : decode_schema_version(decode_schema_version_),
          has_dropped_column(has_dropped_column_),
          row(std::move(row_)),
          unknown_data{std::move(extra_), known_type}
    {
        std::ignore = decode_schema_version;
        if (!isSortedByColumnID(row) || !isSortedByColumnID(unknown_data.row))
            throw Exception(std::string(__PRETTY_FUNCTION__) + ": should be sorted by column id", ErrorCodes::LOGICAL_ERROR);
    }

private:
    static bool isSortedByColumnID(const DecodedRow & decoded_row)
    {
        for (size_t i = 1; i < decoded_row.size(); ++i)
        {
            if (decoded_row[i - 1].col_id >= decoded_row[i].col_id)
                return false;
        }
        return true;
    }

private:
    // schema version used while pre-decode.
    const Int64 decode_schema_version;

public:
    // if decoded row doesn't contain column in schema.
    const bool has_dropped_column;
    // decoded column in schema and default/null column. should be sorted by column id.
    const DecodedRow row;
    // decoded column not in schema
    const UnknownData unknown_data;
};

} // namespace DB
