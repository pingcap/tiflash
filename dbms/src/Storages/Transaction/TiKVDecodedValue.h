#pragma once

#include <Storages/Transaction/Codec.h>

namespace DB
{
class Field;

struct DecodedRowElement
{
    const Int64 col_id;
    const Field field;

    DecodedRowElement(const Int64 col_id_, Field && field_) : col_id(col_id_), field(std::move(field_)) {}

    bool operator<(const DecodedRowElement & e) const { return col_id < e.col_id; }
};

using DecodedRow = std::vector<DecodedRowElement>;

template <bool is_key = false>
struct ValueExtraInfo
{
    ~ValueExtraInfo()
    {
        auto ptr = decoded.load();
        if (ptr)
        {
            auto decoded_ptr = reinterpret_cast<DecodedRow *>(ptr);
            delete decoded_ptr;
            decoded = nullptr;
        }
    }

    const DecodedRow * load() const { return reinterpret_cast<DecodedRow *>(decoded.load()); }

    void atomicUpdate(DecodedRow *& data) const
    {
        static void * expected = nullptr;
        if (!decoded.compare_exchange_strong(expected, (void *)data))
            delete data;
        data = nullptr;
    }

    static DecodedRow * computeDecodedRow(const std::string & raw_value)
    {
        size_t cursor = 0;
        DecodedRow decoded_row;

        while (cursor < raw_value.size())
        {
            Field f = DecodeDatum(cursor, raw_value);
            if (f.isNull())
                break;
            ColumnID col_id = f.get<ColumnID>();
            decoded_row.emplace_back(col_id, DecodeDatum(cursor, raw_value));
        }

        if (cursor != raw_value.size())
            throw Exception("ComputeDecodedRow cursor is not end in ", ErrorCodes::LOGICAL_ERROR);

        DecodedRow * res = new DecodedRow(std::move(decoded_row));
        return res;
    }

    ValueExtraInfo() = default;

private:
    ValueExtraInfo(const ValueExtraInfo &) = delete;
    ValueExtraInfo(ValueExtraInfo &&) = delete;

private:
    mutable std::atomic<void *> decoded{nullptr};
};

template <>
struct ValueExtraInfo<true>
{
};

} // namespace DB
