#pragma once
#include <Core/Types.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/RegionRangeKeys.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/TiKVRecordFormat.h>
#include <Storages/Transaction/Types.h>

namespace DB::DM
{
using RowKeyColumns    = ColumnDefines;
using RowKeyColumnsPtr = std::shared_ptr<RowKeyColumns>;
using StringPtr        = std::shared_ptr<String>;

struct RowKeyRange;
String rangeToString(const RowKeyRange & range);

extern const Int64     int_handle_min;
extern const Int64     int_handle_max;
extern const StringPtr int_handle_min_key;
extern const StringPtr int_handle_max_key;
extern const StringPtr empty_string_ptr;

inline int compare(const char * a, size_t a_size, const char * b, size_t b_size)
{
    int res = memcmp(a, b, std::min(a_size, b_size));
    if (res != 0)
        return res;
    return a_size == b_size ? 0 : a_size > b_size ? 1 : -1;
}

inline const String & max(const String & a, const String & b)
{
    return a.compare(b) >= 0 ? a : b;
}

inline const String & min(const String & a, const String & b)
{
    return a.compare(b) < 0 ? a : b;
}

inline const StringPtr max(const StringPtr a, const StringPtr b)
{
    return a->compare(*b) >= 0 ? a : b;
}

inline const StringPtr min(const StringPtr a, const StringPtr b)
{
    return a->compare(*b) < 0 ? a : b;
}

inline const StringRef & max(const StringRef & a, const StringRef & b)
{
    return compare(a.data, a.size, b.data, b.size) >= 0 ? a : b;
}

inline const StringRef & min(const StringRef & a, const StringRef & b)
{
    return compare(a.data, a.size, b.data, b.size) < 0 ? a : b;
}

struct RowKeyValue
{
    bool is_common_handle;
    /// data in RowKeyValue can be nullptr if is_common_handle is false
    const char * data;
    size_t       size;
    Int64        int_value;
    String       toString()
    {
        if (is_common_handle)
            return String(data, size);
        return std::to_string(int_value);
    }
};

inline int compare(const RowKeyValue & a, const RowKeyValue & b)
{
    if (a.is_common_handle)
    {
        return compare(a.data, a.size, b.data, b.size);
    }
    else
    {
        if (a.int_value != b.int_value)
            return a.int_value > b.int_value ? 1 : -1;
        if likely (a.int_value != int_handle_max || (a.data == nullptr && b.data == nullptr))
            return 0;
        bool a_inf = false;
        bool b_inf = false;
        if (a.data != nullptr)
            a_inf = compare(a.data, a.size, int_handle_max_key->data(), int_handle_max_key->size()) == 0;
        if (b.data != nullptr)
            b_inf = compare(b.data, b.size, int_handle_max_key->data(), int_handle_max_key->size()) == 0;
        if (a_inf ^ b_inf)
        {
            return a_inf ? 1 : -1;
        }
        else
        {
            return 0;
        }
    }
}

inline int compare(const StringRef & a, const RowKeyValue & b)
{
    RowKeyValue r_a{true, a.data, a.size, 0};
    return compare(r_a, b);
}

inline int compare(const RowKeyValue & a, const StringRef & b)
{
    RowKeyValue r_b{true, b.data, b.size, 0};
    return compare(a, r_b);
}

inline bool operator<(const RowKeyValue & a, const RowKeyValue & b)
{
    return compare(a, b) < 0;
}

inline bool operator<(const StringRef & a, const RowKeyValue & b)
{
    return compare(a, b) < 0;
}

inline bool operator<(const RowKeyValue & a, const StringRef & b)
{
    return compare(a, b) < 0;
}

inline int compare(Int64 a, const RowKeyValue & b)
{
    RowKeyValue r_a{false, nullptr, 0, a};
    return compare(r_a, b);
}

inline int compare(const RowKeyValue & a, Int64 b)
{
    RowKeyValue r_b{false, nullptr, 0, b};
    return compare(a, r_b);
}

inline bool operator<(const RowKeyValue & a, Int64 b)
{
    return compare(a, b) < 0;
}

inline bool operator<(Int64 a, const RowKeyValue & b)
{
    return compare(a, b) < 0;
}

inline const RowKeyValue & max(const RowKeyValue & a, const RowKeyValue & b)
{
    return compare(a, b) >= 0 ? a : b;
}

struct RowKeyColumnContainer
{
    const ColumnPtr &             column;
    const ColumnString::Chars_t * string_data;
    const ColumnString::Offsets * string_offsets;
    const PaddedPODArray<Int64> * int_data;
    bool                          is_common_handle;
    RowKeyColumnContainer(const ColumnPtr & column_, bool is_common_handle_) : column(column_), is_common_handle(is_common_handle_)
    {
        if (is_common_handle)
        {
            const auto & column_string = *checkAndGetColumn<ColumnString>(&*column);
            string_data                = &column_string.getChars();
            string_offsets             = &column_string.getOffsets();
        }
        else
        {
            int_data = &toColumnVectorData<Int64>(column);
        }
    }
    RowKeyValue getRowKeyValue(size_t index) const
    {
        if (is_common_handle)
        {
            size_t prev_offset = index == 0 ? 0 : (*string_offsets)[index - 1];
            return RowKeyValue{is_common_handle,
                               reinterpret_cast<const char *>(&(*string_data)[prev_offset]),
                               (*string_offsets)[index] - prev_offset - 1,
                               0};
        }
        else
        {
            Int64 int_value = (*int_data)[index];
            return RowKeyValue{is_common_handle, nullptr, 0, int_value};
        }
    }
};

namespace
{
// https://en.cppreference.com/w/cpp/algorithm/lower_bound
size_t lowerBound(const RowKeyColumnContainer & rowkey_column, size_t first, size_t last, const RowKeyValue & value)
{
    size_t count = last - first;
    while (count > 0)
    {
        size_t step  = count / 2;
        size_t index = first + step;
        if (compare(value, rowkey_column.getRowKeyValue(index)) > 0)
        {
            first = index + 1;
            count -= step + 1;
        }
        else
            count = step;
    }
    return first;
}
} // namespace

struct RowKeyRange
{
    // todo use template to refine is_common_handle
    bool is_common_handle;
    /// start and end in RowKeyRange are always meaningful
    StringPtr start;
    StringPtr end;
    Int64     int_start = 0;
    Int64     int_end   = 0;
    size_t    rowkey_column_size;

    struct CommonHandleRangeMinMax
    {
        StringPtr min;
        StringPtr max;
        CommonHandleRangeMinMax(size_t rowkey_column_size)
            : min(std::make_shared<String>(rowkey_column_size, TiDB::CodecFlag::CodecFlagBytes)),
              max(std::make_shared<String>(rowkey_column_size, TiDB::CodecFlag::CodecFlagMax))
        {
        }
    };

    struct TableRangeMinMax
    {
        StringPtr min;
        StringPtr max;

        TableRangeMinMax(TableID table_id, bool is_common_handle, size_t rowkey_column_size)
        {
            std::stringstream ss;
            ss.put('t');
            EncodeInt64(table_id, ss);
            ss.put('_');
            ss.put('r');
            String prefix = ss.str();
            if (is_common_handle)
            {
                auto & common_handle_min_max = RowKeyRange::getMinMaxData(rowkey_column_size);
                String min_str               = prefix + (*common_handle_min_max.min);
                min                          = std::make_shared<String>(min_str);
                String max_str               = prefix + (*common_handle_min_max.max);
                max                          = std::make_shared<String>(max_str);
            }
            else
            {
                min = std::make_shared<String>(prefix + *int_handle_min_key);
                max = std::make_shared<String>(prefix + *int_handle_max_key);
            }
        }
    };

    static std::unordered_map<size_t, CommonHandleRangeMinMax> min_max_data;
    static std::mutex                                          mutex;
    static std::unordered_map<TableID, TableRangeMinMax>       table_min_max_data;
    static std::mutex                                          table_mutex;
    static const CommonHandleRangeMinMax &                     getMinMaxData(size_t rowkey_column_size);
    static const TableRangeMinMax & getTableMinMaxData(TableID table_id, bool is_common_handle, size_t rowkey_column_size);

    RowKeyRange(const StringPtr start_, const StringPtr end_, bool is_common_handle_, size_t rowkey_column_size_)
        : is_common_handle(is_common_handle_), start(start_), end(end_), rowkey_column_size(rowkey_column_size_)
    {
        if (!is_common_handle)
        {
            size_t cursor = 0;
            int_start     = DB::DecodeInt64(cursor, *start);
            cursor        = 0;
            int_end       = DB::DecodeInt64(cursor, *end);
        }
    }

    RowKeyRange(
        const StringPtr start_, const StringPtr end_, Int64 int_start_, Int64 int_end_, bool is_common_handle_, size_t rowkey_column_size_)
        : is_common_handle(is_common_handle_),
          start(start_),
          end(end_),
          int_start(int_start_),
          int_end(int_end_),
          rowkey_column_size(rowkey_column_size_)
    {
    }

    RowKeyRange() : is_common_handle(false), start(empty_string_ptr), end(empty_string_ptr), int_start(0), int_end(0), rowkey_column_size(0)
    {
    }

    void swap(RowKeyRange & other)
    {
        std::swap(is_common_handle, other.is_common_handle);
        std::swap(start, other.start);
        std::swap(end, other.end);
        std::swap(int_start, other.int_start);
        std::swap(int_end, other.int_end);
        std::swap(rowkey_column_size, other.rowkey_column_size);
    }

    static RowKeyRange startFrom(const RowKeyValue & start_value, bool is_common_handle, size_t rowkey_column_size)
    {
        if (is_common_handle)
        {
            const auto & min_max = getMinMaxData(rowkey_column_size);
            return RowKeyRange(
                std::make_shared<String>(start_value.data, start_value.size), min_max.max, is_common_handle, rowkey_column_size);
        }
        else
        {
            if (start_value.data == nullptr)
            {
                std::stringstream ss;
                DB::EncodeInt64(start_value.int_value, ss);
                return RowKeyRange(std::make_shared<String>(ss.str()),
                                   int_handle_max_key,
                                   start_value.int_value,
                                   int_handle_max,
                                   is_common_handle,
                                   rowkey_column_size);
            }
            else
            {
                return RowKeyRange(std::make_shared<String>(start_value.data, start_value.size),
                                   int_handle_max_key,
                                   start_value.int_value,
                                   int_handle_max,
                                   is_common_handle,
                                   rowkey_column_size);
            }
        }
    }

    static RowKeyRange endWith(const RowKeyValue & end_value, bool is_common_handle, size_t rowkey_column_size)
    {
        if (is_common_handle)
        {
            const auto & min_max = getMinMaxData(rowkey_column_size);
            return RowKeyRange(min_max.min, std::make_shared<String>(end_value.data, end_value.size), is_common_handle, rowkey_column_size);
        }
        else
        {
            if (end_value.data == nullptr)
            {
                std::stringstream ss;
                DB::EncodeInt64(end_value.int_value, ss);
                return RowKeyRange(int_handle_min_key,
                                   std::make_shared<String>(ss.str()),
                                   int_handle_min,
                                   end_value.int_value,
                                   is_common_handle,
                                   rowkey_column_size);
            }
            else
                return RowKeyRange(int_handle_min_key,
                                   std::make_shared<String>(end_value.data, end_value.size),
                                   int_handle_min,
                                   end_value.int_value,
                                   is_common_handle,
                                   rowkey_column_size);
        }
    }

    static RowKeyRange newAll(bool is_common_handle, size_t rowkey_column_size)
    {
        if (is_common_handle)
        {
            const auto & min_max = getMinMaxData(rowkey_column_size);
            return RowKeyRange(min_max.min, min_max.max, is_common_handle, rowkey_column_size);
        }
        else
        {
            return RowKeyRange(int_handle_min_key, int_handle_max_key, is_common_handle, rowkey_column_size);
        }
    }

    static RowKeyRange newNone(bool is_common_handle, size_t rowkey_column_size)
    {
        if (is_common_handle)
        {
            const auto & min_max = getMinMaxData(rowkey_column_size);
            return RowKeyRange(min_max.max, min_max.min, is_common_handle, rowkey_column_size);
        }
        else
        {
            return RowKeyRange(
                int_handle_max_key, int_handle_min_key, int_handle_max, int_handle_min, is_common_handle, rowkey_column_size);
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBoolText(is_common_handle, buf);
        writeIntBinary(rowkey_column_size, buf);
        writeStringBinary(*start, buf);
        writeStringBinary(*end, buf);
    }

    static RowKeyRange deserialize(ReadBuffer & buf)
    {
        bool   is_common_handle;
        size_t rowkey_column_size;
        String start, end;
        readBoolText(is_common_handle, buf);
        readIntBinary(rowkey_column_size, buf);
        readStringBinary(start, buf);
        readStringBinary(end, buf);
        return RowKeyRange(std::make_shared<String>(start), std::make_shared<String>(end), is_common_handle, rowkey_column_size);
    }

    inline bool all() const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < rowkey_column_size; i++)
            {
                if (!(static_cast<unsigned char>((*start)[i]) == TiDB::CodecFlag::CodecFlagBytes
                      || static_cast<unsigned char>((*start)[i]) == TiDB::CodecFlag::CodecFlagNil)
                    || static_cast<unsigned char>((*end)[i]) != TiDB::CodecFlag::CodecFlagMax)
                    return false;
            }
            return true;
        }
        else
        {
            return int_start == int_handle_min && int_end == int_handle_max && end->compare(*int_handle_max_key) >= 0;
        }
    }

    inline bool isEndInfinite() const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < rowkey_column_size; i++)
            {
                if (static_cast<unsigned char>((*end)[i]) != TiDB::CodecFlagMax)
                    return false;
            }
            return true;
        }
        else
        {
            return int_end == int_handle_max && end->compare(*int_handle_max_key) >= 0;
        }
    }

    inline bool isStartInfinite() const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < rowkey_column_size; i++)
            {
                if (!(static_cast<unsigned char>((*start)[i]) == TiDB::CodecFlagBytes
                      || static_cast<unsigned char>((*start)[i]) == TiDB::CodecFlag::CodecFlagNil))
                    return false;
            }
            return true;
        }
        else
        {
            return int_start == int_handle_min;
        }
    }

    inline bool none() const { return start->compare(*end) >= 0; }

    inline RowKeyRange shrink(const RowKeyRange & other) const
    {
        return RowKeyRange(max(start, other.start),
                           min(end, other.end),
                           std::max(int_start, other.int_start),
                           std::min(int_end, other.int_end),
                           is_common_handle,
                           rowkey_column_size);
    }

    inline RowKeyRange merge(const RowKeyRange & other) const
    {
        return RowKeyRange(min(start, other.start),
                           max(end, other.end),
                           std::min(int_start, other.int_start),
                           std::max(int_end, other.int_end),
                           is_common_handle,
                           rowkey_column_size);
    }

    inline void setStart(const RowKeyValue & value)
    {
        if (is_common_handle)
        {
            start = std::make_shared<String>(value.data, value.size);
        }
        else
        {
            int_start = value.int_value;
            if (value.data != nullptr)
                start = std::make_shared<String>(value.data, value.size);
            else
            {
                std::stringstream ss;
                DB::EncodeInt64(value.int_value, ss);
                start = std::make_shared<String>(ss.str());
            }
        }
    }

    inline void setEnd(const RowKeyValue & value)
    {
        if (is_common_handle)
        {
            end = std::make_shared<String>(value.data, value.size);
        }
        else
        {
            int_end = value.int_value;
            if (value.data != nullptr)
                end = std::make_shared<String>(value.data, value.size);
            else
            {
                std::stringstream ss;
                DB::EncodeInt64(value.int_value, ss);
                end = std::make_shared<String>(ss.str());
            }
        }
    }

    inline bool intersect(const RowKeyRange & other) const { return max(other.start, start)->compare(*min(other.end, end)) < 0; }

    // [first, last_include]
    inline bool include(const RowKeyValue & first, const RowKeyValue & last_include) const { return check(first) && check(last_include); }

    inline bool checkStart(const RowKeyValue & value) const { return compare(getStart(), value) <= 0; }

    inline bool checkEnd(const RowKeyValue & value) const { return compare(value, getEnd()) < 0; }

    inline bool check(const RowKeyValue & value) const { return checkStart(value) && checkEnd(value); }

    inline RowKeyValue getStart() const
    {
        RowKeyValue ret;
        ret.is_common_handle = is_common_handle;
        ret.data             = start->data();
        ret.size             = start->size();
        ret.int_value        = int_start;
        return ret;
    }

    inline RowKeyValue getEnd() const
    {
        RowKeyValue ret;
        ret.is_common_handle = is_common_handle;
        ret.data             = end->data();
        ret.size             = end->size();
        ret.int_value        = int_end;
        return ret;
    }

    inline HandleRange toHandleRange() const
    {
        if (is_common_handle)
            throw Exception("Can not convert comman handle range to HandleRange");
        return {int_start, int_end};
    }

    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> toRegionRange(TableID table_id)
    {
        std::stringstream ss;
        ss.put('t');
        EncodeInt64(table_id, ss);
        ss.put('_');
        ss.put('r');
        String            prefix    = ss.str();
        DecodedTiKVKeyPtr start_key = std::make_shared<DecodedTiKVKey>(prefix + *start);
        DecodedTiKVKeyPtr end_key   = std::make_shared<DecodedTiKVKey>(prefix + *end);
        return {start_key, end_key};
    }

    /// return <offset, limit>
    std::pair<size_t, size_t> getPosRange(const ColumnPtr & column, const size_t offset, const size_t limit) const
    {
        RowKeyColumnContainer rowkey_column(column, is_common_handle);
        size_t                start_index
            = check(rowkey_column.getRowKeyValue(offset)) ? offset : lowerBound(rowkey_column, offset, offset + limit, getStart());
        size_t end_index = check(rowkey_column.getRowKeyValue(offset + limit - 1))
            ? offset + limit
            : lowerBound(rowkey_column, offset, offset + limit, getEnd());
        return {start_index, end_index - start_index};
    }

    static RowKeyRange fromHandleRange(const HandleRange & handle_range)
    {
        if (handle_range.all())
        {
            return RowKeyRange(int_handle_min_key, int_handle_max_key, int_handle_min, int_handle_max, false, 1);
        }
        std::stringstream ss;
        DB::EncodeInt64(handle_range.start, ss);
        String start = ss.str();
        ss.str(std::string());
        DB::EncodeInt64(handle_range.end, ss);
        String end = ss.str();
        return RowKeyRange(std::make_shared<String>(start), std::make_shared<String>(end), handle_range.start, handle_range.end, false, 1);
    }

    static RowKeyRange fromRegionRange(const std::shared_ptr<const RegionRangeKeys> & region_range,
                                       const TableID                                  table_id,
                                       bool                                           is_common_handle,
                                       size_t                                         rowkey_column_size)
    {
        return fromRegionRange(region_range->rawKeys(), region_range->getMappedTableID(), table_id, is_common_handle, rowkey_column_size);
    }
    static RowKeyRange fromRegionRange(const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & raw_keys,
                                       const TableID                                           table_id_in_raw_key,
                                       const TableID                                           table_id,
                                       bool                                                    is_common_handle,
                                       size_t                                                  rowkey_column_size)
    {
        if (likely(table_id_in_raw_key == table_id))
        {
            auto &    start_key             = *raw_keys.first;
            auto &    end_key               = *raw_keys.second;
            auto      table_range_min_max   = getTableMinMaxData(table_id, is_common_handle, rowkey_column_size);
            auto      common_handle_min_max = getMinMaxData(rowkey_column_size);
            StringPtr start_ptr, end_ptr;
            if (start_key.compare(*table_range_min_max.min) <= 0)
            {
                if (is_common_handle)
                    start_ptr = common_handle_min_max.min;
                else
                    start_ptr = int_handle_min_key;
            }
            else
            {
                start_ptr = std::make_shared<std::string>(start_key.begin() + RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE, start_key.end());
            }
            if (end_key.compare(*table_range_min_max.max) >= 0)
            {
                if (is_common_handle)
                    end_ptr = common_handle_min_max.max;
                else
                    end_ptr = int_handle_max_key;
            }
            else
                end_ptr = std::make_shared<std::string>(end_key.begin() + RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE, end_key.end());
            return RowKeyRange(start_ptr, end_ptr, is_common_handle, rowkey_column_size);
        }
        else
        {
            /// if table id is not the same, just return none range
            /// maybe should throw exception since it should not happen
            return newNone(is_common_handle, rowkey_column_size);
        }
    }


    inline String toString() const { return rangeToString(*this); }

    bool operator==(const RowKeyRange & rhs) const { return start->compare(*rhs.start) == 0 && end->compare(*rhs.end) == 0; }
    bool operator!=(const RowKeyRange & rhs) const { return !(*this == rhs); }
};

template <bool right_open = true>
inline String rangeToString(const String & start, const String & end, bool)
{
    String s = "[" + ToHex(start.data(), start.size()) + "," + ToHex(end.data(), end.size());
    if constexpr (right_open)
        s += ")";
    else
        s += "]";
    return s;
}

inline String rangeToString(const RowKeyRange & range)
{
    return rangeToString<true>(*range.start, *range.end, range.is_common_handle);
}

// DB::DM::Handle
using RowKeyRanges = std::vector<RowKeyRange>;

inline String toString(const RowKeyRanges & ranges)
{
    String s = "{";
    for (auto & r : ranges)
    {
        s += r.toString() + ",";
    }
    if (!ranges.empty())
        s.erase(s.size() - 1);
    s += "}";
    return s;
}

inline RowKeyRange mergeRanges(const RowKeyRanges & ranges, bool is_common_handle, size_t rowkey_column_size)
{
    RowKeyRange range = RowKeyRange::newNone(is_common_handle, rowkey_column_size);
    for (auto & r : ranges)
    {
        range.start     = min(range.start, r.start);
        range.end       = max(range.end, r.end);
        range.int_start = std::min(range.int_start, r.int_start);
        range.int_end   = std::max(range.int_end, r.int_end);
    }
    return range;
}

struct RowKeySplitPoint
{
    RowKeySplitPoint() = default;
    explicit RowKeySplitPoint(const RowKeyValue & rowkey_value)
    {
        is_common_handle = rowkey_value.is_common_handle;
        if (is_common_handle)
            value = std::make_shared<String>(rowkey_value.data, rowkey_value.size);
        else
        {
            std::stringstream ss;
            DB::EncodeInt64(rowkey_value.int_value, ss);
            value = std::make_shared<String>(ss.str());
        }
        int_value = rowkey_value.int_value;
    }
    bool        is_common_handle;
    StringPtr   value;
    Int64       int_value;
    RowKeyValue toRowKeyValue() { return RowKeyValue{is_common_handle, value->data(), value->size(), int_value}; }
};

} // namespace DB::DM
