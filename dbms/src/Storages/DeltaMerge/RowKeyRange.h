#pragma once
#include <Core/Types.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/Types.h>

namespace DB::DM
{
using PrimaryKey    = ColumnDefines;
using PrimaryKeyPtr = std::shared_ptr<PrimaryKey>;
using StringPtr     = std::shared_ptr<String>;

struct RowKeyRange;
String rangeToString(const RowKeyRange & range);

static const Int64 int_handle_min = std::numeric_limits<HandleID>::min();
static const Int64 int_handle_max = std::numeric_limits<HandleID>::max();

inline String getIntHandleMinKey()
{
    std::stringstream ss;
    DB::EncodeInt64(int_handle_min, ss);
    return ss.str();
}

inline String getIntHandleMaxKey()
{
    std::stringstream ss;
    DB::EncodeInt64(int_handle_max, ss);
    ss.put('\0');
    return ss.str();
}

static const String int_handle_min_key = getIntHandleMinKey();
static const String int_handle_max_key = getIntHandleMaxKey();

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
            a_inf = compare(a.data, a.size, int_handle_max_key.data(), int_handle_max_key.size()) == 0;
        if (b.data != nullptr)
            b_inf = compare(b.data, b.size, int_handle_max_key.data(), int_handle_max_key.size()) == 0;
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

struct RowKeyColumn
{
    const ColumnPtr &             column;
    const ColumnString::Chars_t * string_data;
    const ColumnString::Offsets * string_offsets;
    const PaddedPODArray<Int64> * int_data;
    bool                          is_common_handle;
    RowKeyColumn(const ColumnPtr & column_, bool is_common_handle_) : column(column_), is_common_handle(is_common_handle_)
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
size_t lowerBound(const RowKeyColumn & rowkey_column, size_t first, size_t last, const RowKeyValue & value)
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
    String start;
    String end;
    Int64  int_start = 0;
    Int64  int_end   = 0;
    size_t rowkey_column_size;

    RowKeyRange(const String & start_, const String & end_, bool is_common_handle_, size_t rowkey_column_size_)
        : is_common_handle(is_common_handle_), start(start_), end(end_), rowkey_column_size(rowkey_column_size_)
    {
        if (!is_common_handle)
        {
            size_t cursor = 0;
            int_start     = DB::DecodeInt64(cursor, start);
            cursor        = 0;
            int_end       = DB::DecodeInt64(cursor, end);
        }
    }

    RowKeyRange(
        const String & start_, const String & end_, Int64 int_start_, Int64 int_end_, bool is_common_handle_, size_t rowkey_column_size_)
        : is_common_handle(is_common_handle_),
          start(start_),
          end(end_),
          int_start(int_start_),
          int_end(int_end_),
          rowkey_column_size(rowkey_column_size_)
    {
    }

    RowKeyRange() : is_common_handle(false), start(""), end(""), int_start(0), int_end(0), rowkey_column_size(0) {}

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
            return RowKeyRange(String(start_value.data, start_value.size),
                               String(rowkey_column_size, TiDB::CodecFlag::CodecFlagMax),
                               is_common_handle,
                               rowkey_column_size);
        }
        else
        {
            if (start_value.data == nullptr)
            {
                std::stringstream ss;
                DB::EncodeInt64(start_value.int_value, ss);
                return RowKeyRange(
                    ss.str(), int_handle_max_key, start_value.int_value, int_handle_max, is_common_handle, rowkey_column_size);
            }
            else
            {
                return RowKeyRange(String(start_value.data, start_value.size),
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
            return RowKeyRange(String(rowkey_column_size, TiDB::CodecFlag::CodecFlagBytes),
                               String(end_value.data, end_value.size),
                               is_common_handle,
                               rowkey_column_size);
        else
        {
            if (end_value.data == nullptr)
            {
                std::stringstream ss;
                DB::EncodeInt64(end_value.int_value, ss);
                return RowKeyRange(int_handle_min_key, ss.str(), int_handle_min, end_value.int_value, is_common_handle, rowkey_column_size);
            }
            else
                return RowKeyRange(int_handle_min_key,
                                   String(end_value.data, end_value.size),
                                   int_handle_min,
                                   end_value.int_value,
                                   is_common_handle,
                                   rowkey_column_size);
        }
    }

    static RowKeyRange newAll(size_t rowkey_column_size, bool is_common_handle)
    {
        if (is_common_handle)
        {
            String start = String(rowkey_column_size, TiDB::CodecFlag::CodecFlagBytes);
            String end   = String(rowkey_column_size, TiDB::CodecFlag::CodecFlagMax);
            return RowKeyRange(start, end, is_common_handle, rowkey_column_size);
        }
        else
        {
            return RowKeyRange(int_handle_min_key, int_handle_max_key, is_common_handle, rowkey_column_size);
        }
    }

    static RowKeyRange newNone(size_t rowkey_column_size, bool is_common_handle)
    {
        if (is_common_handle)
        {
            String end   = String(rowkey_column_size, TiDB::CodecFlag::CodecFlagBytes);
            String start = String(rowkey_column_size, TiDB::CodecFlag::CodecFlagMax);
            return RowKeyRange(start, end, is_common_handle, rowkey_column_size);
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
        writeStringBinary(start, buf);
        writeStringBinary(end, buf);
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
        return RowKeyRange(start, end, is_common_handle, rowkey_column_size);
    }

    inline bool all() const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < rowkey_column_size; i++)
            {
                if (!(static_cast<unsigned char>(start[i]) == TiDB::CodecFlag::CodecFlagBytes
                      || static_cast<unsigned char>(start[i]) == TiDB::CodecFlag::CodecFlagNil)
                    || static_cast<unsigned char>(end[i]) != TiDB::CodecFlag::CodecFlagMax)
                    return false;
            }
            return true;
        }
        else
        {
            return int_start == int_handle_min && int_end == int_handle_max && end.compare(int_handle_max_key) >= 0;
        }
    }

    inline bool isEndInfinite() const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < rowkey_column_size; i++)
            {
                if (static_cast<unsigned char>(end[i]) != TiDB::CodecFlagMax)
                    return false;
            }
            return true;
        }
        else
        {
            return int_end == int_handle_max && end.compare(int_handle_max_key) >= 0;
        }
    }

    inline bool isStartInfinite() const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < rowkey_column_size; i++)
            {
                if (!(static_cast<unsigned char>(start[i]) == TiDB::CodecFlagBytes
                      || static_cast<unsigned char>(start[i]) == TiDB::CodecFlag::CodecFlagNil))
                    return false;
            }
            return true;
        }
        else
        {
            return int_start == int_handle_min;
        }
    }

    inline bool none() const { return start.compare(end) >= 0; }

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

    inline bool intersect(const RowKeyRange & other) const { return max(other.start, start).compare(min(other.end, end)) < 0; }

    // [first, last_include]
    inline bool include(const RowKeyValue & first, const RowKeyValue & last_include) const { return check(first) && check(last_include); }

    inline bool checkStart(const RowKeyValue & value) const { return compare(getStart(), value) <= 0; }

    inline bool checkEnd(const RowKeyValue & value) const { return compare(value, getEnd()) < 0; }

    inline bool check(const RowKeyValue & value) const { return checkStart(value) && checkEnd(value); }

    inline RowKeyValue getStart() const
    {
        RowKeyValue ret;
        ret.is_common_handle = is_common_handle;
        ret.data             = start.data();
        ret.size             = start.size();
        ret.int_value        = int_start;
        return ret;
    }

    inline RowKeyValue getEnd() const
    {
        RowKeyValue ret;
        ret.is_common_handle = is_common_handle;
        ret.data             = end.data();
        ret.size             = end.size();
        ret.int_value        = int_end;
        return ret;
    }

    inline HandleRange toHandleRange() const
    {
        if (is_common_handle)
            throw Exception("Can not convert comman handle range to HandleRange");
        return {int_start, int_end};
    }

    /// return <offset, limit>
    std::pair<size_t, size_t> getPosRange(const ColumnPtr & column, const size_t offset, const size_t limit) const
    {
        RowKeyColumn rowkey_column(column, is_common_handle);
        size_t       start_index
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
        return RowKeyRange(start, end, handle_range.start, handle_range.end, false, 1);
    }

    inline String toString() const { return rangeToString(*this); }

    bool operator==(const RowKeyRange & rhs) const { return start.compare(rhs.start) == 0 && end.compare(rhs.end) == 0; }
    bool operator!=(const RowKeyRange & rhs) const { return !(*this == rhs); }
};

template <bool right_open = true>
inline String rangeToString(const String & start, const String & end, bool)
{
    /// todo show the decoded value
    String s = "[" + DB::toString(start) + "," + DB::toString(end);
    if constexpr (right_open)
        s += ")";
    else
        s += "]";
    return s;
}

inline String rangeToString(const RowKeyRange & range)
{
    return rangeToString<true>(range.start, range.end, range.is_common_handle);
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

inline RowKeyRange mergeRanges(const RowKeyRanges & ranges, size_t rowkey_column_size, bool is_common_handle)
{
    RowKeyRange range = RowKeyRange::newNone(rowkey_column_size, is_common_handle);
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
            value = String(rowkey_value.data, rowkey_value.size);
        else
        {
            std::stringstream ss;
            DB::EncodeInt64(rowkey_value.int_value, ss);
            value = ss.str();
        }
        int_value = rowkey_value.int_value;
    }
    bool        is_common_handle;
    String      value;
    Int64       int_value;
    RowKeyValue toRowKeyValue() { return RowKeyValue{is_common_handle, value.data(), value.size(), int_value}; }
};

} // namespace DB::DM
