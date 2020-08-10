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

struct RowKeyRange;
String rangeToString(const RowKeyRange & range);

inline StringRef getStringRefData(const ColumnString::Chars_t & data, const ColumnString::Offsets & offsets, const size_t pos)
{
    size_t prev_offset = pos == 0 ? 0 : offsets[pos - 1];
    return StringRef(reinterpret_cast<const char *>(&data[prev_offset]), offsets[pos] - prev_offset - 1);
}

inline String getIntHandleMinKey()
{
    std::stringstream ss;
    DB::EncodeInt64(std::numeric_limits<Int64>::min(), ss);
    return ss.str();
}
inline String getIntHandleMaxKey()
{
    std::stringstream ss;
    DB::EncodeInt64(std::numeric_limits<Int64>::min(), ss);
    ss.put('\0');
    return ss.str();
}

static const String int_handle_min_key = getIntHandleMinKey();
static const String int_handle_max_key = getIntHandleMaxKey();

inline int compare(const char * a, size_t a_size, const char * b, size_t b_size)
{
    int res = memcmp(a, b, std::min(a_size, b_size));
    return res != 0 ? res : (Int64)a_size - (Int64)b_size;
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
    bool         is_common_handle;
    const char * data;
    size_t       size;
    Int64        int_value;
    bool         include;
};

inline bool operator<(const RowKeyValue & a, const RowKeyValue & b)
{
    int compare_result = compare(a.data, a.size, b.data, b.size);
    if (compare_result || (a.include ^ b.include))
        return compare_result < 0;
    return !a.include && b.include;
}

inline bool operator<(const StringRef & a, const RowKeyValue & b)
{
    RowKeyValue r_a{false, a.data, a.size, 0, true};
    return r_a < b;
}

inline bool operator<(const RowKeyValue & a, const StringRef & b)
{
    RowKeyValue r_b{false, b.data, b.size, 0, true};
    return a < r_b;
}

inline int compare(Int64 a, const RowKeyValue & b)
{
    if (a != b.int_value)
        return a > b.int_value ? 1 : -1;
    if (unlikely(a == std::numeric_limits<Int64>::max()
                 && compare(b.data, b.size, int_handle_max_key.data(), int_handle_max_key.size()) == 0))
        return -1;
    return 0;
}

inline int compare(const RowKeyValue & a, Int64 b)
{
    if (a.int_value != b)
        return a.int_value > b ? 1 : -1;
    if (unlikely(b == std::numeric_limits<Int64>::max()
                 && compare(a.data, a.size, int_handle_max_key.data(), int_handle_max_key.size()) == 0))
        return 1;
    return 0;
}

inline bool operator<(const RowKeyValue & a, Int64 b)
{
    return compare(a, b) < 0;
}

inline bool operator<(Int64 a, const RowKeyValue & b)
{
    return compare(a, b) < 0;
}

namespace
{
// https://en.cppreference.com/w/cpp/algorithm/lower_bound
size_t
lowerBound(const ColumnString::Chars_t & data, const ColumnString::Offsets & offsets, size_t first, size_t last, const RowKeyValue & value)
{
    size_t count = last - first;
    while (count > 0)
    {
        size_t    step    = count / 2;
        size_t    index   = first + step;
        StringRef current = getStringRefData(data, offsets, index);
        if (compare(value.data, value.size, current.data, current.size) > 0)
        {
            first = index + 1;
            count -= step + 1;
        }
        else
            count = step;
    }
    return first;
}

// https://en.cppreference.com/w/cpp/algorithm/lower_bound
size_t lowerBound(const PaddedPODArray<Int64> & data, size_t first, size_t last, const RowKeyValue & value)
{
    size_t count = last - first;
    while (count > 0)
    {
        size_t step  = count / 2;
        size_t index = first + step;
        if (compare(value, data[index]) > 0)
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
    bool   is_common_handle;
    String start;
    String end;
    Int64  int_start;
    Int64  int_end;

    RowKeyRange(const String & start_, const String & end_, bool is_common_handle_)
        : is_common_handle(is_common_handle_), start(start_), end(end_)
    {
        if (is_common_handle)
        {
            size_t cursor = 0;
            int_start     = DB::DecodeInt64(cursor, start);
            cursor        = 0;
            int_end       = DB::DecodeInt64(cursor, end);
        }
    }

    RowKeyRange(const String & start_, const String & end_, Int64 int_start_, Int64 int_end_)
        : is_common_handle(false), start(start_), end(end_), int_start(int_start_), int_end(int_end_)
    {
    }

    RowKeyRange() = default;

    void swap(RowKeyRange & other)
    {
        std::swap(is_common_handle, other.is_common_handle);
        std::swap(start, other.start);
        std::swap(end, other.end);
        std::swap(int_start, other.int_start);
        std::swap(int_end, other.int_end);
    }

    static RowKeyRange newAll(const PrimaryKeyPtr pk, bool is_common_handle)
    {
        if (is_common_handle)
        {
            String start = String(pk->size(), TiDB::CodecFlag::CodecFlagBytes);
            String end   = String(pk->size(), TiDB::CodecFlag::CodecFlagMax);
            return RowKeyRange(start, end, is_common_handle);
        }
        else
        {
            return RowKeyRange(int_handle_min_key, int_handle_max_key, is_common_handle);
        }
    }

    static RowKeyRange newNone(const PrimaryKeyPtr pk, bool is_common_handle)
    {
        if (is_common_handle)
        {
            String end   = String(pk->size(), TiDB::CodecFlag::CodecFlagBytes);
            String start = String(pk->size(), TiDB::CodecFlag::CodecFlagMax);
            return RowKeyRange(start, end, is_common_handle);
        }
        else
        {
            return RowKeyRange(int_handle_max_key, int_handle_min_key, is_common_handle);
        }
    }

    inline bool all(const PrimaryKeyPtr pk) const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < pk->size(); i++)
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
            return int_start == std::numeric_limits<Int64>::min() && int_end == std::numeric_limits<Int64>::max()
                && end.compare(int_handle_max_key) >= 0;
        }
    }

    inline bool isEndInfinite(const PrimaryKeyPtr pk) const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < pk->size(); i++)
            {
                if (static_cast<unsigned char>(end[i]) != TiDB::CodecFlagMax)
                    return false;
            }
            return true;
        }
        else
        {
            return end.compare(int_handle_max_key) >= 0;
        }
    }

    inline bool isStartInfinite(const PrimaryKeyPtr pk) const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < pk->size(); i++)
            {
                if (!(static_cast<unsigned char>(start[i]) == TiDB::CodecFlagBytes
                      || static_cast<unsigned char>(start[i]) == TiDB::CodecFlag::CodecFlagNil))
                    return false;
            }
            return true;
        }
        else
        {
            return int_start == std::numeric_limits<Int64>::min();
        }
    }

    inline bool none() const { return start.compare(end) >= 0; }

    inline RowKeyRange shrink(const RowKeyRange & other) const
    {
        return RowKeyRange(max(start, other.start), min(end, other.end), is_common_handle);
    }

    inline RowKeyRange merge(const RowKeyRange & other) const
    {
        return RowKeyRange(min(start, other.start), max(end, other.end), is_common_handle);
    }

    inline bool intersect(const RowKeyRange & other) const { return max(other.start, start).compare(min(other.end, end)) < 0; }

    // [first, last_include]
    inline bool intersect(const StringRef & first, const StringRef & last_include) const
    {
        const StringRef & max_start = max(first, StringRef(start.data(), start.size()));
        return (compare(last_include.data, last_include.size, end.data(), end.size()) >= 0 && checkEnd(max_start))
            || (compare(last_include.data, last_include.size, end.data(), end.size()) < 0
                && compare(max_start.data, max_start.size, last_include.data, last_include.size) <= 0);
    }

    // [first, last_include]
    inline bool include(const StringRef & first, const StringRef & last_include) const { return check(first) && check(last_include); }
    inline bool include(Int64 first, Int64 last_include) const { return check(first) && check(last_include); }

    inline bool checkStart(const StringRef & value) const { return compare(start.data(), start.size(), value.data, value.size) <= 0; }
    inline bool checkStart(Int64 value) const { return int_start <= value; }

    inline bool checkEnd(const StringRef & value) const { return compare(value.data, value.size, end.data(), end.size()) < 0; }
    inline bool checkEnd(Int64 value) const
    {
        if (value != int_end)
            return value < int_end;
        return value == std::numeric_limits<Int64>::max() && end.compare(int_handle_max_key);
    }

    inline bool check(const StringRef & value) const { return checkStart(value) && checkEnd(value); }
    inline bool check(Int64 value) const { return checkStart(value) && checkEnd(value); }

    inline RowKeyValue getStart() const
    {
        RowKeyValue ret;
        ret.is_common_handle = is_common_handle;
        ret.data             = start.data();
        ret.size             = start.size();
        ret.int_value        = int_start;
        ret.include          = true;
        return ret;
    }

    inline RowKeyValue getEnd() const
    {
        RowKeyValue ret;
        ret.is_common_handle = is_common_handle;
        ret.data             = end.data();
        ret.size             = end.size();
        ret.int_value        = int_end;
        ret.include          = false;
        return ret;
    }

    inline HandleRange toHandleRange() const
    {
        if (is_common_handle)
            throw Exception("Can not convert comman handle range to HandleRange");
        return {int_start, int_end};
    }
    /// return <offset, limit>
    std::pair<size_t, size_t>
    getPosRange(const ColumnString::Chars_t & data, const ColumnString::Offsets & offsets, const size_t offset, const size_t limit) const
    {
        StringRef start_ref   = getStringRefData(data, offsets, offset);
        size_t    start_index = check(start_ref) ? offset : lowerBound(data, offsets, offset, offset + limit, getStart());
        StringRef end_ref     = getStringRefData(data, offsets, offset + limit - 1);
        size_t    end_index   = check(end_ref) ? offset + limit : lowerBound(data, offsets, offset, offset + limit, getEnd());
        return {start_index, end_index - start_index};
    }

    /// return <offset, limit>
    std::pair<size_t, size_t> getPosRange(const ColumnPtr & column, const size_t offset, const size_t limit) const
    {
        if (is_common_handle)
        {
            const auto * column_string = checkAndGetColumn<ColumnString>(&*column);
            return getPosRange(column_string->getChars(), column_string->getOffsets(), offset, limit);
        }
        const auto & int_handle_data = toColumnVectorData<Int64>(column);
        size_t start_index = check(int_handle_data[offset]) ? offset : lowerBound(int_handle_data, offset, offset + limit, getStart());
        size_t end_index
            = check(int_handle_data[offset + limit - 1]) ? offset + limit : lowerBound(int_handle_data, offset, offset + limit, getEnd());
        return {start_index, end_index - start_index};
    }

    static RowKeyRange fromHandleRange(const HandleRange & handle_range)
    {
        std::stringstream ss;
        DB::EncodeInt64(handle_range.start, ss);
        String start = ss.str();
        ss.str(std::string());
        DB::EncodeInt64(handle_range.end, ss);
        String end = ss.str();
        return RowKeyRange(start, end, handle_range.start, handle_range.end);
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

inline RowKeyRange mergeRanges(const RowKeyRanges & ranges, const PrimaryKeyPtr pk, bool is_common_handle)
{
    RowKeyRange range = RowKeyRange::newNone(pk, is_common_handle);
    for (auto & r : ranges)
    {
        range.start = min(range.start, r.start);
        range.end   = max(range.end, r.end);
    }
    return range;
}

} // namespace DB::DM
