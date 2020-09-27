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
using HandleValuePtr   = std::shared_ptr<String>;

struct RowKeyRange;
String rangeToString(const RowKeyRange & range);

inline int compare(const char * a, size_t a_size, const char * b, size_t b_size)
{
    int res = memcmp(a, b, std::min(a_size, b_size));
    if (res != 0)
        return res;
    return a_size == b_size ? 0 : a_size > b_size ? 1 : -1;
}

struct RowKeyValue;

/// A reference to RowKeyValue, normally the real data value is stored in a column.
struct RowKeyValueRef
{
    /// when is_common_handle = true, it means the table has clustered index, the rowkey value is a string
    /// when is_common_handle = false, it means the table has int/uint handle, the rowkey value is a int/uint
    bool is_common_handle;
    /// data in RowKeyValue can be nullptr if is_common_handle is false
    /// generally speaking:
    /// if the RowKeyValueRef comes from handle column, data is nullptr,
    /// if the RowKeyValueRef comes from meta data, the data is not nullptr
    const char * data;
    size_t       size;
    Int64        int_value;

    String toString() const
    {
        if (is_common_handle)
            return ToHex(data, size);
        return std::to_string(int_value);
    }
    RowKeyValue toRowKeyValue() const;
};

struct RowKeyValue
{
    RowKeyValue() = default;
    RowKeyValue(bool is_common_handle_, HandleValuePtr value_, Int64 int_value_)
        : is_common_handle(is_common_handle_), value(value_), int_value(int_value_)
    {
    }

    RowKeyValue(bool is_common_handle_, HandleValuePtr value_) : is_common_handle(is_common_handle_), value(value_)
    {
        if (is_common_handle)
            int_value = 0;
        else
        {
            size_t cursor = 0;
            int_value     = DB::DecodeInt64(cursor, *value);
        }
    }

    explicit RowKeyValue(const RowKeyValueRef & rowkey_value)
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

    String toString() const
    {
        if (is_common_handle)
            return ToHex(value->data(), value->size());
        return std::to_string(int_value);
    }

    RowKeyValueRef    toRowKeyValueRef() const { return RowKeyValueRef{is_common_handle, value->data(), value->size(), int_value}; }
    DecodedTiKVKeyPtr toRegionKey(TableID table_id) const
    {
        // FIXME: move this to TiKVRecordFormat.h
        std::stringstream ss;
        ss.put('t');
        EncodeInt64(table_id, ss);
        ss.put('_');
        ss.put('r');
        String prefix = ss.str();
        return std::make_shared<DecodedTiKVKey>(prefix + *value);
    }

    bool is_common_handle;
    /// In case of non common handle, the value field is redundant in most cases, except taht int_value == Int64::max_value,
    /// because RowKeyValue is an end point of RowKeyRange, assuming that RowKeyRange = [start_value, end_value), since the
    /// end_value of RowKeyRange is always exclusive, if we want to construct a RowKeyRange that include Int64::max_value,
    /// just set end_value.int_value to Int64::max_value is not enough, we still need to set end_value.value a carefully
    /// designed value. You can refer to INT_HANDLE_MAX_KEY for more details
    HandleValuePtr value;
    Int64          int_value;

    static const RowKeyValue INT_HANDLE_MIN_KEY;
    static const RowKeyValue INT_HANDLE_MAX_KEY;
    static const RowKeyValue EMPTY_STRING_KEY;
};

using RowKeyValues = std::vector<RowKeyValue>;

inline int compare(const RowKeyValueRef & a, const RowKeyValueRef & b)
{
    if (unlikely(a.is_common_handle != b.is_common_handle))
        throw Exception("Should not reach here, common handle rowkey value compare with non common handle rowkey value");
    if (a.is_common_handle)
    {
        return compare(a.data, a.size, b.data, b.size);
    }
    else
    {
        /// in case of non common handle, we can compare the int value directly in most cases
        if (a.int_value != b.int_value)
            return a.int_value > b.int_value ? 1 : -1;
        if (likely(a.int_value != RowKeyValue::INT_HANDLE_MAX_KEY.int_value || (a.data == nullptr && b.data == nullptr)))
            return 0;

        /// if a.int_value == b.int_value == Int64::max_value, we need to further check the data field because even if
        /// the RowKeyValueRef is bigger that Int64::max_value, the int_value field is Int64::max_value at most.
        bool a_inf = false;
        bool b_inf = false;
        if (a.data != nullptr)
            a_inf = compare(a.data, a.size, RowKeyValue::INT_HANDLE_MAX_KEY.value->data(), RowKeyValue::INT_HANDLE_MAX_KEY.value->size())
                == 0;
        if (b.data != nullptr)
            b_inf = compare(b.data, b.size, RowKeyValue::INT_HANDLE_MAX_KEY.value->data(), RowKeyValue::INT_HANDLE_MAX_KEY.value->size())
                == 0;
        if (a_inf != b_inf)
        {
            return a_inf ? 1 : -1;
        }
        else
        {
            return 0;
        }
    }
}

inline int compare(const StringRef & a, const RowKeyValueRef & b)
{
    RowKeyValueRef r_a{true, a.data, a.size, 0};
    return compare(r_a, b);
}

inline int compare(const RowKeyValueRef & a, const StringRef & b)
{
    RowKeyValueRef r_b{true, b.data, b.size, 0};
    return compare(a, r_b);
}

inline bool operator<(const RowKeyValueRef & a, const RowKeyValueRef & b)
{
    return compare(a, b) < 0;
}

inline bool operator<(const StringRef & a, const RowKeyValueRef & b)
{
    return compare(a, b) < 0;
}

inline bool operator<(const RowKeyValueRef & a, const StringRef & b)
{
    return compare(a, b) < 0;
}

inline int compare(Int64 a, const RowKeyValueRef & b)
{
    RowKeyValueRef r_a{false, nullptr, 0, a};
    return compare(r_a, b);
}

inline int compare(const RowKeyValueRef & a, Int64 b)
{
    RowKeyValueRef r_b{false, nullptr, 0, b};
    return compare(a, r_b);
}

inline bool operator<(const RowKeyValueRef & a, Int64 b)
{
    return compare(a, b) < 0;
}

inline bool operator<(Int64 a, const RowKeyValueRef & b)
{
    return compare(a, b) < 0;
}

inline const RowKeyValueRef & max(const RowKeyValueRef & a, const RowKeyValueRef & b)
{
    return compare(a, b) >= 0 ? a : b;
}

inline const RowKeyValue & max(const RowKeyValue & a, const RowKeyValue & b)
{
    return a.value->compare(*b.value) >= 0 ? a : b;
}

inline const RowKeyValue & min(const RowKeyValue & a, const RowKeyValue & b)
{
    return a.value->compare(*b.value) < 0 ? a : b;
}

struct RowKeyColumnContainer
{
    const ColumnPtr & column;
    bool              is_common_handle;
    bool              is_constant_column;

    /// The following members are simply references to values in 'column', for faster access.
    const ColumnString::Chars_t * string_data    = nullptr;
    const ColumnString::Offsets * string_offsets = nullptr;
    const PaddedPODArray<Int64> * int_data       = nullptr;

    RowKeyColumnContainer(const ColumnPtr & column_, bool is_common_handle_)
        : column(column_), is_common_handle(is_common_handle_), is_constant_column(column->isColumnConst())
    {
        ColumnPtr non_const_column_ptr = column;
        if (unlikely(is_constant_column))
        {
            non_const_column_ptr = checkAndGetColumn<ColumnConst>(column.get())->getDataColumnPtr();
        }
        if (is_common_handle)
        {
            const auto & column_string = *checkAndGetColumn<ColumnString>(non_const_column_ptr.get());
            string_data                = &column_string.getChars();
            string_offsets             = &column_string.getOffsets();
        }
        else
        {
            int_data = &toColumnVectorData<Int64>(non_const_column_ptr);
        }
    }
    RowKeyValueRef getRowKeyValue(size_t index) const
    {
        // todo check index out of bound error
        if (unlikely(is_constant_column))
            index = 0;
        if (is_common_handle)
        {
            size_t prev_offset = index == 0 ? 0 : (*string_offsets)[index - 1];
            return RowKeyValueRef{is_common_handle,
                                  reinterpret_cast<const char *>(&(*string_data)[prev_offset]),
                                  (*string_offsets)[index] - prev_offset - 1,
                                  0};
        }
        else
        {
            Int64 int_value = (*int_data)[index];
            return RowKeyValueRef{is_common_handle, nullptr, 0, int_value};
        }
    }
};

namespace
{
// https://en.cppreference.com/w/cpp/algorithm/lower_bound
size_t lowerBound(const RowKeyColumnContainer & rowkey_column, size_t first, size_t last, const RowKeyValueRef & value)
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
    RowKeyValue start;
    RowKeyValue end;
    size_t      rowkey_column_size;

    struct CommonHandleRangeMinMax
    {
        RowKeyValue min;
        RowKeyValue max;
        CommonHandleRangeMinMax(size_t rowkey_column_size)
            : min(true, std::make_shared<String>(rowkey_column_size, TiDB::CodecFlag::CodecFlagBytes), 0),
              max(true, std::make_shared<String>(rowkey_column_size, TiDB::CodecFlag::CodecFlagMax), 0)
        {
        }
    };

    struct TableRangeMinMax
    {
        HandleValuePtr min;
        HandleValuePtr max;

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
                String min_str               = prefix + (*common_handle_min_max.min.value);
                min                          = std::make_shared<String>(min_str);
                String max_str               = prefix + (*common_handle_min_max.max.value);
                max                          = std::make_shared<String>(max_str);
            }
            else
            {
                min = std::make_shared<String>(prefix + *RowKeyValue::INT_HANDLE_MIN_KEY.value);
                max = std::make_shared<String>(prefix + *RowKeyValue::INT_HANDLE_MAX_KEY.value);
            }
        }
    };

    static std::unordered_map<size_t, CommonHandleRangeMinMax> min_max_data;
    static std::mutex                                          mutex;
    static std::unordered_map<TableID, TableRangeMinMax>       table_min_max_data;
    static std::mutex                                          table_mutex;
    static const CommonHandleRangeMinMax &                     getMinMaxData(size_t rowkey_column_size);
    static const TableRangeMinMax & getTableMinMaxData(TableID table_id, bool is_common_handle, size_t rowkey_column_size);

    RowKeyRange(const RowKeyValue & start_, const RowKeyValue & end_, bool is_common_handle_, size_t rowkey_column_size_)
        : is_common_handle(is_common_handle_), start(start_), end(end_), rowkey_column_size(rowkey_column_size_)
    {
    }

    RowKeyRange() : is_common_handle(true), start(RowKeyValue::EMPTY_STRING_KEY), end(RowKeyValue::EMPTY_STRING_KEY), rowkey_column_size(0)
    {
    }

    void swap(RowKeyRange & other)
    {
        std::swap(is_common_handle, other.is_common_handle);
        std::swap(start, other.start);
        std::swap(end, other.end);
        std::swap(rowkey_column_size, other.rowkey_column_size);
    }

    static RowKeyRange startFrom(const RowKeyValueRef & start_value, bool is_common_handle, size_t rowkey_column_size)
    {
        if (is_common_handle)
        {
            const auto & min_max = getMinMaxData(rowkey_column_size);
            return RowKeyRange(start_value.toRowKeyValue(), min_max.max, is_common_handle, rowkey_column_size);
        }
        else
        {
            return RowKeyRange(start_value.toRowKeyValue(), RowKeyValue::INT_HANDLE_MAX_KEY, is_common_handle, rowkey_column_size);
        }
    }

    static RowKeyRange endWith(const RowKeyValueRef & end_value, bool is_common_handle, size_t rowkey_column_size)
    {
        if (is_common_handle)
        {
            const auto & min_max = getMinMaxData(rowkey_column_size);
            return RowKeyRange(min_max.min, end_value.toRowKeyValue(), is_common_handle, rowkey_column_size);
        }
        else
        {
            return RowKeyRange(RowKeyValue::INT_HANDLE_MIN_KEY, end_value.toRowKeyValue(), is_common_handle, rowkey_column_size);
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
            return RowKeyRange(RowKeyValue::INT_HANDLE_MIN_KEY, RowKeyValue::INT_HANDLE_MAX_KEY, is_common_handle, rowkey_column_size);
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
            return RowKeyRange(RowKeyValue::INT_HANDLE_MAX_KEY, RowKeyValue::INT_HANDLE_MIN_KEY, is_common_handle, rowkey_column_size);
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBoolText(is_common_handle, buf);
        writeIntBinary(rowkey_column_size, buf);
        writeStringBinary(*start.value, buf);
        writeStringBinary(*end.value, buf);
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
        return RowKeyRange(RowKeyValue(is_common_handle, std::make_shared<String>(start)),
                           RowKeyValue(is_common_handle, std::make_shared<String>(end)),
                           is_common_handle,
                           rowkey_column_size);
    }

    inline bool all() const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < rowkey_column_size; i++)
            {
                if (!(static_cast<unsigned char>((*start.value)[i]) == TiDB::CodecFlag::CodecFlagBytes
                      || static_cast<unsigned char>((*start.value)[i]) == TiDB::CodecFlag::CodecFlagNil)
                    || static_cast<unsigned char>((*end.value)[i]) != TiDB::CodecFlag::CodecFlagMax)
                    return false;
            }
            return true;
        }
        else
        {
            return start.int_value == RowKeyValue::INT_HANDLE_MIN_KEY.int_value
                && end.int_value == RowKeyValue::INT_HANDLE_MAX_KEY.int_value
                && end.value->compare(*RowKeyValue::INT_HANDLE_MAX_KEY.value) >= 0;
        }
    }

    inline bool isEndInfinite() const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < rowkey_column_size; i++)
            {
                if (static_cast<unsigned char>((*end.value)[i]) != TiDB::CodecFlagMax)
                    return false;
            }
            return true;
        }
        else
        {
            return end.int_value == RowKeyValue::INT_HANDLE_MAX_KEY.int_value
                && end.value->compare(*RowKeyValue::INT_HANDLE_MAX_KEY.value) >= 0;
        }
    }

    inline bool isStartInfinite() const
    {
        if (is_common_handle)
        {
            for (size_t i = 0; i < rowkey_column_size; i++)
            {
                if (!(static_cast<unsigned char>((*start.value)[i]) == TiDB::CodecFlagBytes
                      || static_cast<unsigned char>((*start.value)[i]) == TiDB::CodecFlag::CodecFlagNil))
                    return false;
            }
            return true;
        }
        else
        {
            return start.int_value == RowKeyValue::INT_HANDLE_MIN_KEY.int_value;
        }
    }

    inline bool none() const { return start.value->compare(*end.value) >= 0; }

    inline RowKeyRange shrink(const RowKeyRange & other) const
    {
        return RowKeyRange(max(start, other.start), min(end, other.end), is_common_handle, rowkey_column_size);
    }

    inline RowKeyRange merge(const RowKeyRange & other) const
    {
        return RowKeyRange(min(start, other.start), max(end, other.end), is_common_handle, rowkey_column_size);
    }

    inline void setStart(const RowKeyValue & value) { start = value; }

    inline void setEnd(const RowKeyValue & value) { end = value; }

    inline bool intersect(const RowKeyRange & other) const
    {
        return max(other.start, start).value->compare(*min(other.end, end).value) < 0;
    }

    // [first, last_include]
    inline bool include(const RowKeyValueRef & first, const RowKeyValueRef & last_include) const
    {
        return check(first) && check(last_include);
    }

    inline bool checkStart(const RowKeyValueRef & value) const { return compare(getStart(), value) <= 0; }

    inline bool checkEnd(const RowKeyValueRef & value) const { return compare(value, getEnd()) < 0; }

    inline bool check(const RowKeyValueRef & value) const { return checkStart(value) && checkEnd(value); }

    inline RowKeyValueRef getStart() const { return start.toRowKeyValueRef(); }

    inline RowKeyValueRef getEnd() const { return end.toRowKeyValueRef(); }

    inline HandleRange toHandleRange() const
    {
        if (is_common_handle)
            throw Exception("Can not convert comman handle range to HandleRange");
        return {start.int_value, end.int_value};
    }

    std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> toRegionRange(TableID table_id)
    {
        // FIXME: move this to TiKVRecordFormat.h
        std::stringstream ss;
        ss.put('t');
        EncodeInt64(table_id, ss);
        ss.put('_');
        ss.put('r');
        String            prefix    = ss.str();
        DecodedTiKVKeyPtr start_key = std::make_shared<DecodedTiKVKey>(prefix + *start.value);
        DecodedTiKVKeyPtr end_key   = std::make_shared<DecodedTiKVKey>(prefix + *end.value);
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
            return RowKeyRange(RowKeyValue::INT_HANDLE_MIN_KEY, RowKeyValue::INT_HANDLE_MAX_KEY, false, 1);
        }
        std::stringstream ss;
        DB::EncodeInt64(handle_range.start, ss);
        String start = ss.str();
        ss.str(std::string());
        DB::EncodeInt64(handle_range.end, ss);
        String end = ss.str();
        /// when handle_range.end == HandleRange::MAX, according to previous implementation, it should be +Inf
        return RowKeyRange(RowKeyValue(false, std::make_shared<String>(start), handle_range.start),
                           handle_range.end == HandleRange::MAX ? RowKeyValue::INT_HANDLE_MAX_KEY
                                                                : RowKeyValue(false, std::make_shared<String>(end), handle_range.end),
                           false,
                           1);
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
            auto &      start_key             = *raw_keys.first;
            auto &      end_key               = *raw_keys.second;
            auto        table_range_min_max   = getTableMinMaxData(table_id, is_common_handle, rowkey_column_size);
            auto        common_handle_min_max = getMinMaxData(rowkey_column_size);
            RowKeyValue start_value, end_value;
            if (start_key.compare(*table_range_min_max.min) <= 0)
            {
                if (is_common_handle)
                    start_value = common_handle_min_max.min;
                else
                    start_value = RowKeyValue::INT_HANDLE_MIN_KEY;
            }
            else
            {
                start_value = RowKeyValue(
                    is_common_handle,
                    std::make_shared<std::string>(start_key.begin() + RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE, start_key.end()));
            }
            if (end_key.compare(*table_range_min_max.max) >= 0)
            {
                if (is_common_handle)
                    end_value = common_handle_min_max.max;
                else
                    end_value = RowKeyValue::INT_HANDLE_MAX_KEY;
            }
            else
                end_value
                    = RowKeyValue(is_common_handle,
                                  std::make_shared<std::string>(end_key.begin() + RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE, end_key.end()));
            return RowKeyRange(start_value, end_value, is_common_handle, rowkey_column_size);
        }
        else
        {
            /// if table id is not the same, just return none range
            /// maybe should throw exception since it should not happen
            return newNone(is_common_handle, rowkey_column_size);
        }
    }


    inline String toString() const { return rangeToString(*this); }

    bool operator==(const RowKeyRange & rhs) const
    {
        return start.value->compare(*rhs.start.value) == 0 && end.value->compare(*rhs.end.value) == 0;
    }
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
    return rangeToString<true>(*range.start.value, *range.end.value, range.is_common_handle);
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
        range.start = min(range.start, r.start);
        range.end   = max(range.end, r.end);
    }
    return range;
}

} // namespace DB::DM
