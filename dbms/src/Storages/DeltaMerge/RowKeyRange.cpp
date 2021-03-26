#include <Common/RedactHelpers.h>
#include <Storages/DeltaMerge/RowKeyRange.h>

namespace DB::DM
{

const Int64 int_handle_min = std::numeric_limits<HandleID>::min();
const Int64 int_handle_max = std::numeric_limits<HandleID>::max();

String getIntHandleMinKey()
{
    std::stringstream ss;
    DB::EncodeInt64(int_handle_min, ss);
    return ss.str();
}

String getIntHandleMaxKey()
{
    std::stringstream ss;
    DB::EncodeInt64(int_handle_max, ss);
    ss.put('\0');
    return ss.str();
}

const RowKeyValue RowKeyValue::INT_HANDLE_MIN_KEY    = RowKeyValue(false, std::make_shared<String>(getIntHandleMinKey()), int_handle_min);
const RowKeyValue RowKeyValue::INT_HANDLE_MAX_KEY    = RowKeyValue(false, std::make_shared<String>(getIntHandleMaxKey()), int_handle_max);
const RowKeyValue RowKeyValue::COMMON_HANDLE_MIN_KEY = RowKeyValue(true, std::make_shared<String>(1, TiDB::CodecFlag::CodecFlagBytes), 0);
const RowKeyValue RowKeyValue::COMMON_HANDLE_MAX_KEY = RowKeyValue(true, std::make_shared<String>(1, TiDB::CodecFlag::CodecFlagMax), 0);
const RowKeyValue RowKeyValue::EMPTY_STRING_KEY      = RowKeyValue(true, std::make_shared<String>(""), 0);

RowKeyValue RowKeyValueRef::toRowKeyValue() const
{
    if (data == nullptr)
    {
        std::stringstream ss;
        DB::EncodeInt64(int_value, ss);
        return RowKeyValue(is_common_handle, std::make_shared<String>(ss.str()), int_value);
    }
    else
    {
        return RowKeyValue(is_common_handle, std::make_shared<String>(data, size), int_value);
    }
}

std::unordered_map<TableID, RowKeyRange::TableRangeMinMax> RowKeyRange::table_min_max_data;
std::shared_mutex                                          RowKeyRange::table_mutex;

const RowKeyRange::TableRangeMinMax & RowKeyRange::getTableMinMaxData(TableID table_id, bool is_common_handle)
{
    {
        std::shared_lock lock(table_mutex);
        if (auto it = table_min_max_data.find(table_id); it != table_min_max_data.end())
            return it->second;
    }
    std::unique_lock lock(table_mutex);
    return table_min_max_data.try_emplace(table_id, table_id, is_common_handle).first->second;
}

template <bool enable_redact, bool right_open = true>
inline String rangeToString(const RowKeyValue & start, const RowKeyValue & end)
{
    String s = "[";
    if constexpr (enable_redact)
        s += start.toDebugString() + "," + end.toDebugString();
    else
        s += start.toString() + "," + end.toString();

    if constexpr (right_open)
        s += ")";
    else
        s += "]";
    return s;
}

template <bool enable_redact>
inline String rangeToString(const RowKeyRange & range)
{
    return rangeToString<enable_redact, true>(range.start, range.end);
}

String RowKeyValueRef::toDebugString() const
{
    if (is_common_handle)
        return Redact::keyToDebugString(data, size);
    return Redact::handleToDebugString(int_value);
}

String RowKeyValue::toString() const
{
    if (is_common_handle)
        return Redact::keyToHexString(value->data(), value->size());
    return DB::toString(int_value);
}

String RowKeyValue::toDebugString() const
{
    if (is_common_handle)
        return Redact::keyToDebugString(value->data(), value->size());
    return Redact::handleToDebugString(int_value);
}

String RowKeyRange::toDebugString() const
{
    return rangeToString</*enable_redact*/ true>(*this);
}

String RowKeyRange::toString() const
{
    return rangeToString</*enable_redact*/ false>(*this);
}

} // namespace DB::DM
