// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/RedactHelpers.h>
#include <Common/SharedMutexProtected.h>
#include <Storages/DeltaMerge/RowKeyRange.h>

namespace DB::DM
{
const Int64 int_handle_min = std::numeric_limits<HandleID>::min();
const Int64 int_handle_max = std::numeric_limits<HandleID>::max();

String getIntHandleMinKey()
{
    WriteBufferFromOwnString ss;
    DB::EncodeInt64(int_handle_min, ss);
    return ss.releaseStr();
}

String getIntHandleMaxKey()
{
    WriteBufferFromOwnString ss;
    DB::EncodeInt64(int_handle_max, ss);
    ss.write('\0');
    return ss.releaseStr();
}

const RowKeyValue RowKeyValue::INT_HANDLE_MIN_KEY
    = RowKeyValue(false, std::make_shared<String>(getIntHandleMinKey()), int_handle_min);
const RowKeyValue RowKeyValue::INT_HANDLE_MAX_KEY
    = RowKeyValue(false, std::make_shared<String>(getIntHandleMaxKey()), int_handle_max);
const RowKeyValue RowKeyValue::COMMON_HANDLE_MIN_KEY
    = RowKeyValue(true, std::make_shared<String>(1, TiDB::CodecFlag::CodecFlagBytes), 0);
const RowKeyValue RowKeyValue::COMMON_HANDLE_MAX_KEY
    = RowKeyValue(true, std::make_shared<String>(1, TiDB::CodecFlag::CodecFlagMax), 0);
const RowKeyValue RowKeyValue::EMPTY_STRING_KEY = RowKeyValue(true, std::make_shared<String>(""), 0);

RowKeyValue RowKeyValueRef::toRowKeyValue() const
{
    if (data == nullptr)
    {
        WriteBufferFromOwnString ss;
        DB::EncodeInt64(int_value, ss);
        return RowKeyValue(is_common_handle, std::make_shared<String>(ss.releaseStr()), int_value);
    }
    else
    {
        return RowKeyValue(is_common_handle, std::make_shared<String>(data, size), int_value);
    }
}

SharedMutexProtected<std::unordered_map<KeyspaceTableID, RowKeyRange::TableRangeMinMax, boost::hash<KeyspaceTableID>>>
    RowKeyRange::table_min_max_data;

const RowKeyRange::TableRangeMinMax & RowKeyRange::getTableMinMaxData(
    KeyspaceID keyspace_id,
    TableID table_id,
    bool is_common_handle)
{
    auto keyspace_table_id = KeyspaceTableID{keyspace_id, table_id};
    {
        auto lock = table_min_max_data.lockShared();
        if (auto it = lock->find(keyspace_table_id); it != lock->end())
            return it->second;
    }
    auto lock = table_min_max_data.lockExclusive();
    return lock->try_emplace(keyspace_table_id, keyspace_id, table_id, is_common_handle).first->second;
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

namespace
{
// https://en.cppreference.com/w/cpp/algorithm/lower_bound
size_t lowerBound(const RowKeyColumnContainer & rowkey_column, size_t first, size_t last, const RowKeyValueRef & value)
{
    size_t count = last - first;
    while (count > 0)
    {
        size_t step = count / 2;
        size_t index = first + step;
        if (value > rowkey_column.getRowKeyValue(index))
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

std::pair<size_t, size_t> RowKeyRange::getPosRange(const ColumnPtr & column, const size_t offset, const size_t limit)
    const
{
    RowKeyColumnContainer rowkey_column(column, is_common_handle);
    size_t start_index = check(rowkey_column.getRowKeyValue(offset))
        ? offset
        : lowerBound(rowkey_column, offset, offset + limit, getStart());
    size_t end_index = check(rowkey_column.getRowKeyValue(offset + limit - 1))
        ? offset + limit
        : lowerBound(rowkey_column, offset, offset + limit, getEnd());
    return {start_index, end_index - start_index};
}

} // namespace DB::DM
