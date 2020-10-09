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

const RowKeyValue RowKeyValue::INT_HANDLE_MIN_KEY = RowKeyValue(false, std::make_shared<String>(getIntHandleMinKey()), int_handle_min);
const RowKeyValue RowKeyValue::INT_HANDLE_MAX_KEY = RowKeyValue(false, std::make_shared<String>(getIntHandleMaxKey()), int_handle_max);
const RowKeyValue RowKeyValue::EMPTY_STRING_KEY   = RowKeyValue(true, std::make_shared<String>(""), 0);

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

std::unordered_map<size_t, RowKeyRange::CommonHandleRangeMinMax> RowKeyRange::min_max_data = {
    {1, RowKeyRange::CommonHandleRangeMinMax(1)},
    {2, RowKeyRange::CommonHandleRangeMinMax(2)},
    {3, RowKeyRange::CommonHandleRangeMinMax(3)},
    {4, RowKeyRange::CommonHandleRangeMinMax(4)},
    {5, RowKeyRange::CommonHandleRangeMinMax(5)},
    {6, RowKeyRange::CommonHandleRangeMinMax(6)},
    {7, RowKeyRange::CommonHandleRangeMinMax(7)},
    {8, RowKeyRange::CommonHandleRangeMinMax(8)},
    {9, RowKeyRange::CommonHandleRangeMinMax(9)},
    {10, RowKeyRange::CommonHandleRangeMinMax(10)},
};
std::mutex RowKeyRange::mutex;

const RowKeyRange::CommonHandleRangeMinMax & RowKeyRange::getMinMaxData(size_t rowkey_column_size)
{
    if (auto it = min_max_data.find(rowkey_column_size); it != min_max_data.end())
        return it->second;
    std::lock_guard<std::mutex> lock(mutex);
    return min_max_data.try_emplace(rowkey_column_size, rowkey_column_size).first->second;
}

std::unordered_map<TableID, RowKeyRange::TableRangeMinMax> RowKeyRange::table_min_max_data;
std::mutex                                                 RowKeyRange::table_mutex;

const RowKeyRange::TableRangeMinMax & RowKeyRange::getTableMinMaxData(TableID table_id, bool is_common_handle, size_t rowkey_column_size)
{
    if (auto it = table_min_max_data.find(table_id); it != table_min_max_data.end())
        return it->second;
    std::lock_guard<std::mutex> lock(table_mutex);
    return table_min_max_data.try_emplace(table_id, table_id, is_common_handle, rowkey_column_size).first->second;
}

} // namespace DB::DM
