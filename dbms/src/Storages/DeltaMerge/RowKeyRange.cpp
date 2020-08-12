#include <Storages/DeltaMerge/RowKeyRange.h>

namespace DB::DM
{

const Int64 int_handle_min = std::numeric_limits<HandleID>::min();
const Int64 int_handle_max = std::numeric_limits<HandleID>::max();
String      getIntHandleMinKey()
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

const StringPtr int_handle_min_key = std::make_shared<String>(getIntHandleMinKey());
const StringPtr int_handle_max_key = std::make_shared<String>(getIntHandleMaxKey());
const StringPtr empty_string_ptr   = std::make_shared<String>("");

std::unordered_map<size_t, CommonHandleRangeMinMax> RowKeyRange::min_max_data = {
    {1, CommonHandleRangeMinMax(1)},
    {2, CommonHandleRangeMinMax(2)},
    {3, CommonHandleRangeMinMax(3)},
    {4, CommonHandleRangeMinMax(4)},
    {5, CommonHandleRangeMinMax(5)},
    {6, CommonHandleRangeMinMax(6)},
    {7, CommonHandleRangeMinMax(7)},
    {8, CommonHandleRangeMinMax(8)},
    {9, CommonHandleRangeMinMax(9)},
    {10, CommonHandleRangeMinMax(10)},
};
std::mutex RowKeyRange::mutex;

const CommonHandleRangeMinMax & RowKeyRange::getMinMaxData(size_t rowkey_column_size)
{
    if (auto it = min_max_data.find(rowkey_column_size); it != min_max_data.end())
        return it->second;
    std::lock_guard<std::mutex> lock(mutex);
    return min_max_data.try_emplace(rowkey_column_size, rowkey_column_size).first->second;
}

} // namespace DB::DM
