#include <Storages/Transaction/TableRowIDMinMax.h>

namespace DB
{

std::unordered_map<TableID, TableRowIDMinMax> TableRowIDMinMax::data;
std::mutex TableRowIDMinMax::mutex;

const TableRowIDMinMax & TableRowIDMinMax::getMinMax(const TableID table_id)
{
    std::lock_guard<std::mutex> lock(mutex);

    if (auto it = data.find(table_id); it != data.end())
        return it->second;
    return data.try_emplace(table_id, table_id).first->second;
}

} // namespace DB
