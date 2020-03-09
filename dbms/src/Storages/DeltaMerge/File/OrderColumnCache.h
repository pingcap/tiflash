#pragma once

#include <unordered_map>


#include <Columns/IColumn.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{
namespace DM
{

class OrderColumnCache
{
private:
    using ColumnMap = std::unordered_map<UInt64, ColumnPtr>;
    ColumnMap column_map;

public:
    ColumnPtr get(size_t pack_id, ColId col_id)
    {
        SipHash hash;
        hash.update(pack_id);
        hash.update(col_id);
        UInt64 key = hash.get64();
        auto   it  = column_map.find(key);
        if (it == column_map.end())
            return {};
        else
            return it->second;
    }

    void put(size_t pack_id, ColId col_id, const ColumnPtr & col)
    {
        SipHash hash;
        hash.update(pack_id);
        hash.update(col_id);
        UInt64 key = hash.get64();
        column_map.emplace(key, col);
    }
};

using OrderColumnCachePtr = std::shared_ptr<OrderColumnCache>;

} // namespace DM
} // namespace DB