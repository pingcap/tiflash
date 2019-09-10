#pragma once

#include <Storages/Transaction/TiKVRecordFormat.h>

namespace DB
{

/// TableRowIDMinMax is used to store the min/max key for specific table.
struct TableRowIDMinMax
{
    TableRowIDMinMax(const TableID table_id)
        : handle_min(RecordKVFormat::genRawKey(table_id, std::numeric_limits<HandleID>::min())),
          handle_max(RecordKVFormat::genRawKey(table_id, std::numeric_limits<HandleID>::max()))
    {}

    /// Make this struct can't be copied or moved.
    TableRowIDMinMax(const TableRowIDMinMax &) = delete;
    TableRowIDMinMax(TableRowIDMinMax &&) = delete;

    const DecodedTiKVKey handle_min;
    const DecodedTiKVKey handle_max;

    /// It's a long lived object, so just return const ref directly.
    static const TableRowIDMinMax & getMinMax(const TableID table_id);

private:
    static std::unordered_map<TableID, TableRowIDMinMax> data;
    static std::mutex mutex;
};

} // namespace DB
