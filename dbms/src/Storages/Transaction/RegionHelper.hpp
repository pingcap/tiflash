#pragma once

namespace DB
{

/// Ignoring all keys other than records.
inline TableID checkRecordAndValidTable(const std::string & raw_key)
{
    // Ignoring all keys other than records.
    if (!RecordKVFormat::isRecord(raw_key))
        return InvalidTableID;

    auto table_id = RecordKVFormat::getTableId(raw_key);
    if (isTiDBSystemTable(table_id))
        return InvalidTableID;

    return table_id;
}

} // namespace DB
