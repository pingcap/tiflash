#pragma once

namespace DB
{

/// Ignoring all keys other than records.
inline TableID checkRecordAndValidTable(const DecodedTiKVKey & raw_key)
{
    // Ignoring all keys other than records.
    if (!RecordKVFormat::isRecord(raw_key))
        return InvalidTableID;

    auto table_id = RecordKVFormat::getTableId(raw_key);
    if (isTiDBSystemTable(table_id))
        return InvalidTableID;

    return table_id;
}

void tryPreDecodeTiKVValue(std::optional<ExtraCFDataQueue> && values)
{
    if (!values)
        return;

    for (const auto & val : *values)
    {
        auto & decoded_row_info = val->extraInfo();
        if (decoded_row_info.load())
            continue;
        DecodedRow * decoded_row = ValueExtraInfo<>::computeDecodedRow(val->getStr());
        decoded_row_info.atomicUpdate(decoded_row);
    }
}

const metapb::Peer & findPeer(const metapb::Region & region, UInt64 store_id)
{
    for (const auto & peer : region.peers())
    {
        if (peer.store_id() == store_id)
            return peer;
    }
    throw Exception("[findPeer] peer with store_id " + DB::toString(store_id) + " not found", ErrorCodes::LOGICAL_ERROR);
}

} // namespace DB
