#pragma once

namespace DB
{

inline size_t tryPreDecodeTiKVValue(std::optional<ExtraCFDataQueue> && values)
{
    if (!values)
        return 0;

    size_t cnt = 0;
    for (const auto & val : *values)
    {
        auto & decoded_row_info = val->extraInfo();
        if (decoded_row_info.load())
            continue;
        DecodedRow * decoded_row = ValueExtraInfo<>::computeDecodedRow(val->getStr());
        decoded_row_info.atomicUpdate(decoded_row);
        cnt++;
    }

    return cnt;
}

inline const metapb::Peer & findPeer(const metapb::Region & region, UInt64 store_id)
{
    for (const auto & peer : region.peers())
    {
        if (peer.store_id() == store_id)
            return peer;
    }
    throw Exception("[findPeer] peer with store_id " + DB::toString(store_id) + " not found", ErrorCodes::LOGICAL_ERROR);
}

} // namespace DB
