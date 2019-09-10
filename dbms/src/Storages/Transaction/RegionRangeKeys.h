#pragma once

#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/TiKVKeyValue.h>

namespace DB
{

class RegionRangeKeys : boost::noncopyable
{
public:
    const std::pair<TiKVKey, TiKVKey> & keys() const;
    const std::pair<DecodedTiKVKey, DecodedTiKVKey> & rawKeys() const;
    HandleRange<HandleID> getHandleRangeByTable(const TableID table_id) const;
    explicit RegionRangeKeys(TiKVKey start_key, TiKVKey end_key);

private:
    std::pair<TiKVKey, TiKVKey> ori;
    std::pair<DecodedTiKVKey, DecodedTiKVKey> raw;
};

} // namespace DB
