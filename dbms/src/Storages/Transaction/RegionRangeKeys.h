#pragma once

#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/TiKVKeyValue.h>

namespace DB
{

struct TiKVRangeKey : boost::noncopyable
{
    enum State : Int8
    {
        MIN = 1,
        NORMAL = 2,
        MAX = 4,
    };

    int compare(const TiKVKey & tar) const;
    int compare(const TiKVRangeKey & tar) const;

    template <bool is_start>
    static TiKVRangeKey makeTiKVRangeKey(TiKVKey &&);

    TiKVRangeKey(State state_, TiKVKey && key_);
    TiKVRangeKey(TiKVRangeKey &&);

    TiKVRangeKey copy() const;
    TiKVRangeKey & operator=(TiKVRangeKey &&);

    State state;
    TiKVKey key;
};

class RegionRangeKeys : boost::noncopyable
{
public:
    using RegionRange = std::pair<TiKVRangeKey, TiKVRangeKey>;

    const RegionRange & comparableKeys() const;
    static RegionRange makeComparableKeys(TiKVKey && start_key, TiKVKey && end_key);
    const std::pair<DecodedTiKVKey, DecodedTiKVKey> & rawKeys() const;
    HandleRange<HandleID> getHandleRangeByTable(const TableID table_id) const;
    explicit RegionRangeKeys(TiKVKey && start_key, TiKVKey && end_key);
    TableID getFlashTableID() const;

private:
    RegionRange ori;
    std::pair<DecodedTiKVKey, DecodedTiKVKey> raw;
    TableID flash_table_id;
    HandleRange<HandleID> flash_handle_range;
};

} // namespace DB
