#include <Storages/Transaction/RegionCFDataBase.h>
#include <Storages/Transaction/RegionCFDataTrait.h>

namespace DB
{

template <typename Trait>
const TiKVKey & RegionCFDataBase<Trait>::getTiKVKey(const Value & val)
{
    return *std::get<0>(val);
}

template <typename Value>
const std::shared_ptr<const TiKVValue> & getTiKVValuePtr(const Value & val)
{
    return std::get<1>(val);
}

template <typename Trait>
const TiKVValue & RegionCFDataBase<Trait>::getTiKVValue(const Value & val)
{
    return *getTiKVValuePtr<Value>(val);
}

template <typename Trait>
TableID RegionCFDataBase<Trait>::insert(TiKVKey && key, TiKVValue && value)
{
    const String & raw_key = RecordKVFormat::decodeTiKVKey(key);
    return insert(std::move(key), std::move(value), raw_key);
}

template <typename Trait>
TableID RegionCFDataBase<Trait>::insert(TiKVKey && key, TiKVValue && value, const String & raw_key)
{
    Pair kv_pair = Trait::genKVPair(std::move(key), raw_key, std::move(value));
    if (shouldIgnoreInsert(kv_pair.second))
        return InvalidTableID;

    return insert(RecordKVFormat::getTableId(raw_key), std::move(kv_pair));
}

template <typename Trait>
TableID RegionCFDataBase<Trait>::insert(const TableID table_id, std::pair<Key, Value> && kv_pair)
{
    auto & map = data[table_id];
    auto [it, ok] = map.emplace(std::move(kv_pair));
    if (!ok)
        throw Exception("Found existing key in hex: " + getTiKVKey(it->second).toHex(), ErrorCodes::LOGICAL_ERROR);

    if constexpr (std::is_same_v<Trait, RegionWriteCFDataTrait>)
        extra.add(Trait::getRowRawValuePtr(it->second));
    else
        extra.add(getTiKVValuePtr(it->second));
    return table_id;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::calcTiKVKeyValueSize(const Value & value)
{
    return calcTiKVKeyValueSize(getTiKVKey(value), getTiKVValue(value));
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::calcTiKVKeyValueSize(const TiKVKey & key, const TiKVValue & value)
{
    if constexpr (std::is_same<Trait, RegionLockCFDataTrait>::value)
        return 0;
    else
        return key.dataSize() + value.dataSize();
}


template <typename Trait>
bool RegionCFDataBase<Trait>::shouldIgnoreRemove(const RegionCFDataBase::Value &)
{
    return false;
}

template <>
bool RegionCFDataBase<RegionWriteCFDataTrait>::shouldIgnoreRemove(const RegionCFDataBase::Value & value)
{
    return RegionWriteCFDataTrait::getWriteType(value) == CFModifyFlag::DelFlag;
}

template <typename Trait>
bool RegionCFDataBase<Trait>::shouldIgnoreInsert(const RegionCFDataBase::Value &)
{
    return false;
}

template <>
bool RegionCFDataBase<RegionWriteCFDataTrait>::shouldIgnoreInsert(const RegionCFDataBase::Value & value)
{
    // only keep records with DelFlag or PutFlag.
    const auto flag = RegionWriteCFDataTrait::getWriteType(value);
    return flag != CFModifyFlag::DelFlag && flag != CFModifyFlag::PutFlag;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::remove(TableID table_id, const Key & key, bool quiet)
{
    auto & map = data[table_id];

    if (auto it = map.find(key); it != map.end())
    {
        const Value & value = it->second;

        if (shouldIgnoreRemove(value))
            return 0;

        size_t size = calcTiKVKeyValueSize(value);
        map.erase(it);
        return size;
    }
    else if (!quiet)
        throw Exception("Key not found", ErrorCodes::LOGICAL_ERROR);

    return 0;
}

template <typename Trait>
bool RegionCFDataBase<Trait>::cmp(const Map & a, const Map & b)
{
    if (a.size() != b.size())
        return false;
    for (const auto & [key, value] : a)
    {
        if (auto it = b.find(key); it != b.end())
        {
            if (getTiKVKey(value) != getTiKVKey(it->second) || getTiKVValue(value) != getTiKVValue(it->second))
                return false;
        }
        else
            return false;
    }
    return true;
}

template <typename Trait>
bool RegionCFDataBase<Trait>::operator==(const RegionCFDataBase & cf) const
{
    if (getSize() != cf.getSize())
        return false;

    const auto & cf_data = cf.data;
    for (const auto & [table_id, map] : data)
    {
        if (map.empty())
            continue;

        if (auto it = cf_data.find(table_id); it != cf_data.end())
        {
            if (!cmp(map, it->second))
                return false;
        }
        else
            return false;
    }
    return true;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::getSize() const
{
    size_t size = 0;
    for (auto data_it = data.begin(); data_it != data.end(); ++data_it)
        size += data_it->second.size();
    return size;
}

template <typename Trait>
RegionCFDataBase<Trait>::RegionCFDataBase(RegionCFDataBase && region) : data(std::move(region.data)), extra(std::move(region.extra))
{}

template <typename Trait>
RegionCFDataBase<Trait> & RegionCFDataBase<Trait>::operator=(RegionCFDataBase && region)
{
    data = std::move(region.data);
    extra = std::move(region.extra);
    return *this;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::splitInto(const RegionRange & range, RegionCFDataBase & new_region_data)
{
    const auto & [start_key, end_key] = range;
    size_t size_changed = 0;

    for (auto data_it = data.begin(); data_it != data.end();)
    {
        const auto & table_id = data_it->first;
        auto & ori_map = data_it->second;
        if (ori_map.empty())
        {
            data_it = data.erase(data_it);
            continue;
        }

        auto & tar_map = new_region_data.data[table_id];

        for (auto it = ori_map.begin(); it != ori_map.end();)
        {
            const auto & key = getTiKVKey(it->second);

            bool ok = start_key ? key >= start_key : true;
            ok = ok && (end_key ? key < end_key : true);
            if (ok)
            {
                size_changed += calcTiKVKeyValueSize(it->second);
                tar_map.insert(std::move(*it));
                it = ori_map.erase(it);
            }
            else
                ++it;
        }

        ++data_it;
    }
    return size_changed;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::serialize(WriteBuffer & buf) const
{
    size_t total_size = 0;

    size_t size = getSize();

    total_size += writeBinary2(size, buf);

    for (const auto & [table_id, map] : data)
    {
        std::ignore = table_id;
        for (const auto & ele : map)
        {
            const auto & key = getTiKVKey(ele.second);
            const auto & value = getTiKVValue(ele.second);
            total_size += key.serialize(buf);
            total_size += value.serialize(buf);
        }
    }

    return total_size;
}

template <typename Trait>
size_t RegionCFDataBase<Trait>::deserialize(ReadBuffer & buf, RegionCFDataBase & new_region_data)
{
    size_t size = readBinary2<size_t>(buf);
    size_t cf_data_size = 0;
    for (size_t i = 0; i < size; ++i)
    {
        auto key = TiKVKey::deserialize(buf);
        auto value = TiKVValue::deserialize(buf);
        const auto size = calcTiKVKeyValueSize(key, value);
        new_region_data.insert(std::move(key), std::move(value));
        cf_data_size += size;
    }
    return cf_data_size;
}

template <typename Trait>
TableIDSet RegionCFDataBase<Trait>::getAllTables() const
{
    TableIDSet tables;
    for (const auto & [table_id, map] : data)
    {
        if (map.empty())
            continue;
        tables.insert(table_id);
    }
    return tables;
}

template <typename Trait>
const typename RegionCFDataBase<Trait>::Data & RegionCFDataBase<Trait>::getData() const
{
    return data;
}

template <typename Trait>
typename RegionCFDataBase<Trait>::Data & RegionCFDataBase<Trait>::getDataMut()
{
    return data;
}

template <typename Trait>
void RegionCFDataBase<Trait>::deleteRange(const TiKVKey & start_key, const TiKVKey & end_key)
{
    for (auto data_it = data.begin(); data_it != data.end();)
    {
        auto & ori_map = data_it->second;

        for (auto it = ori_map.begin(); it != ori_map.end();)
        {
            const auto & key = getTiKVKey(it->second);

            bool ok = start_key ? key >= start_key : true;
            ok = ok && (end_key ? key < end_key : true);
            if (ok)
                it = ori_map.erase(it);
            else
                ++it;
        }

        if (ori_map.empty())
            data_it = data.erase(data_it);
        else
            ++data_it;
    }
}

template <typename Trait>
ExtraCFData<Trait> & RegionCFDataBase<Trait>::getExtra()
{
    return extra;
}

template struct RegionCFDataBase<RegionWriteCFDataTrait>;
template struct RegionCFDataBase<RegionDefaultCFDataTrait>;
template struct RegionCFDataBase<RegionLockCFDataTrait>;

} // namespace DB
