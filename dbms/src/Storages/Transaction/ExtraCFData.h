#pragma once

#include <Storages/Transaction/TiKVKeyValue.h>

namespace DB
{

struct RegionDefaultCFDataTrait;

template <typename Trait>
struct ExtraCFData
{
    void add(const std::shared_ptr<const TiKVValue> &) {}
};

template <>
struct ExtraCFData<RegionDefaultCFDataTrait>
{
    std::deque<std::shared_ptr<const TiKVValue>> queue;
    void add(const std::shared_ptr<const TiKVValue> & e) { queue.push_back(e); }
};

} // namespace DB
