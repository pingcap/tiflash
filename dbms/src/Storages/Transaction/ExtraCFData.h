#pragma once

#include <optional>

#include <Storages/Transaction/TiKVKeyValue.h>

namespace DB
{

struct RegionDefaultCFDataTrait;
struct RegionWriteCFDataTrait;

using ExtraCFDataQueue = std::deque<std::shared_ptr<const TiKVValue>>;

template <typename Trait>
struct ExtraCFData
{
    void add(const std::shared_ptr<const TiKVValue> &) {}
};

template <>
struct ExtraCFData<RegionDefaultCFDataTrait>
{
    ExtraCFData() = default;

    void add(const std::shared_ptr<const TiKVValue> & e) { queue.push_back(e); }

    std::optional<ExtraCFDataQueue> popAll()
    {
        if (queue.empty())
            return {};

        ExtraCFDataQueue res;
        queue.swap(res);
        return res;
    }

    ExtraCFData(const ExtraCFData & src) = delete;

private:
    ExtraCFDataQueue queue;
};

template <>
struct ExtraCFData<RegionWriteCFDataTrait> : ExtraCFData<RegionDefaultCFDataTrait>
{
    using Base = ExtraCFData<RegionDefaultCFDataTrait>;
    void add(const std::shared_ptr<const TiKVValue> & e)
    {
        if (!e)
            return;
        Base::add(e);
    }
};

} // namespace DB
