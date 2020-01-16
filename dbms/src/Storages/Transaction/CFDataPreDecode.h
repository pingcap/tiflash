#pragma once

#include <deque>
#include <optional>

#include <Storages/Transaction/TiKVKeyValue.h>

namespace DB
{

struct RegionDefaultCFDataTrait;
struct RegionWriteCFDataTrait;

using CFDataPreDecodeQueue = std::deque<std::shared_ptr<const TiKVValue>>;

template <typename Trait>
struct CFDataPreDecode
{
};

template <>
struct CFDataPreDecode<RegionDefaultCFDataTrait>
{
    CFDataPreDecode() = default;

    void add(const std::shared_ptr<const TiKVValue> & e) { queue.push_back(e); }

    std::optional<CFDataPreDecodeQueue> popAll()
    {
        if (queue.empty())
            return {};

        CFDataPreDecodeQueue res;
        queue.swap(res);
        return res;
    }

    CFDataPreDecode(const CFDataPreDecode & src) = delete;

private:
    CFDataPreDecodeQueue queue;
};

template <>
struct CFDataPreDecode<RegionWriteCFDataTrait> : CFDataPreDecode<RegionDefaultCFDataTrait>
{
    using Base = CFDataPreDecode<RegionDefaultCFDataTrait>;
    void add(const std::shared_ptr<const TiKVValue> & e)
    {
        if (!e)
            return;
        Base::add(e);
    }
};

} // namespace DB
