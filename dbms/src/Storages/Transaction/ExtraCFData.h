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
    using Queue = std::deque<std::shared_ptr<const TiKVValue>>;
    mutable std::mutex default_cf_decode_mutex;

    void add(const std::shared_ptr<const TiKVValue> & e)
    {
        std::lock_guard<std::mutex> lock(default_cf_decode_mutex);
        queue.push_back(e);
    }

    std::optional<Queue> popAll()
    {
        std::lock_guard<std::mutex> lock(default_cf_decode_mutex);
        if (queue.empty())
            return {};

        Queue res;
        queue.swap(res);
        return res;
    }

private:
    Queue queue;
};

} // namespace DB
