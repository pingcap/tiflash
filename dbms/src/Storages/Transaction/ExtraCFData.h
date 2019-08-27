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
    mutable std::mutex default_cf_decode_mutex;

    ExtraCFData() = default;

    void add(const std::shared_ptr<const TiKVValue> & e)
    {
        std::lock_guard<std::mutex> lock(default_cf_decode_mutex);
        queue.push_back(e);
    }

    std::optional<ExtraCFDataQueue> popAll()
    {
        std::lock_guard<std::mutex> lock(default_cf_decode_mutex);
        if (queue.empty())
            return {};

        ExtraCFDataQueue res;
        queue.swap(res);
        return res;
    }

    ExtraCFData(const ExtraCFData & src) = delete;

    ExtraCFData(ExtraCFData && src) { mergeFrom(src); }

    ExtraCFData & operator=(ExtraCFData && src)
    {
        mergeFrom(src);
        return *this;
    }

private:
    void mergeFrom(ExtraCFData & src)
    {
        auto res = src.popAll();
        if (res)
        {
            std::lock_guard<std::mutex> lock(default_cf_decode_mutex);
            for (auto && e : *res)
                queue.emplace_back(std::move(e));
        }
    }

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
