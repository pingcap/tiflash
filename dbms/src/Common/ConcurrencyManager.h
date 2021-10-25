#pragma once

#include <Common/Exception.h>
#include <fmt/core.h>
#include <mutex>
#include <thread>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ConcurrencyManager
{
public:
    static ConcurrencyManager & instance()
    {
        static ConcurrencyManager manager;
        return manager;
    }

    size_t apply(size_t concurrency)
    {
        std::unique_lock lock(mu);
        if (current + concurrency < full_speed_capacity)
            return applyInternal(concurrency);
        if (current + concurrency < half_speed_capacity)
            return applyInternal(concurrency / 2);
        if (current + concurrency < capacity)
            return applyInternal(concurrency / 4);
        return applyInternal(1);
    }

    void release(size_t concurrency)
    {
        std::unique_lock lock(mu);
        if (current < concurrency)
            throw Exception(fmt::format("Invalid concurrency. current: {}, release: {}.", current, concurrency), ErrorCodes::LOGICAL_ERROR);
    }
private:
    ConcurrencyManager()
        : capacity(20 * std::thread::hardware_concurrency())
        , full_speed_capacity(capacity * 0.5)
        , half_speed_capacity(capacity * 0.8)
    {
    }

    size_t applyInternal(size_t concurrency)
    {
        concurrency = std::max<size_t>(concurrency, 1);
        current += concurrency;
        return concurrency;
    }

    std::mutex mu;
    const size_t capacity;
    const size_t full_speed_capacity;
    const size_t half_speed_capacity;
    size_t current = 0;
};
} // namespace DB

