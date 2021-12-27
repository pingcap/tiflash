#pragma once

#include <Common/MemoryTracker.h>
#include <boost/noncopyable.hpp>

namespace DB
{
class MemoryTrackerSetter : private boost::noncopyable
{
public:
    MemoryTrackerSetter(bool enable_, MemoryTracker * memory_tracker)
        : enable(enable_)
        , old_memory_tracker(current_memory_tracker)
    {
        if (enable)
            current_memory_tracker = memory_tracker;
    }

    ~MemoryTrackerSetter()
    {
        if (enable)
            current_memory_tracker = old_memory_tracker;
    }

private:
    bool enable;
    MemoryTracker * old_memory_tracker;
};
} // namespace

