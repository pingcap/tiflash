#pragma once

#include <Common/MemoryTracker.h>

#include <boost/noncopyable.hpp>

namespace DB
{
// MemoryTrackerSetter is a guard for `current_memory_tracker`.
// It ensures `current_memory_tracker` unchanged after leaving the scope of
// where MemoryTrackerSetter guards.
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
        current_memory_tracker = old_memory_tracker;
    }

private:
    bool enable;
    MemoryTracker * old_memory_tracker;
};
} // namespace DB
