#pragma once

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>


class EventRecorder
{
public:
    EventRecorder(ProfileEvents::Event event_, ProfileEvents::Event event_elapsed_) : event(event_), event_elapsed(event_elapsed_), watch()
    {
        watch.start();
    }

    inline void submit()
    {
        ProfileEvents::increment(event);
        ProfileEvents::increment(event_elapsed, watch.elapsed());
    }

private:
    ProfileEvents::Event event;
    ProfileEvents::Event event_elapsed;

    Stopwatch watch;
};