#pragma once

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

/// This class is NOT multi-threads safe!
class EventRecorder
{
public:
    EventRecorder(ProfileEvents::Event event_, ProfileEvents::Event event_elapsed_) : event(event_), event_elapsed(event_elapsed_), watch()
    {
        watch.start();
    }

    ~EventRecorder()
    {
      if (!done) submit();
    }

    inline void submit()
    {
        done = true;
        ProfileEvents::increment(event);
        ProfileEvents::increment(event_elapsed, watch.elapsed());
    }

private:
    ProfileEvents::Event event;
    ProfileEvents::Event event_elapsed;

    Stopwatch watch;
    bool done = false;
};