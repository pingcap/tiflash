// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

/// This class is NOT multi-threads safe!
class EventRecorder
{
public:
    EventRecorder(ProfileEvents::Event event_, ProfileEvents::Event event_elapsed_)
        : event(event_)
        , event_elapsed(event_elapsed_)
    {
        watch.start();
    }

    ~EventRecorder()
    {
        if (!done)
            submit();
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