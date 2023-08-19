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

#include <stddef.h>

#include <atomic>


/** Implements global counters for various events happening in the application
  *  - for high level profiling.
  * See .cpp for list of events.
  */

namespace ProfileEvents
{
/// Event identifier (index in array).
using Event = size_t;
using Count = size_t;

/// Get text description of event by identifier. Returns statically allocated string.
const char * getDescription(Event event);

/// Counters - how many times each event happened.
extern std::atomic<Count> counters[];

/// Increment a counter for event. Thread-safe.
inline void increment(Event event, Count amount = 1)
{
    counters[event].fetch_add(amount, std::memory_order_relaxed);
}

inline Count get(Event event)
{
    return counters[event].load();
}

/// Get index just after last event identifier.
Event end();
} // namespace ProfileEvents
