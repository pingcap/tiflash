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

#include <mutex>
#include <shared_mutex>


namespace DB
{
class ProfilingScopedWriteRWLock
{
public:
    ProfilingScopedWriteRWLock(std::shared_mutex & rwl, ProfileEvents::Event event)
        : scoped_write_lock(rwl)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:
    Stopwatch watch;
    std::unique_lock<std::shared_mutex> scoped_write_lock;
};

class ProfilingScopedReadRWLock
{
public:
    ProfilingScopedReadRWLock(std::shared_mutex & rwl, ProfileEvents::Event event)
        : scoped_read_lock(rwl)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:
    Stopwatch watch;
    std::shared_lock<std::shared_mutex> scoped_read_lock;
};

} // namespace DB
