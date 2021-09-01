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
        : watch()
        , scoped_write_lock(rwl)
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
        : watch()
        , scoped_read_lock(rwl)
    {
        ProfileEvents::increment(event, watch.elapsed());
    }

private:
    Stopwatch watch;
    std::shared_lock<std::shared_mutex> scoped_read_lock;
};

} // namespace DB
