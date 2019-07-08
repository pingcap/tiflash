#pragma once

namespace DB
{

class Snapshot
{
public:
    Snapshot(PageEntryMap *curr, std::shared_mutex *mutex_): current(curr), mutex(mutex_) {
        current->incrRefCount();
    }

    ~Snapshot() {
        current->decrRefCount(*mutex);
    }

    PageEntryMap *pageMap() const {
        return current;
    }

private:
    PageEntryMap *current;
    std::shared_mutex *mutex;
};

using SnapshotPtr = std::shared_ptr<Snapshot>;

} // namespace DB
