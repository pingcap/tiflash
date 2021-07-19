#pragma once
#ifndef NDEBUG

#include <Storages/Page/Page.h>
#include <vector>

namespace DB::tests
{
class MockEntries
{
private:
    std::map<PageId, PageEntry> entries;

public:
    void put(PageId page_id, PageEntry entry) { entries[page_id] = entry; }

    std::set<PageId> validNormalPageIds() const
    {
        std::set<PageId> ids;
        for (auto it = entries.begin(); it != entries.end(); ++it)
            ids.insert(it->first);
        return ids;
    }

    std::optional<PageEntry> findNormalPageEntry(PageId page_id) const
    {
        if (auto it = entries.find(page_id); it != entries.end())
            return it->second;
        else
            return std::nullopt;
    }
};

class MockSnapshot;
using MockSnapshotPtr = std::shared_ptr<MockSnapshot>;
class MockSnapshot
{
private:
    std::shared_ptr<MockEntries> entries;

public:
    MockSnapshot() : entries(std::make_shared<MockEntries>()) {}
    std::shared_ptr<MockEntries> version() { return entries; }

    static MockSnapshotPtr createFrom(std::vector<std::pair<PageId, PageEntry>> && entries)
    {
        auto snap = std::make_shared<MockSnapshot>();
        for (const auto & [pid, entry]:entries)
        {
            snap->entries->put(pid, entry);
        }
        return snap;
    }
};

} // namespace DB::tests

#endif // NDEBUG
