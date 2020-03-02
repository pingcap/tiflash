#pragma once

#include <optional>

namespace DB
{

/// Treat a list of PageEntriesForDelta as a single PageEntries
class PageEntriesView
{
private:
    // tail of the list
    PageEntriesForDeltaPtr tail;

public:
    explicit PageEntriesView(PageEntriesForDeltaPtr tail_) : tail(std::move(tail_)) {}

    std::optional<PageEntry> find(PageId page_id) const;

    const PageEntry at(PageId page_id) const;

    std::pair<bool, PageId> isRefId(PageId page_id) const;

    // For iterate over all pages
    std::set<PageId> validPageIds() const;

    // For iterate over all normal pages
    std::set<PageId>         validNormalPageIds() const;
    std::optional<PageEntry> findNormalPageEntry(PageId page_id) const;

    PageId maxId() const;

    inline PageEntriesForDeltaPtr getSharedTailVersion() const { return tail; }

    void release() { tail.reset(); }

    size_t numPages() const;
    size_t numNormalPages() const;

private:
    PageId resolveRefId(PageId page_id) const;

    friend class DeltaVersionEditAcceptor;
};

} // namespace DB
