#pragma once

namespace DB
{

/// Treat a list of PageEntriesForDelta as a single PageEntries
class PageEntriesView
{
private:
    // tail of the list
    std::shared_ptr<PageEntriesForDelta> tail;

public:
    explicit PageEntriesView(std::shared_ptr<PageEntriesForDelta> tail_) : tail(std::move(tail_)) {}

    const PageEntry * find(PageId page_id) const;

    const PageEntry & at(PageId page_id) const;

    std::pair<bool, PageId> isRefId(PageId page_id) const;

    // For iterate over all pages
    std::set<PageId> validPageIds() const;

    // For iterate over all normal pages
    std::set<PageId> validNormalPageIds() const;

    PageId maxId() const;

    inline std::shared_ptr<PageEntriesForDelta> getSharedTailVersion() const { return tail; }

    inline std::shared_ptr<PageEntriesForDelta> transferTailVersionOwn()
    {
        std::shared_ptr<PageEntriesForDelta> owned_ptr;
        owned_ptr.swap(tail);
        return owned_ptr;
    }

private:
    const PageEntry * findNormalPageEntry(PageId page_id) const;

    PageId resolveRefId(PageId page_id) const;

    friend class DeltaVersionEditAcceptor;
};

} // namespace DB
