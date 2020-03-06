#pragma once

#include <optional>

namespace DB::stable
{

/// Treat a list of PageEntriesForDelta as a single PageEntries
class PageEntriesView
{
private:
    // tail of the list
    std::shared_ptr<PageEntriesForDelta> tail;

public:
    explicit PageEntriesView(std::shared_ptr<PageEntriesForDelta> tail_) : tail(std::move(tail_)) {}

    std::optional<PageEntry> find(PageId page_id) const;

    const PageEntry at(PageId page_id) const;

    std::pair<bool, PageId> isRefId(PageId page_id) const;

    // For iterate over all pages
    std::set<PageId> validPageIds() const;

    // For iterate over all normal pages
    std::set<PageId>         validNormalPageIds() const;
    std::optional<PageEntry> findNormalPageEntry(PageId page_id) const;

    PageId maxId() const;

    inline std::shared_ptr<PageEntriesForDelta> getSharedTailVersion() const { return tail; }

    inline std::shared_ptr<PageEntriesForDelta> transferTailVersionOwn()
    {
        std::shared_ptr<PageEntriesForDelta> owned_ptr;
        owned_ptr.swap(tail);
        return owned_ptr;
    }

    size_t numPages() const;
    size_t numNormalPages() const;

private:
    PageId resolveRefId(PageId page_id) const;

    friend class DeltaVersionEditAcceptor;
};

} // namespace DB::stable
