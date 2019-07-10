#include <Storages/Page/PageEntryMap.h>

#include <IO/WriteHelpers.h>
#include <common/likely.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <bool is_base>
void PageEntryMap_t<is_base>::put(const PageId page_id, const PageEntry & entry)
{
    if constexpr (!is_base)
    {
        page_deletions.erase(page_id);
    }
    const PageId normal_page_id = resolveRefId(page_id);

    // update ref-pairs
    bool is_new_ref_pair_inserted;
    {
        // add a RefPage to Page
        auto res = page_ref.emplace(page_id, normal_page_id);
        is_new_ref_pair_inserted = res.second;
    }

    auto         ori_iter       = normal_pages.find(normal_page_id);
    if (ori_iter == normal_pages.end())
    {
        // Page{normal_page_id} not exist
        normal_pages[normal_page_id]     = entry;
        normal_pages[normal_page_id].ref = 1;
    }
    else
    {
        // replace ori Page{normal_page_id}'s entry but inherit ref-counting
        const UInt32 page_ref_count      = ori_iter->second.ref;
        normal_pages[normal_page_id]     = entry;
        normal_pages[normal_page_id].ref = page_ref_count + is_new_ref_pair_inserted;
    }
    max_page_id = std::max(max_page_id, page_id);
}


template class PageEntryMap_t<true>;
template class PageEntryMap_t<false>;

} // namespace DB
