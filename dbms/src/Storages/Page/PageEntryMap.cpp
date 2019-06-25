#include <Storages/Page/PageEntryMap.h>

#include <IO/WriteHelpers.h>
#include <common/likely.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

void PageEntryMap::put(const PageId page_id, const PageEntry & entry)
{
    const PageId normal_page_id = resolveRefId(page_id);
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
        const UInt32 ref_count           = ori_iter->second.ref;
        normal_pages[normal_page_id]     = entry;
        normal_pages[normal_page_id].ref = ref_count;
    }
    // add a RefPage to Page
    page_ref.emplace(page_id, normal_page_id);
}

} // namespace DB
