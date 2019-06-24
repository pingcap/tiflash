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

void PageEntryMap::del(PageId page_id)
{
    // Note: must resolve ref-id before erasing entry in `page_ref`
    const PageId normal_page_id = resolveRefId(page_id);
    page_ref.erase(page_id);

    // decrease origin page's ref counting
    decreasePageRef(normal_page_id);
}

void PageEntryMap::ref(const PageId ref_id, const PageId page_id)
{
    // if `page_id` is a ref-id, collapse the ref-path to actual PageId
    // eg. exist RefPage2 -> Page1, add RefPage3 -> RefPage2, collapse to RefPage3 -> Page1
    const PageId normal_page_id = resolveRefId(page_id);
    auto         iter           = normal_pages.find(normal_page_id);
    if (likely(iter != normal_pages.end()))
    {
        // if RefPage{ref_id} already exist, release that ref first
        const auto ori_ref = page_ref.find(ref_id);
        if (unlikely(ori_ref != page_ref.end()))
        {
            decreasePageRef(ori_ref->second);
        }
        // build ref
        page_ref[ref_id] = normal_page_id;
        iter->second.ref += 1;
    }
    else
    {
        throw Exception("Adding RefPage" + DB::toString(ref_id) + " to non-exist Page" + DB::toString(page_id), ErrorCodes::LOGICAL_ERROR);
    }
}

void PageEntryMap::decreasePageRef(const PageId page_id)
{
    auto iter = normal_pages.find(page_id);
    assert(iter != normal_pages.end());
    if (iter != normal_pages.end())
    {
        auto & cache = iter->second;
        cache.ref -= 1;
        if (cache.ref == 0)
        {
            normal_pages.erase(iter);
        }
    }
}

} // namespace DB
