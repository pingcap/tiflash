#include <Storages/Page/VersionSet/PageEntriesBuilder.h>

namespace DB
{

void PageEntriesBuilder::apply(const PageEntriesEdit & edit)
{
    for (const auto & rec : edit.getRecords())
    {
        switch (rec.type)
        {
        case WriteBatch::WriteType::PUT:
            current_version->put(rec.page_id, rec.entry);
            break;
        case WriteBatch::WriteType::DEL:
            current_version->del(rec.page_id);
            break;
        case WriteBatch::WriteType::REF:
            if (likely(!ignore_invalid_ref))
            {
                current_version->ref<false>(rec.page_id, rec.ori_page_id);
            }
            else
            {
                try
                {
                    current_version->ref<true>(rec.page_id, rec.ori_page_id);
                }
                catch (DB::Exception & e)
                {
                    LOG_WARNING(log, "Ignore invalid RefPage while opening PageStorage: " + e.message());
                }
            }
            break;
        }
    }
}

} // namespace DB
