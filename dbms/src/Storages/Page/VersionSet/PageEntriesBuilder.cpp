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
            try
            {
                current_version->ref(rec.page_id, rec.ori_page_id);
            }
            catch (DB::Exception & e)
            {
                if (likely(!ignore_invalid_ref))
                {
                    throw;
                }
                else
                {
                    LOG_WARNING(log, "Ignore invalid RefPage in PageEntriesBuilder::apply, " + e.message());
                }
            }
            break;
        case WriteBatch::WriteType::UPSERT:
            current_version->upsertPage(rec.page_id, rec.entry);
            break;
        }
    }
}

} // namespace DB
