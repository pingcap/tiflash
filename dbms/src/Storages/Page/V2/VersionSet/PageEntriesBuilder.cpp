// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Storages/Page/V2/VersionSet/PageEntriesBuilder.h>

namespace DB::PS::V2
{
void PageEntriesBuilder::apply(const PageEntriesEdit & edit)
{
    for (const auto & rec : edit.getRecords())
    {
        switch (rec.type)
        {
        case WriteBatch::WriteType::PUT_EXTERNAL:
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

} // namespace DB::PS::V2
