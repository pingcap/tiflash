// Copyright 2022 PingCAP, Ltd.
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
        case WriteBatchWriteType::PUT_EXTERNAL:
        case WriteBatchWriteType::PUT:
            current_version->put(rec.page_id, rec.entry);
            break;
        case WriteBatchWriteType::DEL:
            current_version->del(rec.page_id);
            break;
        case WriteBatchWriteType::REF:
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
        case WriteBatchWriteType::UPSERT:
            current_version->upsertPage(rec.page_id, rec.entry);
            break;
        case WriteBatchWriteType::PUT_REMOTE:
            throw Exception("", ErrorCodes::LOGICAL_ERROR);
            break;
        }
    }
}

} // namespace DB::PS::V2
