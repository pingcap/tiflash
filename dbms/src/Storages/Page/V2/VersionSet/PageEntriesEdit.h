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

#pragma once

#include <Common/nocopyable.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/WriteBatchImpl.h>

namespace DB::PS::V2
{
/// Page entries change to apply to version set.
class PageEntriesEdit
{
public:
    PageEntriesEdit() = default;

    void put(PageId page_id, const PageEntry & entry)
    {
        EditRecord record{};
        record.type = WriteBatchWriteType::PUT;
        record.page_id = page_id;
        record.entry = entry;
        records.emplace_back(record);
    }

    void upsertPage(PageId page_id, const PageEntry & entry)
    {
        EditRecord record{};
        record.type = WriteBatchWriteType::UPSERT;
        record.page_id = page_id;
        record.entry = entry;
        records.emplace_back(record);
    }

    void del(PageId page_id)
    {
        EditRecord record{};
        record.type = WriteBatchWriteType::DEL;
        record.page_id = page_id;
        records.emplace_back(record);
    }

    void ref(PageId ref_id, PageId page_id)
    {
        EditRecord record{};
        record.type = WriteBatchWriteType::REF;
        record.page_id = ref_id;
        record.ori_page_id = page_id;
        records.emplace_back(record);
    }

    void concate(PageEntriesEdit & edit)
    {
        auto rhs = edit.getRecords();
        records.insert(records.end(), rhs.begin(), rhs.end());
    }

    void clear() { records.clear(); }

    bool empty() const { return records.empty(); }

    size_t size() const { return records.size(); }

    struct EditRecord
    {
        WriteBatchWriteType type;
        PageId page_id;
        PageId ori_page_id;
        PageEntry entry;
    };
    using EditRecords = std::vector<EditRecord>;

    EditRecords & getRecords() { return records; }
    const EditRecords & getRecords() const { return records; }

private:
    EditRecords records;

public:
    // No copying allowed
    DISALLOW_COPY(PageEntriesEdit);
    // Only move allowed
    PageEntriesEdit(PageEntriesEdit && rhs) noexcept
        : PageEntriesEdit()
    {
        *this = std::move(rhs);
    }
    PageEntriesEdit & operator=(PageEntriesEdit && rhs) noexcept
    {
        if (this != &rhs)
        {
            records.swap(rhs.records);
        }
        return *this;
    }
};

} // namespace DB::PS::V2
