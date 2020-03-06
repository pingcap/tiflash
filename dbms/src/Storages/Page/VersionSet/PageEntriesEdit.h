#pragma once

#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{

/// Page entries change to apply to version set.
class PageEntriesEdit
{
public:
    PageEntriesEdit() = default;

    void put(PageId page_id, const PageEntry & entry)
    {
        EditRecord record;
        record.type    = WriteBatch::WriteType::PUT;
        record.page_id = page_id;
        record.entry   = entry;
        records.emplace_back(record);
    }

    void upsertPage(PageId page_id, const PageEntry & entry)
    {
        EditRecord record;
        record.type    = WriteBatch::WriteType::UPSERT;
        record.page_id = page_id;
        record.entry   = entry;
        records.emplace_back(record);
    }

    void del(PageId page_id)
    {
        EditRecord record;
        record.type    = WriteBatch::WriteType::DEL;
        record.page_id = page_id;
        records.emplace_back(record);
    }

    void ref(PageId ref_id, PageId page_id)
    {
        EditRecord record;
        record.type        = WriteBatch::WriteType::REF;
        record.page_id     = ref_id;
        record.ori_page_id = page_id;
        records.emplace_back(record);
    }

    void clear() { records.clear(); }

    bool empty() const { return records.empty(); }

    size_t size() const { return records.size(); }

    struct EditRecord
    {
        WriteBatch::WriteType type;
        PageId                page_id;
        PageId                ori_page_id;
        PageEntry             entry;
    };
    using EditRecords = std::vector<EditRecord>;

    EditRecords &       getRecords() { return records; }
    const EditRecords & getRecords() const { return records; }

private:
    EditRecords records;

public:
    // No copying allowed
    PageEntriesEdit(const PageEntriesEdit &) = delete;
    PageEntriesEdit & operator=(const PageEntriesEdit &) = delete;
    // Only move allowed
    PageEntriesEdit(PageEntriesEdit && rhs) noexcept : PageEntriesEdit() { *this = std::move(rhs); }
    PageEntriesEdit & operator=(PageEntriesEdit && rhs) noexcept
    {
        if (this != &rhs)
        {
            records.swap(rhs.records);
        }
        return *this;
    }
};

} // namespace DB
