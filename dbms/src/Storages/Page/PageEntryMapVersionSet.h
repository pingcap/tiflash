#pragma once

#include <set>
#include <vector>

#include <Common/VersionSet.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageEntryMap.h>
#include <Storages/Page/WriteBatch.h>

namespace DB
{

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

    bool empty() const { return records.empty(); }

    size_t size() const { return records.size(); }

    struct EditRecord
    {
        WriteBatch::WriteType type;
        char                  _padding[7]; // 7 bytes unused since type is only 1 byte.
        PageId                page_id;
        PageId                ori_page_id;
        PageEntry             entry;
    };
    using EditRecords = std::vector<EditRecord>;
    static_assert(std::is_trivially_copyable_v<EditRecord>);

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

class PageEntryMapBuilder
{
public:
    explicit PageEntryMapBuilder(const PageEntryMap * base_, //
                                 bool                 ignore_invalid_ref_ = false,
                                 Poco::Logger *       log_                = nullptr)
        : base(const_cast<PageEntryMap *>(base_)),
          v(new PageEntryMap), //
          ignore_invalid_ref(ignore_invalid_ref_),
          log(log_)
    {
#ifndef NDEBUG
        if (ignore_invalid_ref)
        {
            assert(log != nullptr);
        }
#endif
        base->incrRefCount();
        v->copyEntries(*base);
    }

    ~PageEntryMapBuilder() { base->decrRefCount(); }

    void apply(const PageEntriesEdit & edit);

    void gcApply(const PageEntriesEdit & edit);

    PageEntryMap * build() { return v; }

private:
    PageEntryMap * base;
    PageEntryMap * v;
    bool           ignore_invalid_ref;
    Poco::Logger * log;
};

class PageEntryMapVersionSet : public ::DB::MVCC::VersionSet<PageEntryMap, PageEntriesEdit, PageEntryMapBuilder>
{
public:
    explicit PageEntryMapVersionSet(const ::DB::MVCC::VersionSetConfig & config_ = ::DB::MVCC::VersionSetConfig())
        : ::DB::MVCC::VersionSet<PageEntryMap, PageEntriesEdit, PageEntryMapBuilder>(config_)
    {
    }

public:
    using SnapshotPtr = ::DB::MVCC::VersionSet<PageEntryMap, PageEntriesEdit, PageEntryMapBuilder>::SnapshotPtr;

    /// `gcApply` only accept PageEntry's `PUT` changes and will discard changes if PageEntry is invalid
    /// append new version to version-list
    std::set<PageFileIdAndLevel> gcApply(const PageEntriesEdit & edit);

    /// List all PageFile that are used by any version
    std::set<PageFileIdAndLevel> listAllLiveFiles() const;
};


} // namespace DB
