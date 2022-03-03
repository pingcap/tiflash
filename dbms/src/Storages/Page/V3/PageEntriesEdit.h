#pragma once

#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/WriteBatch.h>
#include <fmt/format.h>

namespace DB::PS::V3
{
// `PageDirectory::apply` with create a version={directory.sequence, epoch=0}.
// After data compaction and page entries need to be updated, will create
// some entries with a version={old_sequence, epoch=old_epoch+1}.
struct PageVersionType
{
    UInt64 sequence; // The write sequence
    UInt64 epoch; // The GC epoch

    explicit PageVersionType(UInt64 seq)
        : sequence(seq)
        , epoch(0)
    {}

    PageVersionType(UInt64 seq, UInt64 epoch_)
        : sequence(seq)
        , epoch(epoch_)
    {}

    PageVersionType()
        : PageVersionType(0)
    {}

    bool operator<(const PageVersionType & rhs) const
    {
        if (sequence == rhs.sequence)
            return epoch < rhs.epoch;
        return sequence < rhs.sequence;
    }

    bool operator==(const PageVersionType & rhs) const
    {
        return (sequence == rhs.sequence) && (epoch == rhs.epoch);
    }
};
using VersionedEntry = std::pair<PageVersionType, PageEntryV3>;
using VersionedEntries = std::vector<VersionedEntry>;


/// Page entries change to apply to PageDirectory
class PageEntriesEdit
{
public:
    PageEntriesEdit() = default;

    explicit PageEntriesEdit(size_t capacity)
    {
        records.reserve(capacity);
    }

    void put(PageIdV3Internal page_id, const PageEntryV3 & entry)
    {
        EditRecord record{};
        record.type = WriteBatch::WriteType::PUT;
        record.page_id = page_id;
        record.entry = entry;
        records.emplace_back(record);
    }

    void upsertPage(PageIdV3Internal page_id, const PageVersionType & ver, const PageEntryV3 & entry)
    {
        EditRecord record{};
        record.type = WriteBatch::WriteType::UPSERT;
        record.page_id = page_id;
        record.version = ver;
        record.entry = entry;
        records.emplace_back(record);
    }

    void del(PageIdV3Internal page_id)
    {
        EditRecord record{};
        record.type = WriteBatch::WriteType::DEL;
        record.page_id = page_id;
        records.emplace_back(record);
    }

    void ref(PageIdV3Internal ref_id, PageIdV3Internal page_id)
    {
        EditRecord record{};
        record.type = WriteBatch::WriteType::REF;
        record.page_id = ref_id;
        record.ori_page_id = page_id;
        records.emplace_back(record);
    }

    void concat(PageEntriesEdit & rhs)
    {
        auto rhs_records = rhs.getRecords();
        records.insert(records.end(), rhs_records.begin(), rhs_records.end());
    }

    void clear() { records.clear(); }

    bool empty() const { return records.empty(); }

    size_t size() const { return records.size(); }

    struct EditRecord
    {
        WriteBatch::WriteType type;
        PageIdV3Internal page_id;
        PageIdV3Internal ori_page_id;
        PageVersionType version;
        PageEntryV3 entry;
    };
    using EditRecords = std::vector<EditRecord>;

    void appendRecord(const EditRecord & rec)
    {
        records.emplace_back(rec);
    }

    EditRecords & getMutRecords() { return records; }
    const EditRecords & getRecords() const { return records; }

private:
    EditRecords records;

public:
    // No copying allowed
    PageEntriesEdit(const PageEntriesEdit &) = delete;
    PageEntriesEdit & operator=(const PageEntriesEdit &) = delete;
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

} // namespace DB::PS::V3

/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <>
struct fmt::formatter<DB::PS::V3::PageVersionType>
{
    static constexpr auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::PS::V3::PageVersionType & ver, FormatContext & ctx)
    {
        return format_to(ctx.out(), "<{},{}>", ver.sequence, ver.epoch);
    }
};
