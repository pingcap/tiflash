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

enum class EditRecordType : UInt8
{
    PUT,
    PUT_EXTERNAL,
    REF,
    DEL,

    // `UPSERT` is created by `PageStorage::gc` that copy and move data to another
    // BlobFile to reduce space amplification.
    UPSERT,

    // `COLLAPSED_ENTRY`, `COLLAPSED_MAPPING` is created by `CollapsingPageDirectory`
    // to save the collapsed state of log files. We need these kinds of edits to
    // reconstruct the memory collapsed state created by writebatches like:
    // put 10, ref 11->10, ref 12->11, del 10
    COLLAPSED_ENTRY,
    COLLAPSED_MAPPING,
};

/// Page entries change to apply to PageDirectory
class PageEntriesEdit
{
public:
    PageEntriesEdit() = default;

    explicit PageEntriesEdit(size_t capacity)
    {
        records.reserve(capacity);
    }

    void put(PageId page_id, const PageEntryV3 & entry)
    {
        EditRecord record{};
        record.type = EditRecordType::PUT;
        record.page_id = page_id;
        record.entry = entry;
        records.emplace_back(record);
    }

    void upsertPage(PageId page_id, const PageVersionType & ver, const PageEntryV3 & entry)
    {
        EditRecord record{};
        record.type = EditRecordType::UPSERT;
        record.page_id = page_id;
        record.version = ver;
        record.entry = entry;
        records.emplace_back(record);
    }

    void del(PageId page_id)
    {
        EditRecord record{};
        record.type = EditRecordType::DEL;
        record.page_id = page_id;
        records.emplace_back(record);
    }

    void ref(PageId ref_id, PageId page_id)
    {
        EditRecord record{};
        record.type = EditRecordType::REF;
        record.page_id = ref_id;
        record.ori_page_id = page_id;
        records.emplace_back(record);
    }

    void collapsedEntry(PageId page_id, const PageVersionType & ver, const PageEntryV3 & entry)
    {
        EditRecord record{};
        record.type = EditRecordType::COLLAPSED_ENTRY;
        record.page_id = page_id;
        record.version = ver;
        record.entry = entry;
        records.emplace_back(record);
    }

    void collapsedMapping(PageId ref_id, PageId page_id, const PageVersionType & ver)
    {
        EditRecord record{};
        record.type = EditRecordType::COLLAPSED_MAPPING;
        record.page_id = ref_id;
        record.ori_page_id = page_id;
        record.version = ver;
        records.emplace_back(record);
    }

    void concate(PageEntriesEdit & rhs)
    {
        auto rhs_records = rhs.getRecords();
        records.insert(records.end(), rhs_records.begin(), rhs_records.end());
    }

    void clear() { records.clear(); }

    bool empty() const { return records.empty(); }

    size_t size() const { return records.size(); }

    struct EditRecord
    {
        EditRecordType type;
        PageId page_id;
        PageId ori_page_id;
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
