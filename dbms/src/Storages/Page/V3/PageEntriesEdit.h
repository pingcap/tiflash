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

#pragma once

#include <Common/nocopyable.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/WriteBatch.h>
#include <common/types.h>
#include <fmt/format.h>

namespace DB::PS::V3
{
// `PageDirectory::apply` with create a version={directory.sequence, epoch=0}.
// After data compaction and page entries need to be updated, will create
// some entries with a version={old_sequence, epoch=old_epoch+1}.
struct PageVersion
{
    UInt64 sequence; // The write sequence
    UInt64 epoch; // The GC epoch

    explicit PageVersion(UInt64 seq)
        : sequence(seq)
        , epoch(0)
    {}

    PageVersion(UInt64 seq, UInt64 epoch_)
        : sequence(seq)
        , epoch(epoch_)
    {}

    PageVersion()
        : PageVersion(0)
    {}

    bool operator<(const PageVersion & rhs) const
    {
        if (sequence == rhs.sequence)
            return epoch < rhs.epoch;
        return sequence < rhs.sequence;
    }

    bool operator==(const PageVersion & rhs) const
    {
        return (sequence == rhs.sequence) && (epoch == rhs.epoch);
    }

    bool operator<=(const PageVersion & rhs) const
    {
        if (sequence == rhs.sequence)
            return epoch <= rhs.epoch;
        return sequence <= rhs.sequence;
    }
};
} // namespace DB::PS::V3
/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <>
struct fmt::formatter<DB::PS::V3::PageVersion>
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
    auto format(const DB::PS::V3::PageVersion & ver, FormatContext & ctx)
    {
        return format_to(ctx.out(), "<{},{}>", ver.sequence, ver.epoch);
    }
};

namespace DB::PS::V3
{
using VersionedEntry = std::pair<PageVersion, PageEntryV3>;
using VersionedEntries = std::vector<VersionedEntry>;

enum class EditRecordType
{
    PUT,
    PUT_EXTERNAL,
    REF,
    DEL,
    //
    UPSERT,
    // Variant types for dumping the in-memory entries into
    // snapshot
    VAR_ENTRY,
    VAR_REF,
    VAR_EXTERNAL,
    VAR_DELETE,
};

inline const char * typeToString(EditRecordType t)
{
    switch (t)
    {
    case EditRecordType::PUT:
        return "PUT    ";
    case EditRecordType::PUT_EXTERNAL:
        return "EXT    ";
    case EditRecordType::REF:
        return "REF    ";
    case EditRecordType::DEL:
        return "DEL    ";
    case EditRecordType::UPSERT:
        return "UPSERT ";
    case EditRecordType::VAR_ENTRY:
        return "VAR_ENT";
    case EditRecordType::VAR_REF:
        return "VAR_REF";
    case EditRecordType::VAR_EXTERNAL:
        return "VAR_EXT";
    case EditRecordType::VAR_DELETE:
        return "VAR_DEL";
    default:
        return "INVALID";
    }
}

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
        record.type = EditRecordType::PUT;
        record.page_id = page_id;
        record.entry = entry;
        records.emplace_back(record);
    }

    void putExternal(PageIdV3Internal page_id)
    {
        EditRecord record{};
        record.type = EditRecordType::PUT_EXTERNAL;
        record.page_id = page_id;
        records.emplace_back(record);
    }

    void upsertPage(PageIdV3Internal page_id, const PageVersion & ver, const PageEntryV3 & entry)
    {
        EditRecord record{};
        record.type = EditRecordType::UPSERT;
        record.page_id = page_id;
        record.version = ver;
        record.entry = entry;
        records.emplace_back(record);
    }

    void del(PageIdV3Internal page_id)
    {
        EditRecord record{};
        record.type = EditRecordType::DEL;
        record.page_id = page_id;
        records.emplace_back(record);
    }

    void ref(PageIdV3Internal ref_id, PageIdV3Internal page_id)
    {
        EditRecord record{};
        record.type = EditRecordType::REF;
        record.page_id = ref_id;
        record.ori_page_id = page_id;
        records.emplace_back(record);
    }

    void varRef(PageIdV3Internal ref_id, const PageVersion & ver, PageIdV3Internal ori_page_id)
    {
        EditRecord record{};
        record.type = EditRecordType::VAR_REF;
        record.page_id = ref_id;
        record.version = ver;
        record.ori_page_id = ori_page_id;
        records.emplace_back(record);
    }

    void varExternal(PageIdV3Internal page_id, const PageVersion & create_ver, Int64 being_ref_count)
    {
        EditRecord record{};
        record.type = EditRecordType::VAR_EXTERNAL;
        record.page_id = page_id;
        record.version = create_ver;
        record.being_ref_count = being_ref_count;
        records.emplace_back(record);
    }

    void varEntry(PageIdV3Internal page_id, const PageVersion & ver, const PageEntryV3 & entry, Int64 being_ref_count)
    {
        EditRecord record{};
        record.type = EditRecordType::VAR_ENTRY;
        record.page_id = page_id;
        record.version = ver;
        record.entry = entry;
        record.being_ref_count = being_ref_count;
        records.emplace_back(record);
    }

    void varDel(PageIdV3Internal page_id, const PageVersion & delete_ver)
    {
        EditRecord record{};
        record.type = EditRecordType::VAR_DELETE;
        record.page_id = page_id;
        record.version = delete_ver;
        records.emplace_back(record);
    }

    void clear() { records.clear(); }

    bool empty() const { return records.empty(); }

    size_t size() const { return records.size(); }

    struct EditRecord
    {
        EditRecordType type;
        PageIdV3Internal page_id;
        PageIdV3Internal ori_page_id;
        PageVersion version;
        PageEntryV3 entry;
        Int64 being_ref_count;

        EditRecord()
            : type(EditRecordType::DEL)
            , page_id(0)
            , ori_page_id(0)
            , version(0, 0)
            , being_ref_count(1)
        {}
    };
    using EditRecords = std::vector<EditRecord>;

    static String toDebugString(const EditRecord & rec)
    {
        return fmt::format(
            "{{type:{}, page_id:{}, ori_id:{}, version:{}, entry:{}, being_ref_count:{}}}",
            typeToString(rec.type),
            rec.page_id,
            rec.ori_page_id,
            rec.version,
            DB::PS::V3::toDebugString(rec.entry),
            rec.being_ref_count);
    }

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

} // namespace DB::PS::V3
