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
#include <Storages/Page/V3/CheckpointFile/Proto/manifest_file.pb.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>
#include <common/types.h>
#include <fmt/format.h>

#include <magic_enum.hpp>

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

    bool operator==(const PageVersion & rhs) const { return (sequence == rhs.sequence) && (epoch == rhs.epoch); }

    bool operator<=(const PageVersion & rhs) const
    {
        if (sequence == rhs.sequence)
            return epoch <= rhs.epoch;
        return sequence <= rhs.sequence;
    }
};
} // namespace DB::PS::V3

namespace DB::PS::V3
{
using VersionedEntry = std::pair<PageVersion, PS::V3::PageEntryV3>;
using VersionedEntries = std::vector<VersionedEntry>;

enum class EditRecordType
{
    PUT = 0,
    PUT_EXTERNAL = 1,
    REF = 2,
    DEL = 3,
    //
    UPSERT = 4,
    // Variant types for dumping the in-memory entries into
    // snapshot
    VAR_ENTRY = 5,
    VAR_REF = 6,
    VAR_EXTERNAL = 7,
    VAR_DELETE = 8,
    // Just used to update local cache info for VAR_ENTRY type
    UPDATE_DATA_FROM_REMOTE = 9,
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
    case EditRecordType::UPDATE_DATA_FROM_REMOTE:
        return "UPDATE_DATA_FROM_REMOTE";
    default:
        return "INVALID";
    }
}

inline CheckpointProto::EditType typeToProto(EditRecordType t)
{
    switch (t)
    {
    case EditRecordType::VAR_ENTRY:
        return CheckpointProto::EDIT_TYPE_ENTRY;
    case EditRecordType::VAR_REF:
        return CheckpointProto::EDIT_TYPE_REF;
    case EditRecordType::VAR_EXTERNAL:
        return CheckpointProto::EDIT_TYPE_EXTERNAL;
    case EditRecordType::VAR_DELETE:
        return CheckpointProto::EDIT_TYPE_DELETE;
    default:
        RUNTIME_CHECK_MSG(false, "Unsupported Edit Type {}", magic_enum::enum_name(t));
    }
}

inline EditRecordType typeFromProto(CheckpointProto::EditType t)
{
    switch (t)
    {
    case CheckpointProto::EDIT_TYPE_ENTRY:
        return EditRecordType::VAR_ENTRY;
    case CheckpointProto::EDIT_TYPE_REF:
        return EditRecordType::VAR_REF;
    case CheckpointProto::EDIT_TYPE_EXTERNAL:
        return EditRecordType::VAR_EXTERNAL;
    case CheckpointProto::EDIT_TYPE_DELETE:
        return EditRecordType::VAR_DELETE;
    default:
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            fmt::format("Unsupported Proto Edit Type {}", magic_enum::enum_name(t)));
    }
}

/// Page entries change to apply to PageDirectory
template <typename PageIdType>
class PageEntriesEdit
{
public:
    using PageId = PageIdType;

public:
    PageEntriesEdit() = default;

    explicit PageEntriesEdit(size_t capacity) { records.reserve(capacity); }

    void put(const PageId & page_id, const PageEntryV3 & entry)
    {
        EditRecord record{};
        record.type = EditRecordType::PUT;
        record.page_id = page_id;
        record.entry = entry;
        records.emplace_back(record);
    }

    void updateRemote(const PageId & page_id, const PageEntryV3 & entry)
    {
        EditRecord record{};
        record.type = EditRecordType::UPDATE_DATA_FROM_REMOTE;
        record.page_id = page_id;
        record.entry = entry;
        records.emplace_back(record);
    }

    void putExternal(const PageId & page_id, const PageEntryV3 & entry = {})
    {
        EditRecord record{};
        record.type = EditRecordType::PUT_EXTERNAL;
        record.page_id = page_id;
        record.entry = entry;
        records.emplace_back(record);
    }

    void upsertPage(const PageId & page_id, const PageVersion & ver, const PageEntryV3 & entry)
    {
        EditRecord record{};
        record.type = EditRecordType::UPSERT;
        record.page_id = page_id;
        record.version = ver;
        record.entry = entry;
        records.emplace_back(record);
    }

    void del(const PageId & page_id)
    {
        EditRecord record{};
        record.type = EditRecordType::DEL;
        record.page_id = page_id;
        records.emplace_back(record);
    }

    void ref(const PageId & ref_id, const PageId & page_id)
    {
        EditRecord record{};
        record.type = EditRecordType::REF;
        record.page_id = ref_id;
        record.ori_page_id = page_id;
        records.emplace_back(record);
    }

    void varRef(const PageId & ref_id, const PageVersion & ver, const PageId & ori_page_id)
    {
        EditRecord record{};
        record.type = EditRecordType::VAR_REF;
        record.page_id = ref_id;
        record.version = ver;
        record.ori_page_id = ori_page_id;
        records.emplace_back(record);
    }

    void varExternal(
        const PageId & page_id,
        const PageVersion & create_ver,
        const PageEntryV3 & entry,
        Int64 being_ref_count)
    {
        EditRecord record{};
        record.type = EditRecordType::VAR_EXTERNAL;
        record.page_id = page_id;
        record.version = create_ver;
        record.entry = entry;
        record.being_ref_count = being_ref_count;
        records.emplace_back(record);
    }

    void varEntry(const PageId & page_id, const PageVersion & ver, const PageEntryV3 & entry, Int64 being_ref_count)
    {
        EditRecord record{};
        record.type = EditRecordType::VAR_ENTRY;
        record.page_id = page_id;
        record.version = ver;
        record.entry = entry;
        record.being_ref_count = being_ref_count;
        records.emplace_back(record);
    }

    void varDel(const PageId & page_id, const PageVersion & delete_ver)
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
        EditRecordType type{EditRecordType::DEL};
        PageId page_id{};
        PageId ori_page_id{};
        PageVersion version;
        PageEntryV3 entry;
        Int64 being_ref_count{1};

        CheckpointProto::EditRecord toProto() const;

        static EditRecord fromProto(
            const CheckpointProto::EditRecord & edit_rec,
            CheckpointProto::StringsInternMap & strings_map);
    };
    using EditRecords = std::vector<EditRecord>;

    void appendRecord(const EditRecord & rec) { records.emplace_back(rec); }

    void merge(PageEntriesEdit && other)
    {
        records.insert(
            records.end(),
            std::make_move_iterator(other.records.begin()),
            std::make_move_iterator(other.records.end()));
        other.records.clear();
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

namespace u128
{
using PageEntriesEdit = PageEntriesEdit<PageIdV3Internal>;
}
namespace universal
{
using PageEntriesEdit = PageEntriesEdit<UniversalPageId>;
}
} // namespace DB::PS::V3

template <>
struct fmt::formatter<DB::PS::V3::PageVersion>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::PS::V3::PageVersion & ver, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}.{}", ver.sequence, ver.epoch);
    }
};

template <>
struct fmt::formatter<DB::PS::V3::PageEntriesEdit<DB::PageIdV3Internal>::EditRecord>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::PS::V3::PageEntriesEdit<DB::PageIdV3Internal>::EditRecord & rec, FormatContext & ctx) const
    {
        return fmt::format_to(
            ctx.out(),
            "{{type:{}, page_id:{}, ori_id:{}, version:{}, entry:{}, being_ref_count:{}}}",
            DB::PS::V3::typeToString(rec.type),
            rec.page_id,
            rec.ori_page_id,
            rec.version,
            rec.entry,
            rec.being_ref_count);
    }
};

template <>
struct fmt::formatter<DB::PS::V3::PageEntriesEdit<DB::UniversalPageId>::EditRecord>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::PS::V3::PageEntriesEdit<DB::UniversalPageId>::EditRecord & rec, FormatContext & ctx) const
    {
        return fmt::format_to(
            ctx.out(),
            "{{type:{}, page_id:{}, ori_id:{}, version:{}, entry:{}, being_ref_count:{}}}",
            DB::PS::V3::typeToString(rec.type),
            rec.page_id,
            rec.ori_page_id,
            rec.version,
            rec.entry,
            rec.being_ref_count);
    }
};
