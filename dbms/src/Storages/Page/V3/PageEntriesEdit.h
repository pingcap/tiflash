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
#include <Storages/Page/V3/Remote/Proto/Helper.h>
#include <Storages/Page/V3/Remote/Proto/manifest_file.pb.h>
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

inline Remote::EditType typeToRemote(EditRecordType t)
{
    switch (t)
    {
    case EditRecordType::VAR_ENTRY:
        return Remote::EDIT_TYPE_ENTRY;
    case EditRecordType::VAR_REF:
        return Remote::EDIT_TYPE_REF;
    case EditRecordType::VAR_EXTERNAL:
        return Remote::EDIT_TYPE_EXTERNAL;
    case EditRecordType::VAR_DELETE:
        return Remote::EDIT_TYPE_DELETE;
    default:
        return Remote::EDIT_TYPE_UNSPECIFIED;
    }
}

inline EditRecordType typeFromRemote(Remote::EditType t)
{
    switch (t)
    {
    case Remote::EDIT_TYPE_ENTRY:
        return EditRecordType::VAR_ENTRY;
    case Remote::EDIT_TYPE_REF:
        return EditRecordType::VAR_REF;
    case Remote::EDIT_TYPE_EXTERNAL:
        return EditRecordType::VAR_EXTERNAL;
    case Remote::EDIT_TYPE_DELETE:
        return EditRecordType::VAR_DELETE;
    default:
        RUNTIME_CHECK(false, EditType_Name(t));
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

    explicit PageEntriesEdit(size_t capacity)
    {
        records.reserve(capacity);
    }

    void put(const PageId & page_id, const PageEntryV3 & entry)
    {
        EditRecord record{};
        record.type = EditRecordType::PUT;
        record.page_id = page_id;
        record.entry = entry;
        records.emplace_back(record);
    }

    void putExternal(const PageId & page_id)
    {
        EditRecord record{};
        record.type = EditRecordType::PUT_EXTERNAL;
        record.page_id = page_id;
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

    void varExternal(const PageId & page_id, const PageVersion & create_ver, Int64 being_ref_count)
    {
        EditRecord record{};
        record.type = EditRecordType::VAR_EXTERNAL;
        record.page_id = page_id;
        record.version = create_ver;
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

        Remote::EditRecord toRemote() const
        {
            Remote::EditRecord remote_rec;
            remote_rec.set_type(typeToRemote(type));
            remote_rec.mutable_page_id()->CopyFrom(Remote::toRemote(page_id));
            remote_rec.mutable_ori_page_id()->CopyFrom(Remote::toRemote(ori_page_id));
            remote_rec.set_version_sequence(version.sequence);
            remote_rec.set_version_epoch(version.epoch);
            remote_rec.set_being_ref_count(being_ref_count);
            if (type == EditRecordType::VAR_ENTRY)
            {
                RUNTIME_CHECK(entry.remote_info.has_value());
                *remote_rec.mutable_entry() = entry.remote_info->data_location.toRemote();
                for (const auto & [offset, checksum] : entry.field_offsets)
                {
                    remote_rec.add_fields_offset(offset);
                    remote_rec.add_fields_checksum(checksum);
                }
            }
            return remote_rec;
        }

        // WARNING!!!
        // The edit record rebuild from the `remote_rec` does not have valid blob file ID, blob data size, etc for VAR_ENTRY.
        static EditRecord fromRemote(const Remote::EditRecord & remote_rec)
        {
            EditRecord rec;
            rec.type = typeFromRemote(remote_rec.type());
            rec.page_id = Remote::fromRemote<PageIdType>(remote_rec.page_id());
            rec.ori_page_id = Remote::fromRemote<PageIdType>(remote_rec.ori_page_id());
            rec.version.sequence = remote_rec.version_sequence();
            rec.version.epoch = remote_rec.version_epoch();
            rec.being_ref_count = remote_rec.being_ref_count();
            if (rec.type == EditRecordType::VAR_ENTRY)
            {
                rec.entry.remote_info = RemoteDataInfo{
                    .data_location = RemoteDataLocation::fromRemote(remote_rec.entry()),
                    .is_local_data_reclaimed = true,
                };
                RUNTIME_CHECK(remote_rec.fields_offset_size() == remote_rec.fields_checksum_size());
                auto sz = remote_rec.fields_offset_size();
                for (int i = 0; i < sz; ++i)
                {
                    rec.entry.field_offsets.emplace_back(std::make_pair(
                        remote_rec.fields_offset(i),
                        remote_rec.fields_checksum(i)));
                }
                // Note: rec.entry.* is untouched, leaving zero value.
                // We need to take care when restoring the PS instance.
            }
            return rec;
        }

        String toDebugString() const
        {
            return fmt::format(
                "{{type:{}, page_id:{}, ori_id:{}, version:{}, entry:{}, being_ref_count:{}}}",
                typeToString(type),
                page_id,
                ori_page_id,
                version,
                entry.toDebugString(),
                being_ref_count);
        }
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

    String toDebugString() const
    {
        FmtBuffer buf;
        buf.append('[');
        buf.joinStr(
            records.begin(),
            records.end(),
            [](const auto & arg, FmtBuffer & fb) {
                fb.append(arg.toDebugString());
            },
            ", ");
        buf.append(']');
        return buf.toString();
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
