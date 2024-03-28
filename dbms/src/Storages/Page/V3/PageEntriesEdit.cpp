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

#include <Storages/Page/V3/PageEntriesEdit.h>

namespace DB::PS::V3
{

template <>
CheckpointProto::EditRecord PageEntriesEdit<UniversalPageId>::EditRecord::toProto() const
{
    CheckpointProto::EditRecord proto_edit;
    proto_edit.set_type(typeToProto(type));
    proto_edit.set_page_id(page_id.asStr());
    proto_edit.set_ori_page_id(ori_page_id.asStr());
    proto_edit.set_version_sequence(version.sequence);
    proto_edit.set_version_epoch(version.epoch);
    if (type == EditRecordType::VAR_ENTRY)
    {
        proto_edit.set_entry_size(entry.size);
        proto_edit.set_entry_tag(entry.tag);
        proto_edit.set_entry_checksum(entry.checksum);
        // uploading page data may be disabled
        if (entry.checkpoint_info.has_value())
        {
            proto_edit.mutable_entry_location()->CopyFrom(entry.checkpoint_info.data_location.toProto());
        }
        for (const auto & [offset, checksum] : entry.field_offsets)
        {
            proto_edit.add_entry_fields_offset(offset);
            proto_edit.add_entry_fields_checksum(checksum);
        }
    }
    if (type == EditRecordType::VAR_EXTERNAL)
    {
        RUNTIME_CHECK(entry.checkpoint_info.has_value());
        proto_edit.mutable_entry_location()->CopyFrom(entry.checkpoint_info.data_location.toProto());
    }
    return proto_edit;
}

template <>
typename PageEntriesEdit<UniversalPageId>::EditRecord PageEntriesEdit<UniversalPageId>::EditRecord::fromProto(
    const CheckpointProto::EditRecord & proto_edit,
    CheckpointProto::StringsInternMap & strings_map)
{
    EditRecord rec;
    try
    {
        rec.type = typeFromProto(proto_edit.type());
        rec.page_id = UniversalPageId(proto_edit.page_id());
        rec.ori_page_id = UniversalPageId(proto_edit.ori_page_id());
        rec.version.sequence = proto_edit.version_sequence();
        rec.version.epoch = proto_edit.version_epoch();
        rec.being_ref_count = 1;
    }
    catch (const Exception & e)
    {
        tryLogCurrentException(
            DB::Logger::get(),
            fmt::format("EditRecord::fromProto failed, proto={}", proto_edit.DebugString()));
        e.rethrow();
    }

    if (rec.type == EditRecordType::VAR_ENTRY)
    {
        // uploading page data may be disabled
        auto checkpoint_loc = CheckpointLocation::fromProto(proto_edit.entry_location(), strings_map);
        if (checkpoint_loc.isValid())
        {
            rec.entry.checkpoint_info = OptionalCheckpointInfo(std::move(checkpoint_loc), true, true);
        }
        rec.entry.size = proto_edit.entry_size();
        rec.entry.checksum = proto_edit.entry_checksum();
        rec.entry.tag = proto_edit.entry_tag();
        RUNTIME_CHECK(proto_edit.entry_fields_offset_size() == proto_edit.entry_fields_checksum_size());
        auto sz = proto_edit.entry_fields_offset_size();
        for (int i = 0; i < sz; ++i)
        {
            rec.entry.field_offsets.emplace_back(
                std::make_pair(proto_edit.entry_fields_offset(i), proto_edit.entry_fields_checksum(i)));
        }
        // Note: rec.entry.* is untouched, leaving zero value.
        // We need to take care when restoring the PS instance.
    }
    if (rec.type == EditRecordType::VAR_EXTERNAL)
    {
        rec.entry.checkpoint_info = OptionalCheckpointInfo(
            CheckpointLocation::fromProto(proto_edit.entry_location(), strings_map),
            true,
            true);
    }
    return rec;
}

} // namespace DB::PS::V3
