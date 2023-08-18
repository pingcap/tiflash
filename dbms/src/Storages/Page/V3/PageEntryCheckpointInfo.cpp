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

#include <Storages/Page/V3/PageEntryCheckpointInfo.h>

namespace DB::PS::V3
{

CheckpointLocation CheckpointLocation::copyWithNewDataFileId(std::shared_ptr<const std::string> new_file_id)
{
    return CheckpointLocation{
        .data_file_id = std::move(new_file_id),
        .offset_in_file = this->offset_in_file,
        .size_in_file = this->size_in_file,
    };
}

CheckpointProto::EntryDataLocation CheckpointLocation::toProto() const
{
    CheckpointProto::EntryDataLocation proto_rec;
    proto_rec.set_data_file_id(*data_file_id);
    proto_rec.set_offset_in_file(offset_in_file);
    proto_rec.set_size_in_file(size_in_file);
    return proto_rec;
}

CheckpointLocation CheckpointLocation::fromProto(
    const CheckpointProto::EntryDataLocation & proto_rec,
    CheckpointProto::StringsInternMap & strings_map)
{
    // Try to reuse the same data_file_id from the string intern map.
    // Usually a lot of entries are placed in the same data file. This reduces memory overhead.
    std::shared_ptr<const std::string> data_file_id = nullptr;
    if (auto it = strings_map.find(proto_rec.data_file_id()); it != strings_map.end())
    {
        data_file_id = it->second;
    }
    else
    {
        data_file_id = std::make_shared<std::string>(proto_rec.data_file_id());
        strings_map.try_emplace(*data_file_id, data_file_id);
    }

    CheckpointLocation val;
    val.data_file_id = data_file_id;
    val.offset_in_file = proto_rec.offset_in_file();
    val.size_in_file = proto_rec.size_in_file();
    return val;
}

} // namespace DB::PS::V3
