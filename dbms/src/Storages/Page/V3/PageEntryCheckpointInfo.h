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

#include <Storages/Page/V3/CheckpointFile/Proto/manifest_file.pb.h>

namespace DB::PS::V3
{

namespace CheckpointProto
{
using StringsInternMap = std::unordered_map<std::string_view, std::shared_ptr<const std::string>>;
}

struct CheckpointLocation
{
    // This struct is highly coupled with manifest_file.proto -> EditEntry.

    std::shared_ptr<const std::string> data_file_id;

    uint64_t offset_in_file = 0;
    uint64_t size_in_file = 0;

    CheckpointProto::EntryDataLocation toProto() const
    {
        CheckpointProto::EntryDataLocation proto_rec;
        proto_rec.set_data_file_id(*data_file_id);
        proto_rec.set_offset_in_file(offset_in_file);
        proto_rec.set_size_in_file(size_in_file);
        return proto_rec;
    }

    /**
     * @param strings_map A modifyable map. This function will try to reuse strings in the map
     *                    or insert new strings into the map to share memory for the same string.
     */
    static CheckpointLocation fromProto(
        const CheckpointProto::EntryDataLocation & proto_rec,
        CheckpointProto::StringsInternMap & strings_map)
    {
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
};

struct CheckpointInfo
{
    CheckpointLocation data_location;

    /**
     * Whether the PageEntry's local BlobData has been reclaimed.
     * If the data is reclaimed, you can only read out its data from the checkpoint.
     */
    bool is_local_data_reclaimed = false;
};

} // namespace DB::PS::V3