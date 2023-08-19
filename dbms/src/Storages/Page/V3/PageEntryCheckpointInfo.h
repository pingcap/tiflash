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

#include <Storages/Page/V3/CheckpointFile/Proto/manifest_file.pb.h>
#include <common/defines.h>
#include <fmt/format.h>

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

    CheckpointLocation copyWithNewDataFileId(std::shared_ptr<const std::string> new_file_id);

    CheckpointProto::EntryDataLocation toProto() const;

    /**
     * @param strings_map A modifyable map. This function will try to reuse strings in the intern map
     *                    or insert new strings into the intern map to share memory for the same string.
     */
    static CheckpointLocation fromProto(
        const CheckpointProto::EntryDataLocation & proto_rec,
        CheckpointProto::StringsInternMap & strings_map);

    std::string toDebugString() const
    {
        return fmt::format("{{data_file_id: {}, offset_in_file: {}, size_in_file: {}}}", *data_file_id, offset_in_file, size_in_file);
    }
};

// A more memory compact struct compared to std::optional<CheckpointInfo>
struct OptionalCheckpointInfo
{
    CheckpointLocation data_location;

    /**
     * Whether this object contains valid value or not
     *
     * Share the padding with following bits in this struct
     */
    bool is_valid = false;

    /**
     * Whether the PageEntry's local BlobData has been reclaimed.
     * If the data is reclaimed, you can only read out its data from the checkpoint.
     */
    bool is_local_data_reclaimed = false;

    std::string toDebugString() const
    {
        if (is_valid)
        {
            return fmt::format("{{local_data_reclaimed: {}, data_location: {}}}", is_local_data_reclaimed, data_location.toDebugString());
        }
        else
        {
            return "invalid";
        }
    }

public:
    ALWAYS_INLINE bool has_value() const { return is_valid; } // NOLINT(readability-identifier-naming)
};

} // namespace DB::PS::V3
