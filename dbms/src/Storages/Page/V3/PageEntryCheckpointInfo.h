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
#include <common/defines.h>

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
};

// A more memory compact struct compared to std::optional<CheckpointInfo>
class OptionalCheckpointInfo
{
public:
    OptionalCheckpointInfo(std::nullopt_t) // NOLINT(google-explicit-constructor)
        : is_set(false)
    {}
    OptionalCheckpointInfo(CheckpointLocation data_loc, bool is_local_data_reclaimed_)
        : data_location(std::move(data_loc))
        , is_set(true)
        , is_local_data_reclaimed(is_local_data_reclaimed_)
    {
    }

    // Mock as if std::optional<...>
    ALWAYS_INLINE bool has_value() const { return is_set; } // NOLINT(readability-identifier-naming)
    ALWAYS_INLINE OptionalCheckpointInfo * operator->()
    {
        assert(is_set); // should only be used when contains valid
        return this;
    }
    ALWAYS_INLINE const OptionalCheckpointInfo * operator->() const
    {
        assert(is_set); // should only be used when contains valid
        return this;
    }

    // copy ctor
    OptionalCheckpointInfo(const OptionalCheckpointInfo & rhs) = default;
    OptionalCheckpointInfo & operator=(const OptionalCheckpointInfo & rhs) = default;

    // move ctor
    OptionalCheckpointInfo(OptionalCheckpointInfo && c) noexcept = default;
    OptionalCheckpointInfo & operator=(OptionalCheckpointInfo && rhs) noexcept = default;
    OptionalCheckpointInfo(std::nullopt_t &&) noexcept // NOLINT(google-explicit-constructor)
        : data_location()
        , is_set(false)
        , is_local_data_reclaimed(false)
    {
    }

public:
    CheckpointLocation data_location;

    /**
     * Whether this object contains valid value or not
     */
    bool is_set = false;

    /**
     * Whether the PageEntry's local BlobData has been reclaimed.
     * If the data is reclaimed, you can only read out its data from the checkpoint.
     */
    bool is_local_data_reclaimed = false;
};

} // namespace DB::PS::V3
