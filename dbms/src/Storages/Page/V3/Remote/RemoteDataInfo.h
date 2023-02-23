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

#include <Storages/Page/V3/Remote/Proto/manifest_file.pb.h>

namespace DB::PS::V3
{
struct RemoteDataLocation
{
    // This struct is highly coupled with manifest_file.proto -> EditEntry.

    std::shared_ptr<const std::string> data_file_id;

    uint64_t offset_in_file;
    uint64_t size_in_file;

    uint64_t store_id; // TODO: make it optional

    Remote::EntryDataLocation toRemote() const
    {
        Remote::EntryDataLocation remote_val;
        remote_val.set_data_file_id(*data_file_id);
        remote_val.set_offset_in_file(offset_in_file);
        remote_val.set_size_in_file(size_in_file);
        remote_val.set_store_id(store_id);
        return remote_val;
    }

    static RemoteDataLocation fromRemote(const Remote::EntryDataLocation & remote_rec)
    {
        RemoteDataLocation val;
        // TODO: This does not share the same memory for identical data files, wasting memory usage.
        val.data_file_id = std::make_shared<std::string>(remote_rec.data_file_id());
        val.offset_in_file = remote_rec.offset_in_file();
        val.size_in_file = remote_rec.size_in_file();
        val.store_id = remote_rec.store_id();
        return val;
    }
};

struct RemoteDataInfo
{
    RemoteDataLocation data_location;

    /**
     * Whether the PageEntry's local BlobData has been reclaimed.
     * If the data is reclaimed, you can only read out its data from the remote.
     */
    bool is_local_data_reclaimed = false;
};
}
