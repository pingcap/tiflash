#pragma once

// this include is in a mess, refine it later
#include <Storages/Page/V3/Remote/Proto/manifest_file.pb.h>
#include <common/types.h>

#include <memory>

namespace DB::PS
{
struct RemoteDataLocation
{
    // This struct is highly coupled with manifest_file.proto -> EditEntry.

    std::shared_ptr<const String> data_file_id;

    UInt64 offset_in_file{0};
    UInt64 size_in_file{0};

    RemoteDataLocation copyWithNewFilename(String && filename)
    {
        return RemoteDataLocation{
            .data_file_id = std::make_shared<String>(filename),
            .offset_in_file = offset_in_file,
            .size_in_file = size_in_file,
        };
    }

    V3::Remote::EntryDataLocation toRemote() const
    {
        V3::Remote::EntryDataLocation remote_val;
        remote_val.set_data_file_id(*data_file_id);
        remote_val.set_offset_in_file(offset_in_file);
        remote_val.set_size_in_file(size_in_file);
        return remote_val;
    }

    static RemoteDataLocation fromRemote(const V3::Remote::EntryDataLocation & remote_rec)
    {
        RemoteDataLocation val;
        // TODO: This does not share the same memory for identical data files, wasting memory usage.
        val.data_file_id = std::make_shared<std::string>(remote_rec.data_file_id());
        val.offset_in_file = remote_rec.offset_in_file();
        val.size_in_file = remote_rec.size_in_file();
        return val;
    }
};
} // namespace DB::PS
