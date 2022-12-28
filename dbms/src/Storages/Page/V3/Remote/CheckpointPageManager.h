#pragma once

#include <Storages/Page/V3/Remote/CheckpointManifestFileReader.h>
#include <Storages/Page/V3/PageDirectory.h>

namespace DB::PS::V3
{

class CheckpointPageManager
{
public:
    explicit CheckpointPageManager(CheckpointManifestFileReader<PS::V3::universal::PageDirectoryTrait> & reader, const String & checkpoint_dir_)
        : edit(reader.read())
        , checkpoint_dir(checkpoint_dir_)
    {
        RUNTIME_CHECK(endsWith(checkpoint_dir, "/"));
    }

    // Only used for retrieve raft log, so no need to consider ref id
    std::vector<std::tuple<ReadBufferPtr, size_t, UniversalPageId>> getAllPageWithPrefix(const String & prefix) const;

    // buf, size, size of each fields
    std::tuple<ReadBufferPtr, size_t, PageFieldSizes> getReadBuffer(const UniversalPageId & page_id) const;

private:
    typename PS::V3::universal::PageDirectoryTrait::PageEntriesEdit edit;
    String checkpoint_dir;
};
}
