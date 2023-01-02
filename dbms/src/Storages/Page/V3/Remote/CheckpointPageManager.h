#pragma once

#include <Storages/Page/V3/Remote/CheckpointManifestFileReader.h>
#include <Storages/Page/V3/PageDirectory.h>

namespace DB::PS::V3
{

struct CheckpointInfo
{
    String checkpoint_manifest_path;
    String checkpoint_data_dir;
    UInt64 checkpoint_store_id;
};

class CheckpointPageManager
{
public:
    explicit CheckpointPageManager(CheckpointManifestFileReader<PS::V3::universal::PageDirectoryTrait> & reader, const String & checkpoint_data_dir_)
        : edit(reader.read())
        , checkpoint_data_dir(checkpoint_data_dir_)
    {
        RUNTIME_CHECK(endsWith(checkpoint_data_dir, "/"));
    }

    // Only used for retrieve raft log, so no need to consider ref id
    std::vector<std::tuple<ReadBufferPtr, size_t, UniversalPageId>> getAllPageWithPrefix(const String & prefix) const;

    std::optional<UniversalPageId> getNormalPageId(const UniversalPageId & page_id, bool ignore_if_not_exist = false) const;

    // buf, size, size of each fields
    std::optional<std::tuple<ReadBufferPtr, size_t, PageFieldSizes>> getReadBuffer(const UniversalPageId & page_id, bool ignore_if_not_exist = false) const;

private:
    std::optional<typename PS::V3::universal::PageDirectoryTrait::EditRecord> findPageRecord(const UniversalPageId & page_id) const;

private:
    typename PS::V3::universal::PageDirectoryTrait::PageEntriesEdit edit;
    String checkpoint_data_dir;
};

using CheckpointPageManagerPtr = std::shared_ptr<CheckpointPageManager>;
}
