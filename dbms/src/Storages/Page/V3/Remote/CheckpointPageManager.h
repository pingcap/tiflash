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

    // TODO: remove `enable_linear_search` when the binary search is gurantee to be correct
    std::optional<UniversalPageId> getNormalPageId(const UniversalPageId & page_id, bool ignore_if_not_exist = false, bool enable_linear_search = true) const;

    // buf, size, size of each fields
    std::optional<std::tuple<ReadBufferPtr, size_t, PageFieldSizes>> getReadBuffer(const UniversalPageId & page_id, bool ignore_if_not_exist = false, bool enable_linear_search = true) const;

private:
    std::optional<typename PS::V3::universal::PageDirectoryTrait::EditRecord> findPageRecord(const UniversalPageId & page_id, bool enable_linear_search) const;

private:
    typename PS::V3::universal::PageDirectoryTrait::PageEntriesEdit edit;
    String checkpoint_data_dir;
};

using CheckpointPageManagerPtr = std::shared_ptr<CheckpointPageManager>;
}
