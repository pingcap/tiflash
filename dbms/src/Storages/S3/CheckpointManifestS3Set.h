#pragma once

#include <Core/Types.h>
#include <Storages/S3/S3Common.h>
#include <Storages/Transaction/Types.h>
#include <aws/core/utils/DateTime.h>
#include <common/types.h>

#include <map>
#include <vector>

namespace DB::S3
{
struct CheckpointManifestS3Object
{
    String key;
    Aws::Utils::DateTime last_modification;
};

class CheckpointManifestS3Set
{
public:
    static CheckpointManifestS3Set getFromS3(const S3::TiFlashS3Client & client, StoreID store_id);

    static CheckpointManifestS3Set create(std::vector<CheckpointManifestS3Object> manifest_keys);

    UInt64 latestUploadSequence() const
    {
        assert(!manifests.empty());
        return manifests.rbegin()->first;
    }

    const String & latestManifestKey() const
    {
        assert(!manifests.empty());
        return manifests.rbegin()->second.key;
    }

    Strings perservedManifests() const;

    Strings outdatedManifests(Aws::Utils::DateTime current_time, std::chrono::milliseconds expired_ms) const;

    std::map<UInt64, CheckpointManifestS3Object> objects() const { return manifests; }

private:
    // a order map to let values sorted by upload_seq
    // upload_seq -> {manifest_key, mtime}
    std::map<UInt64, CheckpointManifestS3Object> manifests;
};

} // namespace DB::Remote
