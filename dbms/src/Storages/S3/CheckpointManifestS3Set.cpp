#include <Common/Exception.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Filename.h>

namespace DB::S3
{
CheckpointManifestS3Set
CheckpointManifestS3Set::getFromS3(const S3::TiFlashS3Client & client, StoreID store_id)
{
    const auto store_prefix = S3::S3Filename::fromStoreId(store_id).toManifestPrefix();

    std::vector<CheckpointManifestS3Object> manifests;

    listPrefix(client, client.bucket(), store_prefix, [&](const Aws::S3::Model::ListObjectsV2Result & result) {
        const auto & objects = result.GetContents();
        manifests.reserve(manifests.size() + objects.size());
        for (const auto & object : objects)
        {
            const auto & mf_key = object.GetKey();
            // also store the object.GetLastModified() for removing
            // outdated manifest objects
            manifests.emplace_back(CheckpointManifestS3Object{mf_key, object.GetLastModified()});
        }
        return DB::S3::PageResult{.num_keys = objects.size(), .more = true};
    });
    return CheckpointManifestS3Set::create(std::move(manifests));
}

Strings CheckpointManifestS3Set::perservedManifests() const
{
    // Now only perserve the latest manifest
    return {manifests.rbegin()->second.key};
}

CheckpointManifestS3Set
CheckpointManifestS3Set::create(std::vector<CheckpointManifestS3Object> manifest_keys)
{
    CheckpointManifestS3Set set;
    for (const auto & mf_obj : manifest_keys)
    {
        const auto filename_view = S3::S3FilenameView::fromKey(mf_obj.key);
        RUNTIME_CHECK(filename_view.type == S3::S3FilenameType::CheckpointManifest, mf_obj.key);
        auto upload_seq = filename_view.getUploadSequence();
        auto [iter, ok] = set.manifests.emplace(upload_seq, mf_obj);
        RUNTIME_CHECK_MSG(ok, "duplicated upload seq, prev_mf_key={} duplicated_mf_key={}", iter->second.key, mf_obj.key);
    }
    return set;
}

Strings CheckpointManifestS3Set::outdatedManifests(Aws::Utils::DateTime current_time, std::chrono::milliseconds expired_ms) const
{
    Strings outdated_keys;
    for (const auto & mf : manifests)
    {
        if (auto diff_ms = Aws::Utils::DateTime::Diff(current_time, mf.second.last_modification);
            diff_ms > expired_ms)
        {
            outdated_keys.emplace_back(mf.second.key);
        }
    }
    return outdated_keys;
}

} // namespace DB::S3
