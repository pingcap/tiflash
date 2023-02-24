// Copyright 2023 PingCAP, Ltd.
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

Strings CheckpointManifestS3Set::perservedManifests() const
{
    // Now only perserve the latest manifest
    return {manifests.rbegin()->second.key};
}

} // namespace DB::S3
