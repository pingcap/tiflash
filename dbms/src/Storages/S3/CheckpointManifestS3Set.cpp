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

#include <Common/Exception.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Filename.h>

#include <unordered_set>

namespace DB::S3
{
CheckpointManifestS3Set CheckpointManifestS3Set::getFromS3(const S3::TiFlashS3Client & client, StoreID store_id)
{
    const auto manifest_prefix = S3::S3Filename::fromStoreId(store_id).toManifestPrefix();

    std::vector<CheckpointManifestS3Object> manifests;

    S3::listPrefix(client, manifest_prefix, [&](const Aws::S3::Model::Object & object) {
        const auto & mf_key = object.GetKey();
        // also store the object.GetLastModified() for removing
        // outdated manifest objects
        manifests.emplace_back(CheckpointManifestS3Object{mf_key, object.GetLastModified()});
        return DB::S3::PageResult{.num_keys = 1, .more = true};
    });
    return CheckpointManifestS3Set::create(manifests);
}

CheckpointManifestS3Set CheckpointManifestS3Set::create(const std::vector<CheckpointManifestS3Object> & manifest_keys)
{
    CheckpointManifestS3Set set;
    for (const auto & mf_obj : manifest_keys)
    {
        const auto filename_view = S3::S3FilenameView::fromKey(mf_obj.key);
        RUNTIME_CHECK(filename_view.type == S3::S3FilenameType::CheckpointManifest, mf_obj.key);
        auto upload_seq = filename_view.getUploadSequence();
        auto [iter, ok] = set.manifests.emplace(upload_seq, mf_obj);
        RUNTIME_CHECK_MSG(
            ok,
            "duplicated upload seq, prev_mf_key={} duplicated_mf_key={}",
            iter->second.key,
            mf_obj.key);
    }
    return set;
}

Strings CheckpointManifestS3Set::preservedManifests(
    size_t max_preserved,
    Int64 expired_hour,
    const Aws::Utils::DateTime & timepoint) const
{
    assert(!manifests.empty());

    Strings preserved_mf;
    // the latest manifest
    auto iter = manifests.rbegin();
    preserved_mf.emplace_back(iter->second.key);
    iter++; // move to next
    const auto expired_bound_sec = expired_hour * 3600;
    for (; iter != manifests.rend(); ++iter)
    {
        auto diff_sec = Aws::Utils::DateTime::Diff(timepoint, iter->second.last_modification).count() / 1000.0;
        if (diff_sec > expired_bound_sec)
        {
            break;
        }

        preserved_mf.emplace_back(iter->second.key);
        if (preserved_mf.size() >= max_preserved)
        {
            break;
        }
    }
    return preserved_mf;
}

std::vector<CheckpointManifestS3Object> CheckpointManifestS3Set::outdatedObjects(
    size_t max_preserved,
    Int64 expired_hour,
    const Aws::Utils::DateTime & timepoint) const
{
    auto preserved_mfs = preservedManifests(max_preserved, expired_hour, timepoint);
    std::unordered_set<String> preserved_set;
    for (const auto & s : preserved_mfs)
        preserved_set.emplace(s);

    // the manifest object that does not appear in reserved set
    std::vector<CheckpointManifestS3Object> outdated;
    for (const auto & [seq, obj] : manifests)
    {
        if (preserved_set.count(obj.key) > 0)
            continue;
        outdated.emplace_back(obj);
    }
    return outdated;
}

} // namespace DB::S3
