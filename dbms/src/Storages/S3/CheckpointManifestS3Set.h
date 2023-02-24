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

    std::map<UInt64, CheckpointManifestS3Object> objects() const { return manifests; }

private:
    // a order map to let values sorted by upload_seq
    // upload_seq -> {manifest_key, mtime}
    std::map<UInt64, CheckpointManifestS3Object> manifests;
};

} // namespace DB::S3
