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

#include <Core/Types.h>
#include <Storages/S3/S3Common.h>
#include <aws/core/utils/DateTime.h>
#include <aws/s3/S3Client.h>
#include <common/defines.h>

namespace DB::S3::tests
{
class MockS3Client final : public S3::TiFlashS3Client
{
public:
    MockS3Client()
        : TiFlashS3Client("")
    {}

    ~MockS3Client() override = default;

    void clear();

    Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest & r) const override;
    mutable Strings put_keys;

    Aws::S3::Model::DeleteObjectOutcome DeleteObject(const Aws::S3::Model::DeleteObjectRequest & r) const override;
    mutable Strings delete_keys;

    Aws::S3::Model::ListObjectsV2Outcome ListObjectsV2(const Aws::S3::Model::ListObjectsV2Request & r) const override;
    mutable Strings list_result;

    std::optional<Aws::Utils::DateTime> head_result_mtime;
    Aws::S3::Model::HeadObjectOutcome HeadObject(const Aws::S3::Model::HeadObjectRequest & request) const override;
};
} // namespace DB::S3::tests
