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
using namespace Aws::S3;

class MockS3Client final : public S3::TiFlashS3Client
{
public:
    explicit MockS3Client(const String & bucket = "")
        : TiFlashS3Client(bucket)
    {}

    ~MockS3Client() override = default;

    Model::GetObjectOutcome GetObject(const Model::GetObjectRequest & request) const override;
    Model::PutObjectOutcome PutObject(const Model::PutObjectRequest & request) const override;
    Model::ListObjectsV2Outcome ListObjectsV2(const Model::ListObjectsV2Request & request) const override;
    Model::CreateMultipartUploadOutcome CreateMultipartUpload(const Model::CreateMultipartUploadRequest & request) const override;
    Model::UploadPartOutcome UploadPart(const Model::UploadPartRequest & request) const override;
    Model::CompleteMultipartUploadOutcome CompleteMultipartUpload(const Model::CompleteMultipartUploadRequest & request) const override;
    Model::CreateBucketOutcome CreateBucket(const Model::CreateBucketRequest & request) const override;
    Model::DeleteObjectOutcome DeleteObject(const Model::DeleteObjectRequest & request) const override;
    Model::HeadObjectOutcome HeadObject(const Model::HeadObjectRequest & request) const override;

private:
    using BucketStorage = std::map<String, String>;
    using UploadParts = std::map<UInt64, String>;
    mutable std::mutex mtx;
    mutable std::unordered_map<String, BucketStorage> storage;
    mutable std::unordered_map<String, UploadParts> upload_parts;
};
} // namespace DB::S3::tests
