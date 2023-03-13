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
#include <Common/FailPoint.h>
#include <Common/StringUtils/StringUtils.h>
#include <Storages/S3/MockS3Client.h>
#include <aws/core/AmazonWebServiceRequest.h>
#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateBucketResult.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/GetObjectTaggingRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <common/types.h>
#include <fiu.h>

#include <mutex>

namespace DB::FailPoints
{
extern const char force_set_mocked_s3_object_mtime[];
} // namespace DB::FailPoints
namespace DB::S3::tests
{
using namespace Aws::S3;

Model::GetObjectOutcome MockS3Client::GetObject(const Model::GetObjectRequest & request) const
{
    std::lock_guard lock(mtx);
    auto itr = storage.find(request.GetBucket());
    if (itr == storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchBucket");
    }
    const auto & bucket_storage = itr->second;
    auto itr_obj = bucket_storage.find(request.GetKey());
    if (itr_obj == bucket_storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchKey");
    }
    auto * ss = new std::stringstream(itr_obj->second);
    Model::GetObjectResult result;
    result.ReplaceBody(ss);
    result.SetContentLength(itr_obj->second.size());
    return result;
}

Model::PutObjectOutcome MockS3Client::PutObject(const Model::PutObjectRequest & request) const
{
    std::lock_guard lock(mtx);
    auto itr = storage.find(request.GetBucket());
    if (itr == storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchBucket");
    }
    auto & bucket_storage = itr->second;
    bucket_storage[request.GetKey()] = String{std::istreambuf_iterator<char>(*request.GetBody()), {}};
    return Model::PutObjectResult{};
}

Model::CopyObjectOutcome MockS3Client::CopyObject(const Model::CopyObjectRequest & request) const
{
    std::lock_guard lock(mtx);
    auto itr = storage.find(request.GetBucket());
    if (itr == storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuckBucket");
    }
    auto bucket_storage = itr->second;
    bucket_storage[request.GetKey()] = bucket_storage[request.GetCopySource()];
    storage_tagging[request.GetBucket()][request.GetKey()] = request.GetTagging();
    return Model::CopyObjectResult{};
}

Model::GetObjectTaggingOutcome MockS3Client::GetObjectTagging(const Model::GetObjectTaggingRequest & request) const
{
    std::lock_guard lock(mtx);
    auto itr = storage_tagging.find(request.GetBucket());
    if (itr == storage_tagging.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuckBucket");
    }
    auto taggings = storage_tagging[request.GetBucket()][request.GetKey()];
    auto pos = taggings.find('=');
    RUNTIME_CHECK(pos != String::npos, pos, taggings.size());
    Aws::S3::Model::Tag tag;
    tag.WithKey(taggings.substr(0, pos))
        .WithValue(taggings.substr(pos + 1, taggings.size()));
    auto r = Model::GetObjectTaggingResult{};
    r.AddTagSet(tag);
    return r;
}

Model::DeleteObjectOutcome MockS3Client::DeleteObject(const Model::DeleteObjectRequest & request) const
{
    std::lock_guard lock(mtx);
    auto itr = storage.find(request.GetBucket());
    if (itr == storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchBucket");
    }
    auto & bucket_storage = itr->second;
    bucket_storage.erase(request.GetKey());
    return Model::DeleteObjectResult{};
}

Model::ListObjectsV2Outcome MockS3Client::ListObjectsV2(const Model::ListObjectsV2Request & request) const
{
    std::lock_guard lock(mtx);
    auto itr = storage.find(request.GetBucket());
    if (itr == storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchBucket");
    }
    const auto & bucket_storage = itr->second;
    Model::ListObjectsV2Result result;
    for (auto itr_obj = bucket_storage.lower_bound(request.GetPrefix()); itr_obj != bucket_storage.end(); ++itr_obj)
    {
        if (startsWith(itr_obj->first, request.GetPrefix()))
        {
            Model::Object obj;
            obj.SetKey(itr_obj->first);
            obj.SetSize(itr_obj->second.size());
            result.AddContents(std::move(obj));
        }
        else
        {
            break;
        }
    }
    return result;
}

Model::HeadObjectOutcome MockS3Client::HeadObject(const Model::HeadObjectRequest & request) const
{
    std::lock_guard lock(mtx);
    auto itr = storage.find(request.GetBucket());
    if (itr == storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchBucket");
    }
    const auto & bucket_storage = itr->second;
    auto itr_obj = bucket_storage.find(request.GetKey());
    if (itr_obj != bucket_storage.end())
    {
        auto r = Model::HeadObjectResult{};
        auto try_set_mtime = [&] {
            if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_set_mocked_s3_object_mtime); v)
            {
                auto m = std::any_cast<std::map<String, Aws::Utils::DateTime>>(v.value());
                if (auto iter_m = m.find(request.GetKey()); iter_m != m.end())
                {
                    r.SetLastModified(iter_m->second);
                }
            }
        };
        UNUSED(try_set_mtime);
        fiu_do_on(FailPoints::force_set_mocked_s3_object_mtime, { try_set_mtime(); });
        return r;
    }
    return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchKey");
}

Model::CreateMultipartUploadOutcome MockS3Client::CreateMultipartUpload(const Model::CreateMultipartUploadRequest & /*request*/) const
{
    static std::atomic<UInt64> upload_id{0};
    Model::CreateMultipartUploadResult result;
    result.SetUploadId(std::to_string(upload_id++));
    return result;
}

Model::UploadPartOutcome MockS3Client::UploadPart(const Model::UploadPartRequest & request) const
{
    std::lock_guard lock(mtx);
    upload_parts[request.GetUploadId()][request.GetPartNumber()] = String{std::istreambuf_iterator<char>(*request.GetBody()), {}};
    Model::UploadPartResult result;
    result.SetETag(std::to_string(request.GetPartNumber()));
    return result;
}

Model::CompleteMultipartUploadOutcome MockS3Client::CompleteMultipartUpload(const Model::CompleteMultipartUploadRequest & request) const
{
    std::lock_guard lock(mtx);
    const auto & parts = upload_parts[request.GetUploadId()];
    String s;
    for (const auto & p : parts)
    {
        s += p.second;
    }
    auto itr = storage.find(request.GetBucket());
    if (itr == storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchBucket");
    }
    auto & bucket_storage = itr->second;
    bucket_storage[request.GetKey()] = s;
    return Model::CompleteMultipartUploadResult{};
}

Model::CreateBucketOutcome MockS3Client::CreateBucket(const Model::CreateBucketRequest & request) const
{
    std::lock_guard lock(mtx);
    [[maybe_unused]] auto & bucket_storage = storage[request.GetBucket()];
    [[maybe_unused]] auto & bucket_storage_tagging = storage_tagging[request.GetBucket()];
    return Model::CreateBucketResult{};
}

Model::DeleteBucketOutcome MockS3Client::DeleteBucket(const Model::DeleteBucketRequest & request) const
{
    std::lock_guard lock(mtx);
    storage.erase(request.GetBucket());
    storage_tagging.erase(request.GetBucket());
    return Model::DeleteBucketOutcome{};
}


} // namespace DB::S3::tests
