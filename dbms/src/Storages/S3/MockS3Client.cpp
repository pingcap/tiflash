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
#include <Common/Logger.h>
#include <Common/StringUtils/StringUtils.h>
#include <Storages/S3/MockS3Client.h>
#include <aws/core/AmazonWebServiceRequest.h>
#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/NoResult.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/CommonPrefix.h>
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
#include <aws/s3/model/GetObjectTaggingResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <boost_wrapper/string_split.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <fiu.h>

#include <boost/algorithm/string/classification.hpp>
#include <mutex>
#include <string_view>

namespace DB::S3::tests
{
using namespace Aws::S3;

String MockS3Client::normalizedKey(String ori_key)
{
    if (ori_key.starts_with('/'))
        return ori_key.substr(1, ori_key.size());
    return ori_key;
}

Model::GetObjectOutcome MockS3Client::GetObject(const Model::GetObjectRequest & request) const
{
    std::lock_guard lock(mtx);
    auto itr = storage.find(request.GetBucket());
    if (itr == storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchBucket");
    }
    const auto & bucket_storage = itr->second;
    auto itr_obj = bucket_storage.find(normalizedKey(request.GetKey()));
    if (itr_obj == bucket_storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchKey");
    }
    UInt64 left = 0;
    UInt64 right = itr_obj->second.size() - 1;
    if (request.RangeHasBeenSet())
    {
        std::vector<String> v;
        constexpr std::string_view prefix = "bytes=";
        boost::algorithm::split(v, request.GetRange().substr(prefix.size()), boost::algorithm::is_any_of("-"));
        RUNTIME_CHECK(v.size() == 2, request.GetRange());
        left = std::stoul(v[0]);
        if (!v[1].empty())
        {
            right = std::stoul(v[1]);
        }
    }
    auto size = right - left + 1;
    Model::GetObjectResult result;
    auto * ss = Aws::New<Aws::StringStream>("MockS3Client");
    *ss << itr_obj->second.substr(left, size);
    result.ReplaceBody(ss);
    result.SetContentLength(size);
    return result;
}

Model::PutObjectOutcome MockS3Client::PutObject(const Model::PutObjectRequest & request) const
{
    if (put_object_status == S3Status::FAILED)
        return Aws::S3::S3ErrorMapper::GetErrorForName("");
    std::lock_guard lock(mtx);
    auto itr = storage.find(request.GetBucket());
    if (itr == storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchBucket");
    }
    auto & bucket_storage = itr->second;
    bucket_storage[normalizedKey(request.GetKey())] = String{std::istreambuf_iterator<char>(*request.GetBody()), {}};
    return Model::PutObjectResult{};
}

Model::CopyObjectOutcome MockS3Client::CopyObject(const Model::CopyObjectRequest & request) const
{
    std::lock_guard lock(mtx);
    auto first_pos = request.GetCopySource().find_first_of('/');
    RUNTIME_CHECK(first_pos != String::npos, request.GetCopySource());
    auto src_bucket = request.GetCopySource().substr(0, first_pos);
    auto src_key = request.GetCopySource().substr(first_pos + 1, request.GetCopySource().size());

    auto src_itr = storage.find(src_bucket);
    if (src_itr == storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuckBucket");
    }

    auto dst_itr = storage.find(request.GetBucket());
    if (dst_itr == storage.end())
    {
        return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuckBucket");
    }

    auto src_bucket_storage = src_itr->second;
    auto dst_bucket_storage = dst_itr->second;
    RUNTIME_CHECK(src_bucket_storage.contains(src_key), src_bucket, src_key);
    dst_bucket_storage[normalizedKey(request.GetKey())] = src_bucket_storage[src_key];
    storage_tagging[request.GetBucket()][normalizedKey(request.GetKey())] = request.GetTagging();
    return Model::CopyObjectResult{};
}

Model::GetObjectTaggingOutcome MockS3Client::GetObjectTagging(const Model::GetObjectTaggingRequest & request) const
{
    std::lock_guard lock(mtx);
    bool object_exist = false;
    {
        auto itr = storage.find(request.GetBucket());
        if (itr == storage.end())
        {
            return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuckBucket");
        }
        object_exist = itr->second.count(normalizedKey(request.GetKey())) > 0;
    }

    auto object_tagging_iter = storage_tagging[request.GetBucket()].find(normalizedKey(request.GetKey()));
    RUNTIME_CHECK_MSG(
        object_exist,
        "try to get tagging of non-exist object, bucket={} key={}",
        request.GetBucket(),
        request.GetKey());

    // object exist but tag not exist, consider it as empty
    if (object_tagging_iter == storage_tagging[request.GetBucket()].end())
    {
        return Model::GetObjectTaggingResult{};
    }

    auto taggings = object_tagging_iter->second;
    auto pos = taggings.find('=');
    RUNTIME_CHECK(pos != String::npos, taggings, pos, taggings.size());
    Aws::S3::Model::Tag tag;
    tag.WithKey(taggings.substr(0, pos)).WithValue(taggings.substr(pos + 1, taggings.size()));
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
    bucket_storage.erase(normalizedKey(request.GetKey()));
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
    RUNTIME_CHECK(!request.DelimiterHasBeenSet() || request.GetDelimiter() == "/", request.GetDelimiter());

    auto normalized_prefix = normalizedKey(request.GetPrefix());
    if (!request.DelimiterHasBeenSet())
    {
        for (auto itr_obj = bucket_storage.lower_bound(normalized_prefix); itr_obj != bucket_storage.end(); ++itr_obj)
        {
            if (startsWith(itr_obj->first, normalized_prefix))
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
    }
    else
    {
        std::set<String> common_prefix;
        const auto & delimiter = request.GetDelimiter();
        for (auto itr_obj = bucket_storage.lower_bound(normalized_prefix); itr_obj != bucket_storage.end(); ++itr_obj)
        {
            if (!startsWith(itr_obj->first, normalized_prefix))
                break;
            const auto & key = itr_obj->first;
            auto pos = key.find(delimiter, normalized_prefix.size());
            if (pos == String::npos)
                continue;
            common_prefix.insert(key.substr(0, pos + delimiter.size()));
        }
        for (const auto & p : common_prefix)
        {
            result.AddCommonPrefixes(Model::CommonPrefix().WithPrefix(p));
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
    auto itr_obj = bucket_storage.find(normalizedKey(request.GetKey()));
    if (itr_obj != bucket_storage.end())
    {
        auto r = Model::HeadObjectResult{};
        r.SetContentLength(itr_obj->second.size());
        return r;
    }
    return Aws::S3::S3ErrorMapper::GetErrorForName("NoSuchKey");
}

Model::CreateMultipartUploadOutcome MockS3Client::CreateMultipartUpload(
    const Model::CreateMultipartUploadRequest & /*request*/) const
{
    static std::atomic<UInt64> upload_id{0};
    Model::CreateMultipartUploadResult result;
    result.SetUploadId(std::to_string(upload_id++));
    return result;
}

Model::UploadPartOutcome MockS3Client::UploadPart(const Model::UploadPartRequest & request) const
{
    std::lock_guard lock(mtx);
    upload_parts[request.GetUploadId()][request.GetPartNumber()]
        = String{std::istreambuf_iterator<char>(*request.GetBody()), {}};
    Model::UploadPartResult result;
    result.SetETag(std::to_string(request.GetPartNumber()));
    return result;
}

Model::CompleteMultipartUploadOutcome MockS3Client::CompleteMultipartUpload(
    const Model::CompleteMultipartUploadRequest & request) const
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
    bucket_storage[normalizedKey(request.GetKey())] = s;
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

Model::GetBucketLifecycleConfigurationOutcome MockS3Client::GetBucketLifecycleConfiguration(
    const Model::GetBucketLifecycleConfigurationRequest & request) const
{
    // just mock a stub
    UNUSED(request);
    return Model::GetBucketLifecycleConfigurationResult();
}

Model::PutBucketLifecycleConfigurationOutcome MockS3Client::PutBucketLifecycleConfiguration(
    const Model::PutBucketLifecycleConfigurationRequest & request) const
{
    // just mock a stub
    UNUSED(request);
    return Aws::NoResult();
}

} // namespace DB::S3::tests
