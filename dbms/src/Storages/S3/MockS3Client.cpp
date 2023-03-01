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

#include <Common/StringUtils/StringUtils.h>
#include <Storages/S3/MockS3Client.h>
#include <aws/core/AmazonWebServiceRequest.h>
#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/PutObjectRequest.h>

namespace DB::S3::tests
{
using namespace Aws::S3;

Model::PutObjectOutcome MockS3Client::PutObject(const Model::PutObjectRequest & r) const
{
    put_keys.emplace_back(r.GetKey());
    return Model::PutObjectOutcome{Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>{}};
}

Model::DeleteObjectOutcome MockS3Client::DeleteObject(const Model::DeleteObjectRequest & r) const
{
    delete_keys.emplace_back(r.GetKey());
    return Model::DeleteObjectOutcome{Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>{}};
}

Model::ListObjectsV2Outcome MockS3Client::ListObjectsV2(const Model::ListObjectsV2Request & r) const
{
    Model::ListObjectsV2Result resp;
    for (const auto & k : put_keys)
    {
        if (startsWith(k, r.GetPrefix()))
        {
            bool is_deleted = false;
            for (const auto & d : delete_keys)
            {
                if (k == d)
                {
                    is_deleted = true;
                    break;
                }
            }
            if (is_deleted)
                continue;
            Model::Object o;
            o.SetKey(k);
            resp.AddContents(o);
        }
    }
    for (const auto & k : list_result)
    {
        if (startsWith(k, r.GetPrefix()))
        {
            bool is_deleted = false;
            for (const auto & d : delete_keys)
            {
                if (k == d)
                {
                    is_deleted = true;
                    break;
                }
            }
            if (is_deleted)
                continue;
            Model::Object o;
            o.SetKey(k);
            resp.AddContents(o);
        }
    }
    return Model::ListObjectsV2Outcome{resp};
}

Model::HeadObjectOutcome MockS3Client::HeadObject(const Model::HeadObjectRequest & r) const
{
    for (const auto & k : put_keys)
    {
        if (r.GetKey() == k)
        {
            Model::HeadObjectResult resp;
            return Model::HeadObjectOutcome{resp};
        }
    }

    if (!head_result_mtime)
    {
        Aws::Client::AWSError error(S3Errors::NO_SUCH_KEY, false);
        return Model::HeadObjectOutcome{error};
    }
    Model::HeadObjectResult resp;
    resp.SetLastModified(head_result_mtime.value());
    return Model::HeadObjectOutcome{resp};
}

void MockS3Client::clear()
{
    put_keys.clear();
    delete_keys.clear();
    list_result.clear();
    head_result_mtime.reset();
}

} // namespace DB::S3::tests
