#include <Common/StringUtils/StringUtils.h>
#include <TestUtils/MockS3Client.h>
#include <aws/core/AmazonWebServiceRequest.h>
#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/PutObjectRequest.h>

namespace DB
{

Aws::S3::Model::PutObjectOutcome MockS3Client::PutObject(const Aws::S3::Model::PutObjectRequest & r) const
{
    put_keys.emplace_back(r.GetKey());
    return Aws::S3::Model::PutObjectOutcome{Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>{}};
}

Aws::S3::Model::DeleteObjectOutcome MockS3Client::DeleteObject(const Aws::S3::Model::DeleteObjectRequest & r) const
{
    delete_keys.emplace_back(r.GetKey());
    return Aws::S3::Model::DeleteObjectOutcome{Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>{}};
}

Aws::S3::Model::ListObjectsV2Outcome MockS3Client::ListObjectsV2(const Aws::S3::Model::ListObjectsV2Request & r) const
{
    Aws::S3::Model::ListObjectsV2Result resp;
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
            Aws::S3::Model::Object o;
            o.SetKey(k);
            resp.AddContents(o);
        }
    }
    return Aws::S3::Model::ListObjectsV2Outcome{resp};
}

Aws::S3::Model::HeadObjectOutcome MockS3Client::HeadObject(const Aws::S3::Model::HeadObjectRequest &) const
{
    if (!head_result_mtime)
    {
        Aws::Client::AWSError error(Aws::S3::S3Errors::NO_SUCH_KEY, false);
        return Aws::S3::Model::HeadObjectOutcome{error};
    }
    Aws::S3::Model::HeadObjectResult resp;
    resp.SetLastModified(head_result_mtime.value());
    return Aws::S3::Model::HeadObjectOutcome{resp};
}

void MockS3Client::clear()
{
    put_keys.clear();
    delete_keys.clear();
    list_result.clear();
    head_result_mtime.reset();
}


} // namespace DB
