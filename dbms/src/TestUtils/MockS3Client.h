#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <common/defines.h>

namespace DB
{
class MockS3Client final : public Aws::S3::S3Client
{
public:
    MockS3Client() = default;

    ~MockS3Client() override = default;

    Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest & request) const override
    {
        UNUSED(request);
        return Aws::S3::Model::PutObjectOutcome{Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>{}};
    }
};
} // namespace DB
