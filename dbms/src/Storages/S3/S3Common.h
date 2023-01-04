#pragma once

#include <Common/nocopyable.h>
#include <Core/Types.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <common/types.h>

namespace DB::S3
{

class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    void init(bool enable_s3_log);
    void shutdown();

    std::unique_ptr<Aws::S3::S3Client> create(
        const String & endpoint,
        Aws::Http::Scheme scheme,
        bool verifySSL,
        const String & access_key_id,
        const String & secret_access_key);

private:
    ClientFactory() = default;
    DISALLOW_COPY_AND_MOVE(ClientFactory);

    Aws::SDKOptions aws_options;
};

struct ObjectInfo
{
    size_t size = 0;
    time_t last_modification_time = 0;
};

bool isNotFoundError(Aws::S3::S3Errors error);

Aws::S3::Model::HeadObjectOutcome headObject(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id = "", bool for_disk_s3 = false);

S3::ObjectInfo getObjectInfo(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool throw_on_error, bool for_disk_s3);

size_t getObjectSize(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool throw_on_error, bool for_disk_s3);

bool objectExists(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id = "", bool for_disk_s3 = false);

void uploadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname);

void uploadEmptyFile(const Aws::S3::S3Client & client, const String & bucket, const String & remote_fname);

void downloadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname);

void listPrefix(
    const Aws::S3::S3Client & client,
    const String & bucket,
    const String & prefix,
    const String & delimiter,
    std::function<size_t(const Aws::S3::Model::ListObjectsV2Result & result)> pager);

std::unordered_map<String, size_t> listPrefixWithSize(const Aws::S3::S3Client & client, const String & bucket, const String & prefix);


std::pair<bool, Aws::Utils::DateTime> tryGetObjectModifiedTime(
    const Aws::S3::S3Client & client,
    const String & bucket,
    const String & key);

void deleteObject(const Aws::S3::S3Client & client, const String & bucket, const String & key);

} // namespace DB::S3
