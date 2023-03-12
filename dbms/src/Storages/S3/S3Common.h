// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <Common/nocopyable.h>
#include <Server/StorageConfigParser.h>
#include <aws/core/Aws.h>
#include <aws/core/http/Scheme.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <common/types.h>

#include <magic_enum.hpp>

namespace DB::ErrorCodes
{
extern const int S3_ERROR;
}

namespace DB::S3
{
template <typename... Args>
Exception fromS3Error(const Aws::S3::S3Error & e, const std::string & fmt, Args &&... args)
{
    return DB::Exception(
        ErrorCodes::S3_ERROR,
        fmt + fmt::format(" s3error={} s3msg={}", magic_enum::enum_name(e.GetErrorType()), e.GetMessage()),
        args...);
}

class TiFlashS3Client : public Aws::S3::S3Client
{
public:
    // Usually one tiflash instance only need access one bucket.
    // Store the bucket name to simpilfy some param passing.

    explicit TiFlashS3Client(const String & bucket_name_);

    TiFlashS3Client(
        const String & bucket_name_,
        const Aws::Auth::AWSCredentials & credentials,
        const Aws::Client::ClientConfiguration & clientConfiguration,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads,
        bool useVirtualAddressing);

    TiFlashS3Client(const String & bucket_name_, std::unique_ptr<Aws::S3::S3Client> && raw_client);

    const String & bucket() const { return bucket_name; }

private:
    const String bucket_name;
};

enum class S3GCMethod
{
    Lifecycle,
    ScanThenDelete,
};

class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    bool isEnabled() const;

    void init(const StorageS3Config & config_, bool mock_s3_ = false);

    void shutdown();

    const String & bucket() const;
    std::shared_ptr<Aws::S3::S3Client> sharedClient() const;

    std::shared_ptr<TiFlashS3Client> sharedTiFlashClient() const;

    S3GCMethod gc_method = S3GCMethod::Lifecycle;

private:
    ClientFactory() = default;
    DISALLOW_COPY_AND_MOVE(ClientFactory);
    std::unique_ptr<Aws::S3::S3Client> create() const;

    static std::unique_ptr<Aws::S3::S3Client> create(const StorageS3Config & config_);
    static Aws::Http::Scheme parseScheme(std::string_view endpoint);

    Aws::SDKOptions aws_options;
    StorageS3Config config;
    std::shared_ptr<Aws::S3::S3Client> shared_client;
    std::shared_ptr<TiFlashS3Client> shared_tiflash_client;
};

struct ObjectInfo
{
    size_t size = 0;
    time_t last_modification_time = 0;
};

bool isNotFoundError(Aws::S3::S3Errors error);

Aws::S3::Model::HeadObjectOutcome headObject(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id = "");

S3::ObjectInfo getObjectInfo(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool throw_on_error);

size_t getObjectSize(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool throw_on_error);

bool objectExists(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id = "");

void uploadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname);

constexpr std::string_view TaggingObjectIsDeleted = "tiflash_deleted=true";
void ensureLifecycleRuleExist(const Aws::S3::S3Client & client, const String & bucket, Int32 expire_days);

/**
 * tagging is the tag-set for the object. The tag-set must be encoded as URL Query
 * parameters. (For example, "Key1=Value1")
 */
void uploadEmptyFile(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & tagging = "");

void downloadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname);

void rewriteObjectWithTagging(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & tagging);

struct PageResult
{
    size_t num_keys;
    // true - continue to call next `LIST` when available
    // false - stop `LIST`
    bool more;
};
void listPrefix(
    const Aws::S3::S3Client & client,
    const String & bucket,
    const String & prefix,
    std::function<PageResult(const Aws::S3::Model::ListObjectsV2Result & result)> pager);
void listPrefix(
    const Aws::S3::S3Client & client,
    const String & bucket,
    const String & prefix,
    std::string_view delimiter,
    std::function<PageResult(const Aws::S3::Model::ListObjectsV2Result & result)> pager);

std::unordered_map<String, size_t> listPrefixWithSize(const Aws::S3::S3Client & client, const String & bucket, const String & prefix);


std::pair<bool, Aws::Utils::DateTime> tryGetObjectModifiedTime(
    const Aws::S3::S3Client & client,
    const String & bucket,
    const String & key);

void deleteObject(const Aws::S3::S3Client & client, const String & bucket, const String & key);

} // namespace DB::S3
