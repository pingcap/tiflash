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

#pragma once

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/nocopyable.h>
#include <IO/FileProvider/EncryptionPath.h>
#include <IO/FileProvider/FileProvider_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Server/StorageConfigParser.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <aws/core/Aws.h>
#include <aws/core/http/Scheme.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <common/types.h>

#include <magic_enum.hpp>

namespace pingcap::kv
{
struct Cluster;
}

namespace DB::ErrorCodes
{
extern const int S3_ERROR;
}

namespace DB::S3
{

inline String S3ErrorMessage(const Aws::S3::S3Error & e)
{
    return fmt::format(
        " s3error={} s3exception_name={} s3msg={} request_id={}",
        magic_enum::enum_name(e.GetErrorType()),
        e.GetExceptionName(),
        e.GetMessage(),
        e.GetRequestId());
}

template <typename... Args>
Exception fromS3Error(const Aws::S3::S3Error & e, const std::string & fmt, Args &&... args)
{
    return DB::Exception(ErrorCodes::S3_ERROR, fmt + S3ErrorMessage(e), args...);
}

class TiFlashS3Client : public Aws::S3::S3Client
{
public:
    // Usually one tiflash instance only need access one bucket.
    // Store the bucket name to simpilfy some param passing.

    TiFlashS3Client(const String & bucket_name_, const String & root_);

    TiFlashS3Client(
        const String & bucket_name_,
        const String & root_,
        const Aws::Auth::AWSCredentials & credentials,
        const Aws::Client::ClientConfiguration & clientConfiguration,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads,
        bool useVirtualAddressing);

    TiFlashS3Client(
        const String & bucket_name_,
        const String & root_,
        std::unique_ptr<Aws::S3::S3Client> && raw_client);

    const String & bucket() const { return bucket_name; }

    const String & root() const { return key_root; }

    template <typename Request>
    void setBucketAndKeyWithRoot(Request & req, const String & key) const
    {
        bool is_root_single_slash = key_root == "/";
        // If the `root == '/'`, don't prepend the root to the prefix, otherwise S3 list doesn't work.
        req.WithBucket(bucket_name).WithKey(is_root_single_slash ? key : key_root + key);
    }

private:
    const String bucket_name;
    String key_root;

public:
    LoggerPtr log;
};

enum class S3GCMethod
{
    Lifecycle = 1,
    ScanThenDelete,
};

class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    bool isEnabled() const;

    void disable() { config.disable(); }
    void enable() { config.enable(false, log); }

    void init(const StorageS3Config & config_, bool mock_s3_ = false);

    void setKVCluster(pingcap::kv::Cluster * kv_cluster_)
    {
        std::unique_lock lock_init(mtx_init);
        kv_cluster = kv_cluster_;
    }

    void shutdown();

    StorageS3Config getConfigCopy() const
    {
        std::unique_lock lock_init(mtx_init);
        return config;
    }

    std::shared_ptr<TiFlashS3Client> sharedTiFlashClient();

    S3GCMethod gc_method = S3GCMethod::Lifecycle;

private:
    ClientFactory() = default;
    DISALLOW_COPY_AND_MOVE(ClientFactory);
    std::unique_ptr<Aws::S3::S3Client> create() const;

    static std::unique_ptr<Aws::S3::S3Client> create(const StorageS3Config & config_, const LoggerPtr & log);

    std::shared_ptr<TiFlashS3Client> initClientFromWriteNode();

private:
    Aws::SDKOptions aws_options;

    std::atomic_bool client_is_inited = false;
    mutable std::mutex mtx_init; // protect `config` `shared_tiflash_client` `kv_cluster`
    StorageS3Config config;
    std::shared_ptr<TiFlashS3Client> shared_tiflash_client;
    pingcap::kv::Cluster * kv_cluster = nullptr;

    LoggerPtr log;
};

bool isNotFoundError(Aws::S3::S3Errors error);

Aws::S3::Model::HeadObjectOutcome headObject(const TiFlashS3Client & client, const String & key);

bool objectExists(const TiFlashS3Client & client, const String & key);

void uploadFile(
    const TiFlashS3Client & client,
    const String & local_fname,
    const String & remote_fname,
    const EncryptionPath & encryption_path,
    const FileProviderPtr & file_provider,
    int max_retry_times = 3);

constexpr std::string_view TaggingObjectIsDeleted = "tiflash_deleted=true";
bool ensureLifecycleRuleExist(const TiFlashS3Client & client, Int32 expire_days);

/**
 * tagging is the tag-set for the object. The tag-set must be encoded as URL Query
 * parameters. (For example, "Key1=Value1")
 */
void uploadEmptyFile(
    const TiFlashS3Client & client,
    const String & key,
    const String & tagging = "",
    int max_retry_times = 3);

void downloadFile(const TiFlashS3Client & client, const String & local_fname, const String & remote_fname);
void downloadFileByS3RandomAccessFile(
    std::shared_ptr<TiFlashS3Client> client,
    const String & local_fname,
    const String & remote_fname);

void rewriteObjectWithTagging(const TiFlashS3Client & client, const String & key, const String & tagging);

struct PageResult
{
    size_t num_keys;
    // true - continue to call next `LIST` when available
    // false - stop `LIST`
    bool more;
};
void listPrefix(
    const TiFlashS3Client & client,
    const String & prefix,
    std::function<PageResult(const Aws::S3::Model::Object & object)> pager);
void listPrefixWithDelimiter(
    const TiFlashS3Client & client,
    const String & prefix,
    std::string_view delimiter,
    std::function<PageResult(const Aws::S3::Model::CommonPrefix & common_prefix)> pager);

std::optional<String> anyKeyExistWithPrefix(const TiFlashS3Client & client, const String & prefix);

std::unordered_map<String, size_t> listPrefixWithSize(const TiFlashS3Client & client, const String & prefix);


struct ObjectInfo
{
    bool exist = false;
    Int64 size = 0;
    Aws::Utils::DateTime last_modification_time;
};
ObjectInfo tryGetObjectInfo(const TiFlashS3Client & client, const String & key);

void deleteObject(const TiFlashS3Client & client, const String & key);

// Unlike `listPrefix` or other methods above, this does not handle
// the TiFlashS3Client `root`.
void rawListPrefix(
    const Aws::S3::S3Client & client,
    const String & bucket,
    const String & prefix,
    std::string_view delimiter,
    std::function<PageResult(const Aws::S3::Model::ListObjectsV2Result & result)> pager);

// Unlike `deleteObject` or other method above, this does not handle
// the TiFlashS3Client `root`.
void rawDeleteObject(const Aws::S3::S3Client & client, const String & bucket, const String & key);

template <typename F, typename... T>
void retryWrapper(F f, const T &... args)
{
    Int32 i = 0;
    while (true)
    {
        // When `f` return true or throw exception, break the loop.
        if (f(args..., i))
        {
            break;
        }
        ++i;
    }
}
} // namespace DB::S3
