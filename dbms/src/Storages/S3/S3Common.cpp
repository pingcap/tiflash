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

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/S3/MockS3Client.h>
#include <Storages/S3/S3Common.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/predicate.hpp>
#include <fstream>
#include <memory>

namespace ProfileEvents
{
extern const Event S3HeadObject;
extern const Event S3GetObject;
extern const Event S3ReadBytes;
extern const Event S3PutObject;
extern const Event S3WriteBytes;
extern const Event S3ListObjects;
extern const Event S3DeleteObject;
} // namespace ProfileEvents

namespace
{
Poco::Message::Priority convertLogLevel(Aws::Utils::Logging::LogLevel log_level)
{
    switch (log_level)
    {
    case Aws::Utils::Logging::LogLevel::Off:
    case Aws::Utils::Logging::LogLevel::Fatal:
    case Aws::Utils::Logging::LogLevel::Error:
        return Poco::Message::PRIO_ERROR;
    case Aws::Utils::Logging::LogLevel::Warn:
        return Poco::Message::PRIO_WARNING;
    case Aws::Utils::Logging::LogLevel::Info:
        return Poco::Message::PRIO_INFORMATION;
    case Aws::Utils::Logging::LogLevel::Debug:
        return Poco::Message::PRIO_DEBUG;
    case Aws::Utils::Logging::LogLevel::Trace:
        return Poco::Message::PRIO_TRACE;
    default:
        return Poco::Message::PRIO_INFORMATION;
    }
}

class AWSLogger final : public Aws::Utils::Logging::LogSystemInterface
{
public:
    AWSLogger()
        : default_logger(&Poco::Logger::get("AWSClient"))
    {}

    ~AWSLogger() final = default;

    Aws::Utils::Logging::LogLevel GetLogLevel() const final
    {
        return Aws::Utils::Logging::LogLevel::Info;
    }

    void Log(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * format_str, ...) final // NOLINT
    {
        callLogImpl(log_level, tag, format_str);
    }

    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char * tag, const Aws::OStringStream & message_stream) final
    {
        callLogImpl(log_level, tag, message_stream.str().c_str());
    }

    void callLogImpl(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * message)
    {
        auto prio = convertLogLevel(log_level);
        LOG_IMPL(default_logger, prio, "tag={} message={}", tag, message);
    }

    void Flush() final {}

private:
    Poco::Logger * default_logger;
};

} // namespace


namespace DB::S3
{

TiFlashS3Client::TiFlashS3Client(const String & bucket_name_)
    : bucket_name(bucket_name_)
{}

TiFlashS3Client::TiFlashS3Client(
    const String & bucket_name_,
    const Aws::Auth::AWSCredentials & credentials,
    const Aws::Client::ClientConfiguration & clientConfiguration,
    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads,
    bool useVirtualAddressing)
    : Aws::S3::S3Client(credentials, clientConfiguration, signPayloads, useVirtualAddressing)
    , bucket_name(bucket_name_)
{
}

TiFlashS3Client::TiFlashS3Client(const String & bucket_name_, std::unique_ptr<Aws::S3::S3Client> && raw_client)
    : Aws::S3::S3Client(std::move(*raw_client))
    , bucket_name(bucket_name_)
{
}

bool ClientFactory::isEnabled() const
{
    return config.isS3Enabled();
}

void ClientFactory::init(const StorageS3Config & config_, bool mock_s3_)
{
    config = config_;
    Aws::InitAPI(aws_options);
    Aws::Utils::Logging::InitializeAWSLogging(std::make_shared<AWSLogger>());
    if (!mock_s3_)
    {
        shared_client = create();
        shared_tiflash_client = std::make_shared<TiFlashS3Client>(config.bucket, create());
    }
    else
    {
        shared_client = std::make_unique<tests::MockS3Client>();
        shared_tiflash_client = std::make_unique<tests::MockS3Client>(config.bucket);
    }
}

void ClientFactory::shutdown()
{
    shared_tiflash_client.reset();
    shared_client.reset(); // Reset S3Client before Aws::ShutdownAPI.
    Aws::Utils::Logging::ShutdownAWSLogging();
    Aws::ShutdownAPI(aws_options);
}

ClientFactory::~ClientFactory() = default;

ClientFactory & ClientFactory::instance()
{
    static ClientFactory ret;
    return ret;
}

std::unique_ptr<Aws::S3::S3Client> ClientFactory::create() const
{
    return create(config);
}

const String & ClientFactory::bucket() const
{
    // `bucket` is read-only.
    return config.bucket;
}

std::shared_ptr<Aws::S3::S3Client> ClientFactory::sharedClient() const
{
    // `shared_client` is created during initialization and destroyed when process exits
    // which means it is read-only when processing requests. So, it is safe to read `shared_client`
    // without acquiring lock.
    return shared_client;
}

std::shared_ptr<TiFlashS3Client> ClientFactory::sharedTiFlashClient() const
{
    return shared_tiflash_client;
}

std::unique_ptr<Aws::S3::S3Client> ClientFactory::create(const StorageS3Config & config_)
{
    Aws::Client::ClientConfiguration cfg;
    cfg.maxConnections = config_.max_connections;
    cfg.requestTimeoutMs = config_.request_timeout_ms;
    cfg.connectTimeoutMs = config_.connection_timeout_ms;
    if (!config_.endpoint.empty())
    {
        cfg.endpointOverride = config_.endpoint;
        auto scheme = parseScheme(config_.endpoint);
        cfg.scheme = scheme;
        cfg.verifySSL = scheme == Aws::Http::Scheme::HTTPS;
    }
    if (config_.access_key_id.empty() && config_.secret_access_key.empty())
    {
        // Request that does not require authentication.
        // Such as the EC2 access permission to the S3 bucket is configured.
        // If the empty access_key_id and secret_access_key are passed to S3Client,
        // an authentication error will be reported.
        return std::make_unique<Aws::S3::S3Client>(cfg);
    }
    else
    {
        Aws::Auth::AWSCredentials cred(config_.access_key_id, config_.secret_access_key);
        return std::make_unique<Aws::S3::S3Client>(
            cred,
            cfg,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            /*useVirtualAddressing*/ true);
    }
}

Aws::Http::Scheme ClientFactory::parseScheme(std::string_view endpoint)
{
    return boost::algorithm::starts_with(endpoint, "https://") ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
}

bool isNotFoundError(Aws::S3::S3Errors error)
{
    return error == Aws::S3::S3Errors::RESOURCE_NOT_FOUND || error == Aws::S3::S3Errors::NO_SUCH_KEY;
}

Aws::S3::Model::HeadObjectOutcome headObject(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id)
{
    ProfileEvents::increment(ProfileEvents::S3HeadObject);
    Stopwatch sw;
    SCOPE_EXIT({ GET_METRIC(tiflash_storage_s3_request_seconds, type_head_object).Observe(sw.elapsedSeconds()); });
    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    if (!version_id.empty())
    {
        req.SetVersionId(version_id);
    }
    return client.HeadObject(req);
}

S3::ObjectInfo getObjectInfo(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool throw_on_error)
{
    auto outcome = headObject(client, bucket, key, version_id);

    if (outcome.IsSuccess())
    {
        auto read_result = outcome.GetResultWithOwnership();
        return {.size = static_cast<size_t>(read_result.GetContentLength()), .last_modification_time = read_result.GetLastModified().Millis() / 1000};
    }
    else if (throw_on_error)
    {
        throw fromS3Error(outcome.GetError(), "Failed to HEAD object, key={}", key);
    }
    return {};
}

size_t getObjectSize(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool throw_on_error)
{
    return getObjectInfo(client, bucket, key, version_id, throw_on_error).size;
}

bool objectExists(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id)
{
    auto outcome = headObject(client, bucket, key, version_id);
    if (outcome.IsSuccess())
    {
        return true;
    }
    const auto & error = outcome.GetError();
    if (isNotFoundError(error.GetErrorType()))
    {
        return false;
    }
    throw fromS3Error(outcome.GetError(), "Failed to check existence of object, bucket={} key={}", bucket, key);
}

void uploadEmptyFile(const Aws::S3::S3Client & client, const String & bucket, const String & key)
{
    Stopwatch sw;
    Aws::S3::Model::PutObjectRequest req;
    req.WithBucket(bucket).WithKey(key);
    req.SetContentType("binary/octet-stream");
    auto istr = Aws::MakeShared<Aws::StringStream>("EmptyObjectInputStream", "", std::ios_base::in | std::ios_base::binary);
    req.SetBody(istr);
    ProfileEvents::increment(ProfileEvents::S3PutObject);
    auto result = client.PutObject(req);
    if (!result.IsSuccess())
    {
        throw fromS3Error(result.GetError(), "S3 PutEmptyObject failed, bucket={} key={}", bucket, key);
    }
    auto elapsed_seconds = sw.elapsedSeconds();
    GET_METRIC(tiflash_storage_s3_request_seconds, type_put_object).Observe(elapsed_seconds);
    static auto log = Logger::get();
    LOG_DEBUG(log, "remote_fname={}, cost={}ms", key, elapsed_seconds);
}

void uploadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname)
{
    Stopwatch sw;
    Aws::S3::Model::PutObjectRequest req;
    req.WithBucket(bucket).WithKey(remote_fname);
    req.SetContentType("binary/octet-stream");
    auto istr = Aws::MakeShared<Aws::FStream>("PutObjectInputStream", local_fname, std::ios_base::in | std::ios_base::binary);
    RUNTIME_CHECK_MSG(istr->is_open(), "Open {} fail: {}", local_fname, strerror(errno));
    req.SetBody(istr);
    ProfileEvents::increment(ProfileEvents::S3PutObject);
    auto result = client.PutObject(req);
    if (!result.IsSuccess())
    {
        throw fromS3Error(result.GetError(), "S3 PutObject failed, local_fname={} bucket={} key={}", local_fname, bucket, remote_fname);
    }
    auto write_bytes = istr->tellg();
    ProfileEvents::increment(ProfileEvents::S3WriteBytes, write_bytes);
    auto elapsed_seconds = sw.elapsedSeconds();
    GET_METRIC(tiflash_storage_s3_request_seconds, type_put_object).Observe(elapsed_seconds);
    static auto log = Logger::get();
    LOG_DEBUG(log, "local_fname={}, remote_fname={}, write_bytes={} cost={}ms", local_fname, remote_fname, write_bytes, elapsed_seconds);
}

void downloadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname)
{
    Stopwatch sw;
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(remote_fname);
    ProfileEvents::increment(ProfileEvents::S3GetObject);
    auto outcome = client.GetObject(req);
    if (!outcome.IsSuccess())
    {
        throw fromS3Error(outcome.GetError(), "remote_fname={}", remote_fname);
    }
    ProfileEvents::increment(ProfileEvents::S3ReadBytes, outcome.GetResult().GetContentLength());
    GET_METRIC(tiflash_storage_s3_request_seconds, type_get_object).Observe(sw.elapsedSeconds());
    Aws::OFStream ostr(local_fname, std::ios_base::out | std::ios_base::binary);
    RUNTIME_CHECK_MSG(ostr.is_open(), "Open {} fail: {}", local_fname, strerror(errno));
    ostr << outcome.GetResult().GetBody().rdbuf();
    RUNTIME_CHECK_MSG(ostr.good(), "Write {} fail: {}", local_fname, strerror(errno));
}

void listPrefix(
    const Aws::S3::S3Client & client,
    const String & bucket,
    const String & prefix,
    std::function<PageResult(const Aws::S3::Model::ListObjectsV2Result & result)> pager)
{
    // Usually we don't need to set delimiter.
    // Check the docs here for Delimiter && CommonPrefixes when you really need it.
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
    listPrefix(client, bucket, prefix, /*delimiter*/ "", pager);
}

void listPrefix(
    const Aws::S3::S3Client & client,
    const String & bucket,
    const String & prefix,
    std::string_view delimiter,
    std::function<PageResult(const Aws::S3::Model::ListObjectsV2Result & result)> pager)
{
    Stopwatch sw;
    Aws::S3::Model::ListObjectsV2Request req;
    req.WithBucket(bucket).WithPrefix(prefix);
    if (!delimiter.empty())
    {
        req.SetDelimiter(String(delimiter));
    }

    static auto log = Logger::get("S3ListPrefix");

    bool done = false;
    size_t num_keys = 0;
    while (!done)
    {
        Stopwatch sw_list;
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        auto outcome = client.ListObjectsV2(req);
        if (!outcome.IsSuccess())
        {
            throw fromS3Error(outcome.GetError(), "S3 ListObjects failed, bucket={} prefix={} delimiter={}", bucket, prefix, delimiter);
        }
        GET_METRIC(tiflash_storage_s3_request_seconds, type_list_objects).Observe(sw_list.elapsedSeconds());

        const auto & result = outcome.GetResult();
        PageResult page_res = pager(result);
        num_keys += page_res.num_keys;

        // handle the result size over max size
        done = !result.GetIsTruncated();
        if (!done && page_res.more)
        {
            const auto & next_token = result.GetNextContinuationToken();
            req.SetContinuationToken(next_token);
            LOG_DEBUG(log, "prefix={}, keys={}, total_keys={}, next_token={}", prefix, page_res.num_keys, num_keys, next_token);
        }
    }
    LOG_DEBUG(log, "prefix={}, total_keys={}, cost={}", prefix, num_keys, sw.elapsedMilliseconds());
}

std::unordered_map<String, size_t> listPrefixWithSize(const Aws::S3::S3Client & client, const String & bucket, const String & prefix)
{
    std::unordered_map<String, size_t> keys_with_size;
    listPrefix(client, bucket, prefix, "", [&](const Aws::S3::Model::ListObjectsV2Result & result) {
        const auto & objects = result.GetContents();
        keys_with_size.reserve(keys_with_size.size() + objects.size());
        for (const auto & object : objects)
        {
            keys_with_size.emplace(object.GetKey().substr(prefix.size()), object.GetSize()); // Cut prefix
        }
        return PageResult{.num_keys = objects.size(), .more = true};
    });

    return keys_with_size;
}

std::pair<bool, Aws::Utils::DateTime> tryGetObjectModifiedTime(
    const Aws::S3::S3Client & client,
    const String & bucket,
    const String & key)
{
    auto o = headObject(client, bucket, key);
    if (!o.IsSuccess())
    {
        if (const auto & err = o.GetError(); isNotFoundError(err.GetErrorType()))
        {
            return {false, {}};
        }
        throw fromS3Error(o.GetError(), "Failed to check existence of object, bucket={} key={}", bucket, key);
    }
    // Else the object still exist
    const auto & res = o.GetResult();
    // "DeleteMark" of S3 service, don't know what will lead to this
    RUNTIME_CHECK(!res.GetDeleteMarker(), bucket, key);
    return {true, res.GetLastModified()};
}

void deleteObject(const Aws::S3::S3Client & client, const String & bucket, const String & key)
{
    Stopwatch sw;
    Aws::S3::Model::DeleteObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    ProfileEvents::increment(ProfileEvents::S3DeleteObject);
    auto o = client.DeleteObject(req);
    RUNTIME_CHECK(o.IsSuccess(), o.GetError().GetMessage());
    const auto & res = o.GetResult();
    // "DeleteMark" of S3 service, don't know what will lead to this
    RUNTIME_CHECK(!res.GetDeleteMarker(), bucket, key);
    GET_METRIC(tiflash_storage_s3_request_seconds, type_delete_object).Observe(sw.elapsedSeconds());
}

} // namespace DB::S3
