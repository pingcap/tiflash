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

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Storages/S3/S3Common.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/predicate.hpp>
#include <fstream>

namespace ProfileEvents
{
extern const Event S3HeadObject;
}

namespace DB::ErrorCodes
{
extern const int S3_ERROR;
}

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


namespace DB
{
namespace S3
{
void ClientFactory::init(const StorageS3Config & config_)
{
    config = config_;
    Aws::InitAPI(aws_options);
    Aws::Utils::Logging::InitializeAWSLogging(std::make_shared<AWSLogger>());
}

void ClientFactory::shutdown()
{
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
    auto schema = parseScheme(config.endpoint);
    return create(
        config.endpoint,
        schema,
        schema == Aws::Http::Scheme::HTTPS,
        config.access_key_id,
        config.secret_access_key);
}

std::unique_ptr<Aws::S3::S3Client> ClientFactory::create(
    const String & endpoint,
    Aws::Http::Scheme scheme,
    bool verifySSL,
    const String & access_key_id,
    const String & secret_access_key)
{
    Aws::Client::ClientConfiguration cfg;
    cfg.endpointOverride = endpoint;
    cfg.scheme = scheme;
    cfg.verifySSL = verifySSL;
    Aws::Auth::AWSCredentials cred(access_key_id, secret_access_key);
    return std::make_unique<Aws::S3::S3Client>(
        cred,
        cfg,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        /*useVirtualAddressing*/ true);
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
        const auto & error = outcome.GetError();
        throw DB::Exception(ErrorCodes::S3_ERROR,
                            "Failed to HEAD object: {}. HTTP response code: {}",
                            error.GetMessage(),
                            static_cast<size_t>(error.GetResponseCode()));
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
    throw Exception(ErrorCodes::S3_ERROR,
                    "Failed to check existence of key {} in bucket {}: {}",
                    key,
                    bucket,
                    error.GetMessage());
}

void uploadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname)
{
    Stopwatch sw;
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(remote_fname);
    auto istr = Aws::MakeShared<Aws::FStream>("PutObjectInputStream", local_fname, std::ios_base::in | std::ios_base::binary);
    req.SetBody(istr);
    req.SetContentType("binary/octet-stream");
    auto result = client.PutObject(req);
    RUNTIME_CHECK_MSG(result.IsSuccess(),
                      "S3 PutObject failed, local_fname={}, remote_fname={}, exception={}, message={}",
                      local_fname,
                      remote_fname,
                      result.GetError().GetExceptionName(),
                      result.GetError().GetMessage());
    static auto * log = &Poco::Logger::get("S3PutObject");
    LOG_DEBUG(log, "local_fname={}, remote_fname={}, cost={}ms", local_fname, remote_fname, sw.elapsedMilliseconds());
}

void downloadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname)
{
    Stopwatch sw;
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(remote_fname);
    auto result = client.GetObject(req);
    RUNTIME_CHECK_MSG(result.IsSuccess(),
                      "S3 GetObject failed, local_fname={}, remote_fname={}, exception={}, message={}",
                      local_fname,
                      remote_fname,
                      result.GetError().GetExceptionName(),
                      result.GetError().GetMessage());
    Aws::OFStream ostr(local_fname, std::ios_base::out | std::ios_base::binary);
    ostr << result.GetResult().GetBody().rdbuf();
    static auto * log = &Poco::Logger::get("S3GetObject");
    LOG_DEBUG(log, "local_fname={}, remote_fname={}, cost={}ms", local_fname, remote_fname, sw.elapsedMilliseconds());
}

std::unordered_map<String, size_t> listPrefix(const Aws::S3::S3Client & client, const String & bucket, const String & prefix)
{
    Stopwatch sw;
    Aws::S3::Model::ListObjectsRequest req;
    req.SetBucket(bucket);
    req.SetPrefix(prefix);
    auto result = client.ListObjects(req);
    RUNTIME_CHECK_MSG(result.IsSuccess(),
                      "S3 ListObjects failed, prefix={}, exception={}, message={}",
                      prefix,
                      result.GetError().GetExceptionName(),
                      result.GetError().GetMessage());
    const auto & objects = result.GetResult().GetContents();
    std::unordered_map<String, size_t> keys_with_size;
    keys_with_size.reserve(objects.size());
    for (const auto & object : objects)
    {
        keys_with_size.emplace(object.GetKey().substr(prefix.size()), object.GetSize()); // Cut prefix
    }
    static auto * log = &Poco::Logger::get("S3ListObjects");
    LOG_DEBUG(log, "prefix={}, keys={}, cost={}", prefix, keys_with_size, sw.elapsedMilliseconds());
    return keys_with_size;
}

} // namespace S3

} // namespace DB
