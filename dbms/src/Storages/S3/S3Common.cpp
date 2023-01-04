#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Storages/S3/S3Common.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <common/logger_useful.h>

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
const char * S3_LOGGER_TAG_NAMES[][2] = {
    {"AWSClient", "AWSClient"},
    {"AWSAuthV4Signer", "AWSClient (AWSAuthV4Signer)"},
};

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
    explicit AWSLogger(bool enable_s3_requests_logging_)
        : enable_s3_requests_logging(enable_s3_requests_logging_)
    {
        for (auto [tag, name] : S3_LOGGER_TAG_NAMES)
            tag_loggers[tag] = &Poco::Logger::get(name);

        default_logger = tag_loggers[S3_LOGGER_TAG_NAMES[0][0]];
    }

    ~AWSLogger() final = default;

    Aws::Utils::Logging::LogLevel GetLogLevel() const final
    {
        if (enable_s3_requests_logging)
            return Aws::Utils::Logging::LogLevel::Trace;
        else
            return Aws::Utils::Logging::LogLevel::Info;
    }

    void Log(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * format_str, ...) final // NOLINT
    {
        callLogImpl(log_level, tag, format_str); /// FIXME. Variadic arguments?
    }

    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char * tag, const Aws::OStringStream & message_stream) final
    {
        callLogImpl(log_level, tag, message_stream.str().c_str());
    }

    static void callLogImpl(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * message)
    {
        auto prio = convertLogLevel(log_level);
        auto log = DB::Logger::get(tag);
        LOG_IMPL(log, prio, "{}", message);
    }

    void Flush() final {}

private:
    Poco::Logger * default_logger;
    bool enable_s3_requests_logging;
    std::unordered_map<String, Poco::Logger *> tag_loggers;
};

} // namespace


namespace DB
{
namespace S3
{
void ClientFactory::init(bool enable_s3_log)
{
    Aws::InitAPI(aws_options);
    Aws::Utils::Logging::InitializeAWSLogging(std::make_shared<AWSLogger>(enable_s3_log));
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

bool isNotFoundError(Aws::S3::S3Errors error)
{
    return error == Aws::S3::S3Errors::RESOURCE_NOT_FOUND || error == Aws::S3::S3Errors::NO_SUCH_KEY;
}

Aws::S3::Model::HeadObjectOutcome headObject(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, [[maybe_unused]] bool for_disk_s3)
{
    ProfileEvents::increment(ProfileEvents::S3HeadObject);

    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);

    if (!version_id.empty())
        req.SetVersionId(version_id);

    return client.HeadObject(req);
}

S3::ObjectInfo getObjectInfo(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool throw_on_error, bool for_disk_s3)
{
    auto outcome = headObject(client, bucket, key, version_id, for_disk_s3);

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

size_t getObjectSize(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool throw_on_error, bool for_disk_s3)
{
    return getObjectInfo(client, bucket, key, version_id, throw_on_error, for_disk_s3).size;
}

bool objectExists(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool for_disk_s3)
{
    auto outcome = headObject(client, bucket, key, version_id, for_disk_s3);

    if (outcome.IsSuccess())
        return true;

    const auto & error = outcome.GetError();
    if (isNotFoundError(error.GetErrorType()))
        return false;

    throw Exception(ErrorCodes::S3_ERROR,
                    "Failed to check existence of key {} in bucket {}: {}",
                    key,
                    bucket,
                    error.GetMessage());
}

void uploadEmptyFile(const Aws::S3::S3Client & client, const String & bucket, const String & remote_fname)
{
    Stopwatch sw;
    Aws::S3::Model::PutObjectRequest req;
    req.WithBucket(bucket).WithKey(remote_fname);
    auto istr = Aws::MakeShared<Aws::StringStream>("EmptyObjectInputStream", "", std::ios_base::in | std::ios_base::binary);
    req.SetBody(istr);
    req.SetContentType("binary/octet-stream");
    auto result = client.PutObject(req);
    if (!result.IsSuccess())
    {
        throw Exception(ErrorCodes::S3_ERROR,
                        "S3 PutEmptyObject failed, remote_fname={}, exception={}, message={}",
                        remote_fname,
                        result.GetError().GetExceptionName(),
                        result.GetError().GetMessage());
    }
    static auto * log = &Poco::Logger::get("S3UploadFile");
    LOG_DEBUG(log, "remote_fname={}, cost={}ms", remote_fname, sw.elapsedMilliseconds());
}

void uploadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname)
{
    Stopwatch sw;
    Aws::S3::Model::PutObjectRequest req;
    req.WithBucket(bucket).WithKey(remote_fname);
    auto istr = Aws::MakeShared<Aws::FStream>("PutObjectInputStream", local_fname, std::ios_base::in | std::ios_base::binary);
    req.SetBody(istr);
    req.SetContentType("binary/octet-stream");
    auto result = client.PutObject(req);
    if (!result.IsSuccess())
    {
        throw Exception(ErrorCodes::S3_ERROR,
                        "S3 PutObject failed, local_fname={}, remote_fname={}, exception={}, message={}",
                        local_fname,
                        remote_fname,
                        result.GetError().GetExceptionName(),
                        result.GetError().GetMessage());
    }
    static auto * log = &Poco::Logger::get("S3UploadFile");
    LOG_DEBUG(log, "local_fname={}, remote_fname={}, cost={}ms", local_fname, remote_fname, sw.elapsedMilliseconds());
}

void downloadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname)
{
    Stopwatch sw;
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(remote_fname);
    auto result = client.GetObject(req);
    if (!result.IsSuccess())
    {
        throw Exception(ErrorCodes::S3_ERROR,
                        "S3 GetObject failed, local_fname={}, remote_fname={}, exception={}, message={}",
                        local_fname,
                        remote_fname,
                        result.GetError().GetExceptionName(),
                        result.GetError().GetMessage());
    }
    Aws::OFStream ostr(local_fname, std::ios_base::out | std::ios_base::binary);
    ostr << result.GetResult().GetBody().rdbuf();
    static auto * log = &Poco::Logger::get("S3DownloadFile");
    LOG_DEBUG(log, "local_fname={}, remote_fname={}, cost={}ms", local_fname, remote_fname, sw.elapsedMilliseconds());
}

void listPrefix(
    const Aws::S3::S3Client & client,
    const String & bucket,
    const String & prefix,
    const String & delimiter,
    std::function<size_t(const Aws::S3::Model::ListObjectsV2Result & result)> pager)
{
    Stopwatch sw;
    Aws::S3::Model::ListObjectsV2Request req;
    req.WithBucket(bucket).WithPrefix(prefix);
    if (!delimiter.empty())
    {
        req.SetDelimiter(delimiter);
    }

    static auto log = Logger::get("S3ListPrefix");

    bool done = false;
    size_t num_keys = 0;
    while (!done)
    {
        auto outcome = client.ListObjectsV2(req);
        if (!outcome.IsSuccess())
        {
            throw Exception(ErrorCodes::S3_ERROR,
                            "S3 ListObjects failed, prefix={}, exception={}, message={}",
                            prefix,
                            outcome.GetError().GetExceptionName(),
                            outcome.GetError().GetMessage());
        }

        const auto & result = outcome.GetResult();
        size_t page_nkeys = pager(result);
        num_keys += page_nkeys;

        // handle the result size over max size
        done = !result.GetIsTruncated();
        if (!done)
        {
            const auto & next_token = result.GetNextContinuationToken();
            req.SetContinuationToken(next_token);
            LOG_DEBUG(log, "prefix={}, keys={}, total_keys={}, next_token={}", prefix, page_nkeys, num_keys, next_token);
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
        return objects.size();
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
        throw Exception(ErrorCodes::S3_ERROR, "Failed to check existence. bucket={} key={}", bucket, key);
    }
    // Else the object still exist
    const auto & res = o.GetResult();
    // "DeleteMark" of S3 service, don't know what will lead to this
    RUNTIME_CHECK(!res.GetDeleteMarker(), bucket, key);
    return {true, res.GetLastModified()};
}

void deleteObject(const Aws::S3::S3Client & client, const String & bucket, const String & key)
{
    Aws::S3::Model::DeleteObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);
    auto o = client.DeleteObject(req);
    RUNTIME_CHECK(o.IsSuccess(), o.GetError().GetMessage());
    const auto & res = o.GetResult();
    // "DeleteMark" of S3 service, don't know what will lead to this
    RUNTIME_CHECK(!res.GetDeleteMarker(), bucket, key);
}

} // namespace S3

} // namespace DB
