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
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/RemoteHostFilter.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashMetrics.h>
#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <IO/Buffer/ReadBufferFromRandomAccessFile.h>
#include <IO/Buffer/StdStreamFromReadBuffer.h>
#include <IO/Buffer/WriteBufferFromWritableFile.h>
#include <IO/FileProvider/FileProvider.h>
#include <Interpreters/Context_fwd.h>
#include <Server/StorageConfigParser.h>
#include <Storages/S3/Credentials.h>
#include <Storages/S3/MockS3Client.h>
#include <Storages/S3/PocoHTTPClient.h>
#include <Storages/S3/PocoHTTPClientFactory.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/ExpirationStatus.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/LifecycleConfiguration.h>
#include <aws/s3/model/LifecycleExpiration.h>
#include <aws/s3/model/LifecycleRule.h>
#include <aws/s3/model/LifecycleRuleAndOperator.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/MetadataDirective.h>
#include <aws/s3/model/PutBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/TaggingDirective.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/STSServiceClientModel.h>
#include <aws/sts/model/GetCallerIdentityRequest.h>
#include <aws/sts/model/GetCallerIdentityResult.h>
#include <common/logger_useful.h>
#include <kvproto/disaggregated.pb.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionCache.h>
#include <pingcap/kv/Rpc.h>
#include <pingcap/kv/internal/type_traits.h>
#include <re2/re2.h>

#include <any>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <filesystem>
#include <ios>
#include <magic_enum.hpp>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>


namespace ProfileEvents
{
extern const Event S3HeadObject;
extern const Event S3GetObject;
extern const Event S3ReadBytes;
extern const Event S3PutObject;
extern const Event S3WriteBytes;
extern const Event S3ListObjects;
extern const Event S3DeleteObject;
extern const Event S3CopyObject;
extern const Event S3PutObjectRetry;
extern const Event S3PutDMFile;
extern const Event S3PutDMFileRetry;
extern const Event S3WriteDMFileBytes;
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
        // treat AWS debug log as trace level
        return Poco::Message::PRIO_TRACE;
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

    Aws::Utils::Logging::LogLevel GetLogLevel() const final { return Aws::Utils::Logging::LogLevel::Debug; }

    void Log(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * format_str, ...) final // NOLINT
    {
        callLogImpl(log_level, tag, format_str);
    }

    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char * tag, const Aws::OStringStream & message_stream)
        final
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

namespace DB::FailPoints
{
extern const char force_set_mocked_s3_object_mtime[];
} // namespace DB::FailPoints
namespace DB::S3
{

// ensure the `key_root` format like "user0/". No '/' at the beginning and '/' at the end
String normalizedRoot(String ori_root) // a copy for changing
{
    if (startsWith(ori_root, "/") && ori_root.size() != 1)
    {
        ori_root = ori_root.substr(1, ori_root.size());
    }
    if (!endsWith(ori_root, "/"))
    {
        ori_root += "/";
    }
    return ori_root;
}

TiFlashS3Client::TiFlashS3Client(const String & bucket_name_, const String & root_)
    : bucket_name(bucket_name_)
    , key_root(normalizedRoot(root_))
    , log(Logger::get(fmt::format("bucket={} root={}", bucket_name, key_root)))
{}

TiFlashS3Client::TiFlashS3Client(
    const String & bucket_name_,
    const String & root_,
    const Aws::Auth::AWSCredentials & credentials,
    const Aws::Client::ClientConfiguration & clientConfiguration,
    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads,
    bool useVirtualAddressing)
    : Aws::S3::S3Client(credentials, clientConfiguration, signPayloads, useVirtualAddressing)
    , bucket_name(bucket_name_)
    , key_root(normalizedRoot(root_))
    , log(Logger::get(fmt::format("bucket={} root={}", bucket_name, key_root)))
{}

TiFlashS3Client::TiFlashS3Client(
    const String & bucket_name_,
    const String & root_,
    std::unique_ptr<Aws::S3::S3Client> && raw_client)
    : Aws::S3::S3Client(std::move(*raw_client))
    , bucket_name(bucket_name_)
    , key_root(normalizedRoot(root_))
    , log(Logger::get(fmt::format("bucket={} root={}", bucket_name, key_root)))
{}

bool ClientFactory::isEnabled() const
{
    return config.isS3Enabled();
}

disaggregated::GetDisaggConfigResponse getDisaggConfigFromDisaggWriteNodes(
    pingcap::kv::Cluster * kv_cluster,
    const LoggerPtr & log)
{
    using namespace std::chrono_literals;

    // TODO: Move it into client-c and use Backoff
    // try until success
    while (true)
    {
        auto stores = kv_cluster->pd_client->getAllStores(/*exclude_tombstone*/ true);
        for (const auto & store : stores)
        {
            std::map<String, String> labels;
            for (const auto & label : store.labels())
            {
                labels[label.key()] = label.value();
            }
            const auto & send_address = store.address();
            if (!pingcap::kv::labelFilterOnlyTiFlashWriteNode(labels))
            {
                LOG_INFO(
                    log,
                    "get disagg config ignore store by label, store_id={} address={}",
                    store.id(),
                    send_address);
                continue;
            }

            try
            {
                pingcap::kv::RpcCall<pingcap::kv::RPC_NAME(GetDisaggConfig)> rpc(kv_cluster->rpc_client, send_address);

                grpc::ClientContext client_context;
                rpc.setClientContext(client_context, /*timeout=*/2);
                disaggregated::GetDisaggConfigRequest req;
                disaggregated::GetDisaggConfigResponse resp;
                auto status = rpc.call(&client_context, req, &resp);
                if (!status.ok())
                    throw Exception(rpc.errMsg(status));

                RUNTIME_CHECK(resp.has_s3_config(), resp.ShortDebugString());

                if (resp.s3_config().endpoint().empty() || resp.s3_config().bucket().empty()
                    || resp.s3_config().root().empty())
                {
                    LOG_WARNING(
                        log,
                        "invalid settings, store_id={} address={} resp={}",
                        store.id(),
                        send_address,
                        resp.ShortDebugString());
                    continue;
                }

                LOG_INFO(
                    log,
                    "get disagg config from write node, store_id={} address={} resp={}",
                    store.id(),
                    send_address,
                    resp.ShortDebugString());
                return resp;
            }
            catch (...)
            {
                tryLogCurrentException(
                    log,
                    fmt::format("failed to get disagg config, store_id={} address={}", store.id(), send_address));
            }
        }
        LOG_WARNING(log, "failed to get disagg config from all tiflash stores, retry");
        std::this_thread::sleep_for(2s);
    }
}

void ClientFactory::init(const StorageS3Config & config_, bool mock_s3_)
{
    log = Logger::get();
    LOG_DEBUG(log, "Aws::InitAPI start");
    if (!config_.enable_poco_client)
    {
        LOG_DEBUG(log, "Using default curl client");
    }
    else
    {
        // Override the HTTP client, use PocoHTTPClient instead
        aws_options.httpOptions.httpClientFactory_create_fn = [&config_] {
            // TODO: do we need the remote host filter?
            PocoHTTPClientConfiguration poco_cfg(
                std::make_shared<RemoteHostFilter>(),
                config_.max_redirections,
                /*enable_s3_requests_logging_*/ config_.verbose,
                config_.enable_http_pool);
            return std::make_shared<PocoHTTPClientFactory>(poco_cfg);
        };
    }
    Aws::InitAPI(aws_options);
    Aws::Utils::Logging::InitializeAWSLogging(std::make_shared<AWSLogger>());

    std::unique_lock lock_init(mtx_init);
    if (client_is_inited) // another thread has done init
        return;

    config = config_;
    RUNTIME_CHECK(!config.root.starts_with("//"), config.root);
    config.root = normalizedRoot(config.root);

    if (config.bucket.empty())
    {
        LOG_INFO(log, "bucket is not specified, S3 client will be inited later");
        return;
    }

    if (!mock_s3_)
    {
        LOG_DEBUG(log, "Create TiFlashS3Client start");
        shared_tiflash_client = std::make_shared<TiFlashS3Client>(config.bucket, config.root, create());
        LOG_DEBUG(log, "Create TiFlashS3Client end");
    }
    else
    {
        shared_tiflash_client = std::make_unique<tests::MockS3Client>(config.bucket, config.root);
    }
    client_is_inited = true; // init finish
}

std::shared_ptr<TiFlashS3Client> ClientFactory::initClientFromWriteNode()
{
    std::unique_lock lock_init(mtx_init);
    if (client_is_inited) // another thread has done init
        return shared_tiflash_client;

    using namespace std::chrono_literals;
    while (kv_cluster == nullptr)
    {
        lock_init.unlock();
        LOG_INFO(log, "waiting for kv_cluster init");
        std::this_thread::sleep_for(1s);
        lock_init.lock();
    }
    assert(kv_cluster != nullptr);

    const auto disagg_config = getDisaggConfigFromDisaggWriteNodes(kv_cluster, log);
    // update connection fields and leave other fields unchanged
    config.endpoint = disagg_config.s3_config().endpoint();
    config.root = normalizedRoot(disagg_config.s3_config().root());
    config.bucket = disagg_config.s3_config().bucket();
    LOG_INFO(log, "S3 config updated, {}", config.toString());

    shared_tiflash_client = std::make_shared<TiFlashS3Client>(config.bucket, config.root, create());
    client_is_inited = true; // init finish
    return shared_tiflash_client;
}

void ClientFactory::shutdown()
{
    {
        std::unique_lock lock_init(mtx_init);
        // Reset S3Client before Aws::ShutdownAPI.
        shared_tiflash_client.reset();
    }
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
    return create(config, log);
}

std::shared_ptr<TiFlashS3Client> ClientFactory::sharedTiFlashClient()
{
    if (client_is_inited)
        return shared_tiflash_client;

    return initClientFromWriteNode();
}

namespace
{
bool updateRegionByEndpoint(Aws::Client::ClientConfiguration & cfg, const LoggerPtr & log)
{
    if (cfg.endpointOverride.empty())
    {
        return true;
    }

    const Poco::URI uri(cfg.endpointOverride);
    String matched_region;
    static const RE2 region_pattern(R"(^s3[.\-]([a-z0-9\-]+)\.amazonaws\.)");
    if (re2::RE2::PartialMatch(uri.getHost(), region_pattern, &matched_region))
    {
        boost::algorithm::to_lower(matched_region);
        cfg.region = matched_region;
    }
    else
    {
        /// In global mode AWS C++ SDK send `us-east-1` but accept switching to another one if being suggested.
        cfg.region = Aws::Region::AWS_GLOBAL;
    }

    if (uri.getScheme() == "https")
    {
        cfg.scheme = Aws::Http::Scheme::HTTPS;
    }
    else
    {
        cfg.scheme = Aws::Http::Scheme::HTTP;
    }
    cfg.verifySSL = cfg.scheme == Aws::Http::Scheme::HTTPS;

    bool use_virtual_address = true;
    {
        std::string_view view(cfg.endpointOverride);
        if (auto pos = view.find("://"); pos != std::string_view::npos)
        {
            view.remove_prefix(pos + 3); // remove the "<Scheme>://" prefix
        }
        // For deployed with AWS S3 service (or other S3-like service), the address use default port and port is not included,
        // the service need virtual addressing to do load balancing.
        // For deployed with local minio, the address contains fix port, we should disable virtual addressing
        use_virtual_address = (view.find(':') == std::string_view::npos);
    }

    LOG_INFO(
        log,
        "AwsClientConfig{{endpoint={} region={} scheme={} verifySSL={} useVirtualAddressing={}}}",
        cfg.endpointOverride,
        cfg.region,
        magic_enum::enum_name(cfg.scheme),
        cfg.verifySSL,
        use_virtual_address);
    return use_virtual_address;
}
} // namespace

std::unique_ptr<Aws::S3::S3Client> ClientFactory::create(const StorageS3Config & config_, const LoggerPtr & log)
{
    LOG_INFO(log, "Create ClientConfiguration start");
    Aws::Client::ClientConfiguration cfg(/*profileName*/ "", /*shouldDisableIMDS*/ true);
    LOG_INFO(log, "Create ClientConfiguration end");
    cfg.maxConnections = config_.max_connections;
    cfg.requestTimeoutMs = config_.request_timeout_ms;
    cfg.connectTimeoutMs = config_.connection_timeout_ms;
    if (!config_.endpoint.empty())
    {
        cfg.endpointOverride = config_.endpoint;
    }
    bool use_virtual_addressing = updateRegionByEndpoint(cfg, log);
    if (config_.access_key_id.empty() && config_.secret_access_key.empty())
    {
        // Request that does not require authentication.
        // Such as the EC2 access permission to the S3 bucket is configured.
        // If the empty access_key_id and secret_access_key are passed to S3Client,
        // an authentication error will be reported.
        LOG_DEBUG(log, "Create S3Client start");
        auto provider = std::make_shared<S3CredentialsProviderChain>();
        auto cli = std::make_unique<Aws::S3::S3Client>(
            provider,
            cfg,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            /*userVirtualAddressing*/ use_virtual_addressing);
        LOG_DEBUG(log, "Create S3Client end");
        return cli;
    }
    else
    {
        Aws::Auth::AWSCredentials cred(config_.access_key_id, config_.secret_access_key);
        LOG_DEBUG(log, "Create S3Client start");
        auto cli = std::make_unique<Aws::S3::S3Client>(
            cred,
            cfg,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            /*useVirtualAddressing*/ use_virtual_addressing);
        LOG_DEBUG(log, "Create S3Client end");
        return cli;
    }
}


bool isNotFoundError(Aws::S3::S3Errors error)
{
    return error == Aws::S3::S3Errors::RESOURCE_NOT_FOUND || error == Aws::S3::S3Errors::NO_SUCH_KEY;
}

Aws::S3::Model::HeadObjectOutcome headObject(const TiFlashS3Client & client, const String & key)
{
    ProfileEvents::increment(ProfileEvents::S3HeadObject);
    Stopwatch sw;
    SCOPE_EXIT({ GET_METRIC(tiflash_storage_s3_request_seconds, type_head_object).Observe(sw.elapsedSeconds()); });
    Aws::S3::Model::HeadObjectRequest req;
    client.setBucketAndKeyWithRoot(req, key);
    return client.HeadObject(req);
}

bool objectExists(const TiFlashS3Client & client, const String & key)
{
    auto outcome = headObject(client, key);
    if (outcome.IsSuccess())
    {
        return true;
    }
    const auto & error = outcome.GetError();
    if (isNotFoundError(error.GetErrorType()))
    {
        return false;
    }
    throw fromS3Error(error, "S3 HeadObject failed, bucket={} root={} key={}", client.bucket(), client.root(), key);
}

static bool doUploadEmptyFile(
    const TiFlashS3Client & client,
    const String & key,
    const String & tagging,
    Int32 max_retry_times,
    Int32 current_retry)
{
    Stopwatch sw;
    Aws::S3::Model::PutObjectRequest req;
    client.setBucketAndKeyWithRoot(req, key);
    if (!tagging.empty())
        req.SetTagging(tagging);
    req.SetContentType("binary/octet-stream");
    auto istr
        = Aws::MakeShared<Aws::StringStream>("EmptyObjectInputStream", "", std::ios_base::in | std::ios_base::binary);
    req.SetBody(istr);
    ProfileEvents::increment(ProfileEvents::S3PutObject);
    if (current_retry > 0)
    {
        ProfileEvents::increment(ProfileEvents::S3PutObjectRetry);
    }
    auto result = client.PutObject(req);
    if (!result.IsSuccess())
    {
        if (current_retry == max_retry_times - 1) // Last request
        {
            throw fromS3Error(
                result.GetError(),
                "S3 PutEmptyObject failed, bucket={} root={} key={}",
                client.bucket(),
                client.root(),
                key);
        }
        else
        {
            const auto & e = result.GetError();
            LOG_ERROR(
                client.log,
                "S3 PutEmptyObject failed: {}, request_id={} bucket={} root={} key={}",
                e.GetMessage(),
                e.GetRequestId(),
                client.bucket(),
                client.root(),
                key);
            return false;
        }
    }
    auto elapsed_seconds = sw.elapsedSeconds();
    GET_METRIC(tiflash_storage_s3_request_seconds, type_put_object).Observe(elapsed_seconds);
    LOG_DEBUG(client.log, "uploadEmptyFile key={}, cost={:.2f}s", key, elapsed_seconds);
    return true;
}

void uploadEmptyFile(const TiFlashS3Client & client, const String & key, const String & tagging, int max_retry_times)
{
    retryWrapper(doUploadEmptyFile, client, key, tagging, max_retry_times);
}

static bool doUploadFile(
    const TiFlashS3Client & client,
    const String & local_fname,
    const String & remote_fname,
    const EncryptionPath & encryption_path,
    const FileProviderPtr & file_provider,
    Int32 max_retry_times,
    Int32 current_retry)
{
    Stopwatch sw;
    auto is_dmfile = S3FilenameView::fromKey(remote_fname).isDMFile();
    Aws::S3::Model::PutObjectRequest req;
    client.setBucketAndKeyWithRoot(req, remote_fname);
    req.SetContentType("binary/octet-stream");
    auto write_bytes = std::filesystem::file_size(local_fname);
    RandomAccessFilePtr local_file;
    if (file_provider)
    {
        local_file = file_provider->newRandomAccessFile(local_fname, encryption_path, nullptr);
    }
    else
    {
        local_file = PosixRandomAccessFile::create(local_fname);
    }
    // Read at most 1MB each time.
    auto read_buf
        = std::make_unique<ReadBufferFromRandomAccessFile>(local_file, std::min(1 * 1024 * 1024, write_bytes));
    auto istr = Aws::MakeShared<StdStreamFromReadBuffer>("PutObjectInputStream", std::move(read_buf), write_bytes);
    req.SetContentLength(write_bytes);
    req.SetBody(istr);
    ProfileEvents::increment(is_dmfile ? ProfileEvents::S3PutDMFile : ProfileEvents::S3PutObject);
    if (current_retry > 0)
    {
        ProfileEvents::increment(is_dmfile ? ProfileEvents::S3PutDMFileRetry : ProfileEvents::S3PutObjectRetry);
    }
    auto result = client.PutObject(req);
    if (!result.IsSuccess())
    {
        if (current_retry == max_retry_times - 1) // Last request
        {
            throw fromS3Error(
                result.GetError(),
                "S3 PutObject failed, local_fname={} bucket={} root={} key={}",
                local_fname,
                client.bucket(),
                client.root(),
                remote_fname);
        }
        else
        {
            const auto & e = result.GetError();
            LOG_ERROR(
                client.log,
                "S3 PutObject failed: {}, request_id={} local_fname={} bucket={} root={} key={}",
                e.GetMessage(),
                e.GetRequestId(),
                local_fname,
                client.bucket(),
                client.root(),
                remote_fname);
            return false;
        }
    }
    ProfileEvents::increment(is_dmfile ? ProfileEvents::S3WriteDMFileBytes : ProfileEvents::S3WriteBytes, write_bytes);
    auto elapsed_seconds = sw.elapsedSeconds();
    if (is_dmfile)
    {
        GET_METRIC(tiflash_storage_s3_request_seconds, type_put_dmfile).Observe(elapsed_seconds);
    }
    else
    {
        GET_METRIC(tiflash_storage_s3_request_seconds, type_put_object).Observe(elapsed_seconds);
    }
    LOG_DEBUG(
        client.log,
        "uploadFile local_fname={}, key={}, write_bytes={} cost={:.3f}s",
        local_fname,
        remote_fname,
        write_bytes,
        elapsed_seconds);
    return true;
}

void uploadFile(
    const TiFlashS3Client & client,
    const String & local_fname,
    const String & remote_fname,
    const EncryptionPath & encryption_path,
    const FileProviderPtr & file_provider,
    int max_retry_times)
{
    retryWrapper(doUploadFile, client, local_fname, remote_fname, encryption_path, file_provider, max_retry_times);
}

void downloadFile(const TiFlashS3Client & client, const String & local_fname, const String & remote_fname)
{
    Stopwatch sw;
    Aws::S3::Model::GetObjectRequest req;
    client.setBucketAndKeyWithRoot(req, remote_fname);
    ProfileEvents::increment(ProfileEvents::S3GetObject);
    auto outcome = client.GetObject(req);
    if (!outcome.IsSuccess())
    {
        throw fromS3Error(
            outcome.GetError(),
            "S3 GetObject failed, bucket={} root={} key={}",
            client.bucket(),
            client.root(),
            remote_fname);
    }
    ProfileEvents::increment(ProfileEvents::S3ReadBytes, outcome.GetResult().GetContentLength());
    GET_METRIC(tiflash_storage_s3_request_seconds, type_get_object).Observe(sw.elapsedSeconds());
    Aws::OFStream ostr(local_fname, std::ios_base::out | std::ios_base::binary);
    RUNTIME_CHECK_MSG(ostr.is_open(), "Open {} fail: {}", local_fname, strerror(errno));
    ostr << outcome.GetResult().GetBody().rdbuf();
    RUNTIME_CHECK_MSG(ostr.good(), "Write {} fail: {}", local_fname, strerror(errno));
}


void downloadFileByS3RandomAccessFile(
    std::shared_ptr<TiFlashS3Client> client,
    const String & local_fname,
    const String & remote_fname)
{
    Stopwatch sw;
    S3RandomAccessFile file(client, remote_fname);
    Aws::OFStream ostr(local_fname, std::ios_base::out | std::ios_base::binary);
    RUNTIME_CHECK_MSG(ostr.is_open(), "Open {} fail: {}", local_fname, strerror(errno));

    char buf[8192];
    while (true)
    {
        auto n = file.read(buf, sizeof(buf));
        RUNTIME_CHECK(n >= 0, remote_fname);
        if (n == 0)
        {
            break;
        }

        ostr.write(buf, n);
        RUNTIME_CHECK_MSG(ostr.good(), "Write {} fail: {}", local_fname, strerror(errno));
    }
}

void rewriteObjectWithTagging(const TiFlashS3Client & client, const String & key, const String & tagging)
{
    Stopwatch sw;
    Aws::S3::Model::CopyObjectRequest req;
    // rewrite the object with `key`, adding tagging to the new object
    // The copy_source format is "${source_bucket}/${source_key}"
    auto copy_source = client.bucket() + "/" + (client.root() == "/" ? "" : client.root()) + key;
    client.setBucketAndKeyWithRoot(req, key);
    // metadata directive and tagging directive must be set to `REPLACE`
    req.WithCopySource(copy_source) //
        .WithMetadataDirective(Aws::S3::Model::MetadataDirective::REPLACE)
        .WithTagging(tagging)
        .WithTaggingDirective(Aws::S3::Model::TaggingDirective::REPLACE);
    ProfileEvents::increment(ProfileEvents::S3CopyObject);
    auto outcome = client.CopyObject(req);
    if (!outcome.IsSuccess())
    {
        throw fromS3Error(
            outcome.GetError(),
            "S3 CopyObject failed, bucket={} root={} key={}",
            client.bucket(),
            client.root(),
            key);
    }
    auto elapsed_seconds = sw.elapsedSeconds();
    GET_METRIC(tiflash_storage_s3_request_seconds, type_copy_object).Observe(elapsed_seconds);
    LOG_DEBUG(client.log, "rewrite object key={} cost={:.2f}s", key, elapsed_seconds);
}

bool ensureLifecycleRuleExist(const TiFlashS3Client & client, Int32 expire_days)
{
    bool lifecycle_rule_has_been_set = false;
    Aws::Vector<Aws::S3::Model::LifecycleRule> old_rules;
    do
    {
        Aws::S3::Model::GetBucketLifecycleConfigurationRequest req;
        req.SetBucket(client.bucket());
        auto outcome = client.GetBucketLifecycleConfiguration(req);
        if (!outcome.IsSuccess())
        {
            const auto & error = outcome.GetError();
            // The life cycle is not added at all
            if (error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND
                || error.GetExceptionName() == "NoSuchLifecycleConfiguration")
            {
                break;
            }

            LOG_WARNING(
                client.log,
                "GetBucketLifecycle fail, please check the bucket lifecycle configuration or create the lifecycle rule "
                "manually"
                ", bucket={} {}",
                client.bucket(),
                S3ErrorMessage(error));
            return false;
        }

        auto res = outcome.GetResultWithOwnership();
        old_rules = res.GetRules();
        static_assert(TaggingObjectIsDeleted == "tiflash_deleted=true");
        for (const auto & rule : old_rules)
        {
            const auto & filt = rule.GetFilter();
            if (!filt.AndHasBeenSet())
            {
                continue;
            }
            const auto & and_op = filt.GetAnd();
            const auto & tags = and_op.GetTags();
            if (tags.size() != 1 || !and_op.PrefixHasBeenSet() || !and_op.GetPrefix().empty())
            {
                continue;
            }

            const auto & tag = tags[0];
            if (rule.GetStatus() == Aws::S3::Model::ExpirationStatus::Enabled && tag.GetKey() == "tiflash_deleted"
                && tag.GetValue() == "true")
            {
                lifecycle_rule_has_been_set = true;
                break;
            }
        }
    } while (false);

    if (lifecycle_rule_has_been_set)
    {
        LOG_INFO(
            client.log,
            "The lifecycle rule has been set, n_rules={} filter={}",
            old_rules.size(),
            TaggingObjectIsDeleted);
        return true;
    }

    // Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3OutpostsLifecycleCLIJava.html
    LOG_INFO(
        client.log,
        "The lifecycle rule with filter \"{}\" has not been added, n_rules={}",
        TaggingObjectIsDeleted,
        old_rules.size());
    static_assert(TaggingObjectIsDeleted == "tiflash_deleted=true");
    std::vector<Aws::S3::Model::Tag> filter_tags{
        Aws::S3::Model::Tag().WithKey("tiflash_deleted").WithValue("true"),
    };

    Aws::S3::Model::LifecycleRule rule;
    rule.WithStatus(Aws::S3::Model::ExpirationStatus::Enabled)
        .WithFilter(Aws::S3::Model::LifecycleRuleFilter().WithAnd(
            Aws::S3::Model::LifecycleRuleAndOperator().WithPrefix("").WithTags(filter_tags)))
        .WithExpiration(Aws::S3::Model::LifecycleExpiration().WithDays(expire_days))
        .WithID("tiflashgc");

    old_rules.emplace_back(rule); // existing rules + new rule
    Aws::S3::Model::BucketLifecycleConfiguration lifecycle_config;
    lifecycle_config.WithRules(old_rules);

    Aws::S3::Model::PutBucketLifecycleConfigurationRequest request;
    request.WithBucket(client.bucket()).WithLifecycleConfiguration(lifecycle_config);

    auto outcome = client.PutBucketLifecycleConfiguration(request);
    if (!outcome.IsSuccess())
    {
        const auto & error = outcome.GetError();
        LOG_WARNING(
            client.log,
            "Create lifecycle rule with tag filter \"{}\" failed, please check the bucket lifecycle configuration or "
            "create the lifecycle rule manually"
            ", bucket={} {}",
            TaggingObjectIsDeleted,
            client.bucket(),
            S3ErrorMessage(error));
        return false;
    }
    LOG_INFO(
        client.log,
        "The lifecycle rule has been added, new_n_rules={} tag={}",
        old_rules.size(),
        TaggingObjectIsDeleted);
    return true;
}

void listPrefix(
    const TiFlashS3Client & client,
    const String & prefix,
    std::function<PageResult(const Aws::S3::Model::Object & object)> pager)
{
    Stopwatch sw;
    Aws::S3::Model::ListObjectsV2Request req;
    bool is_root_single_slash = client.root() == "/";
    // If the `root == '/'`, don't prepend the root to the prefix, otherwise S3 list doesn't work.
    req.WithBucket(client.bucket()).WithPrefix(is_root_single_slash ? prefix : client.root() + prefix);

    // If the `root == '/'`, then the return result will cut it off
    // else we need to cut the root in the following codes
    bool need_cut = !is_root_single_slash;
    size_t cut_size = client.root().size();

    bool done = false;
    size_t num_keys = 0;
    while (!done)
    {
        Stopwatch sw_list;
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        auto outcome = client.ListObjectsV2(req);
        if (!outcome.IsSuccess())
        {
            throw fromS3Error(
                outcome.GetError(),
                "S3 ListObjectV2s failed, bucket={} root={} prefix={}",
                client.bucket(),
                client.root(),
                prefix);
        }
        GET_METRIC(tiflash_storage_s3_request_seconds, type_list_objects).Observe(sw_list.elapsedSeconds());

        PageResult page_res{};
        const auto & result = outcome.GetResult();
        auto page_keys = result.GetContents().size();
        num_keys += page_keys;
        for (const auto & object : result.GetContents())
        {
            if (!need_cut)
            {
                page_res = pager(object);
            }
            else
            {
                // Copy the `Object` to cut off the `root` from key, the cost should be acceptable :(
                auto object_without_root = object;
                object_without_root.SetKey(object.GetKey().substr(cut_size, object.GetKey().size()));
                page_res = pager(object_without_root);
            }
            if (!page_res.more)
                break;
        }

        // handle the result size over max size
        done = !result.GetIsTruncated();
        if (!done && page_res.more)
        {
            const auto & next_token = result.GetNextContinuationToken();
            req.SetContinuationToken(next_token);
            LOG_DEBUG(
                client.log,
                "listPrefix prefix={}, keys={}, total_keys={}, next_token={}",
                prefix,
                page_keys,
                num_keys,
                next_token);
        }
    }
    LOG_DEBUG(client.log, "listPrefix prefix={}, total_keys={}, cost={:.2f}s", prefix, num_keys, sw.elapsedSeconds());
}

// Check the docs here for Delimiter && CommonPrefixes when you really need it.
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
void listPrefixWithDelimiter(
    const TiFlashS3Client & client,
    const String & prefix,
    std::string_view delimiter,
    std::function<PageResult(const Aws::S3::Model::CommonPrefix & common_prefix)> pager)
{
    Stopwatch sw;
    Aws::S3::Model::ListObjectsV2Request req;
    bool is_root_single_slash = client.root() == "/";
    // If the `root == '/'`, don't prepend the root to the prefix, otherwise S3 list doesn't work.
    req.WithBucket(client.bucket()).WithPrefix(is_root_single_slash ? prefix : client.root() + prefix);
    if (!delimiter.empty())
    {
        req.SetDelimiter(String(delimiter));
    }

    // If the `root == '/'`, then the return result will cut it off
    // else we need to cut the root in the following codes
    bool need_cut = !is_root_single_slash;
    size_t cut_size = client.root().size();

    bool done = false;
    size_t num_keys = 0;
    while (!done)
    {
        Stopwatch sw_list;
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        auto outcome = client.ListObjectsV2(req);
        if (!outcome.IsSuccess())
        {
            throw fromS3Error(
                outcome.GetError(),
                "S3 ListObjectV2s failed, bucket={} root={} prefix={} delimiter={}",
                client.bucket(),
                client.root(),
                prefix,
                delimiter);
        }
        GET_METRIC(tiflash_storage_s3_request_seconds, type_list_objects).Observe(sw_list.elapsedSeconds());

        PageResult page_res{};
        const auto & result = outcome.GetResult();
        auto page_keys = result.GetCommonPrefixes().size();
        num_keys += page_keys;
        for (const auto & prefix : result.GetCommonPrefixes())
        {
            if (!need_cut)
            {
                page_res = pager(prefix);
            }
            else
            {
                // Copy the `CommonPrefix` to cut off the `root`, the cost should be acceptable :(
                auto prefix_without_root = prefix;
                prefix_without_root.SetPrefix(prefix.GetPrefix().substr(cut_size, prefix.GetPrefix().size()));
                page_res = pager(prefix_without_root);
            }
            if (!page_res.more)
                break;
        }

        // handle the result size over max size
        done = !result.GetIsTruncated();
        if (!done && page_res.more)
        {
            const auto & next_token = result.GetNextContinuationToken();
            req.SetContinuationToken(next_token);
            LOG_DEBUG(
                client.log,
                "listPrefixWithDelimiter prefix={}, delimiter={}, keys={}, total_keys={}, next_token={}",
                prefix,
                delimiter,
                page_keys,
                num_keys,
                next_token);
        }
    }
    LOG_DEBUG(
        client.log,
        "listPrefixWithDelimiter prefix={}, delimiter={}, total_keys={}, cost={:.2f}s",
        prefix,
        delimiter,
        num_keys,
        sw.elapsedSeconds());
}

std::optional<String> anyKeyExistWithPrefix(const TiFlashS3Client & client, const String & prefix)
{
    std::optional<String> key_opt;
    listPrefix(client, prefix, [&key_opt](const Aws::S3::Model::Object & object) {
        key_opt = object.GetKey();
        return PageResult{
            .num_keys = 1,
            .more = false, // do not need more result
        };
    });
    return key_opt;
}

std::unordered_map<String, size_t> listPrefixWithSize(const TiFlashS3Client & client, const String & prefix)
{
    std::unordered_map<String, size_t> keys_with_size;
    listPrefix(client, prefix, [&](const Aws::S3::Model::Object & object) {
        keys_with_size.emplace(object.GetKey().substr(prefix.size()), object.GetSize()); // Cut prefix
        return PageResult{.num_keys = 1, .more = true};
    });

    return keys_with_size;
}

ObjectInfo tryGetObjectInfo(const TiFlashS3Client & client, const String & key)
{
    auto o = headObject(client, key);
    if (!o.IsSuccess())
    {
        if (const auto & err = o.GetError(); isNotFoundError(err.GetErrorType()))
        {
            return ObjectInfo{.exist = false, .size = 0, .last_modification_time = {}};
        }
        throw fromS3Error(
            o.GetError(),
            "S3 HeadObject failed, bucket={} root={} key={}",
            client.bucket(),
            client.root(),
            key);
    }
    // Else the object still exist
#ifndef FIU_ENABLE
    const auto & res = o.GetResult();
#else
    // handle the failpoint for hijacking the last modified time of returned object
    auto & res = o.GetResult();
    auto try_set_mtime = [&] {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_set_mocked_s3_object_mtime); v)
        {
            auto m = std::any_cast<std::map<String, Aws::Utils::DateTime>>(v.value());
            const auto & req_key = key;
            if (auto iter_m = m.find(req_key); iter_m != m.end())
            {
                res.SetLastModified(iter_m->second);
                LOG_WARNING(
                    Logger::get(),
                    "failpoint set mtime, key={} mtime={}",
                    req_key,
                    iter_m->second.ToGmtString(Aws::Utils::DateFormat::ISO_8601));
            }
            else
            {
                LOG_WARNING(Logger::get(), "failpoint set mtime failed, key={}", req_key);
            }
        }
    };
    UNUSED(try_set_mtime);
    fiu_do_on(FailPoints::force_set_mocked_s3_object_mtime, { try_set_mtime(); });
#endif
    // "DeleteMark" of S3 service, don't know what will lead to this
    RUNTIME_CHECK(!res.GetDeleteMarker(), client.bucket(), key);
    return ObjectInfo{.exist = true, .size = res.GetContentLength(), .last_modification_time = res.GetLastModified()};
}

void deleteObject(const TiFlashS3Client & client, const String & key)
{
    Stopwatch sw;
    Aws::S3::Model::DeleteObjectRequest req;
    client.setBucketAndKeyWithRoot(req, key);
    ProfileEvents::increment(ProfileEvents::S3DeleteObject);
    auto o = client.DeleteObject(req);
    if (!o.IsSuccess())
    {
        const auto & e = o.GetError();
        if (e.GetErrorType() != Aws::S3::S3Errors::NO_SUCH_KEY)
        {
            throw fromS3Error(
                o.GetError(),
                "S3 DeleteObject failed, bucket={} root={} key={}",
                client.bucket(),
                client.root(),
                key);
        }
    }
    else
    {
        const auto & res = o.GetResult();
        UNUSED(res);
    }
    GET_METRIC(tiflash_storage_s3_request_seconds, type_delete_object).Observe(sw.elapsedSeconds());
}

void rawListPrefix(
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

    static auto log = Logger::get("S3RawListPrefix");

    bool done = false;
    size_t num_keys = 0;
    while (!done)
    {
        Stopwatch sw_list;
        ProfileEvents::increment(ProfileEvents::S3ListObjects);
        auto outcome = client.ListObjectsV2(req);
        if (!outcome.IsSuccess())
        {
            throw fromS3Error(
                outcome.GetError(),
                "S3 ListObjectV2s failed, bucket={} prefix={} delimiter={}",
                bucket,
                prefix,
                delimiter);
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
            LOG_DEBUG(
                log,
                "rawListPrefix bucket={} prefix={} delimiter={} keys={} total_keys={} next_token={}",
                bucket,
                prefix,
                delimiter,
                page_res.num_keys,
                num_keys,
                next_token);
        }
    }
    LOG_DEBUG(
        log,
        "rawListPrefix bucket={} prefix={} delimiter={} total_keys={} cost={:.2f}s",
        bucket,
        prefix,
        delimiter,
        num_keys,
        sw.elapsedSeconds());
}

void rawDeleteObject(const Aws::S3::S3Client & client, const String & bucket, const String & key)
{
    Stopwatch sw;
    Aws::S3::Model::DeleteObjectRequest req;
    req.WithBucket(bucket).WithKey(key);
    ProfileEvents::increment(ProfileEvents::S3DeleteObject);
    auto o = client.DeleteObject(req);
    if (!o.IsSuccess())
    {
        throw fromS3Error(o.GetError(), "S3 DeleteObject failed, bucket={} key={}", bucket, key);
    }
    const auto & res = o.GetResult();
    UNUSED(res);
    GET_METRIC(tiflash_storage_s3_request_seconds, type_delete_object).Observe(sw.elapsedSeconds());
}

} // namespace DB::S3
