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
#include <Common/grpcpp.h>
#include <Core/Types.h>
#include <IO/createReadBufferFromFileBase.h>
#include <Poco/String.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <common/logger_useful.h>

#include <set>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}

struct TiFlashSecurityConfig
{
    String ca_path;
    String cert_path;
    String key_path;

    bool redact_info_log = false;

    std::set<String> allowed_common_names;

    bool inited = false;
    bool has_tls_config = false;
    grpc::SslCredentialsOptions options;

public:
<<<<<<< HEAD
    TiFlashSecurityConfig() = default;

    TiFlashSecurityConfig(Poco::Util::LayeredConfiguration & config, const LoggerPtr & log)
    {
        if (config.has("security"))
        {
            bool miss_ca_path = true;
            bool miss_cert_path = true;
            bool miss_key_path = true;
            if (config.has("security.ca_path"))
            {
                ca_path = config.getString("security.ca_path");
                miss_ca_path = false;
            }
            if (config.has("security.cert_path"))
            {
                cert_path = config.getString("security.cert_path");
                miss_cert_path = false;
            }
            if (config.has("security.key_path"))
            {
                key_path = config.getString("security.key_path");
                miss_key_path = false;
            }
            if (miss_ca_path && miss_cert_path && miss_key_path)
            {
                LOG_INFO(log, "No security config is set.");
            }
            else if (miss_ca_path || miss_cert_path || miss_key_path)
            {
                throw Exception("ca_path, cert_path, key_path must be set at the same time.", ErrorCodes::INVALID_CONFIG_PARAMETER);
            }
            else
            {
                has_tls_config = true;
                LOG_INFO(
                    log,
                    "security config is set: ca path is {} cert path is {} key path is {}",
                    ca_path,
                    cert_path,
                    key_path);
            }

            if (config.has("security.cert_allowed_cn") && has_tls_config)
            {
                String verify_cns = config.getString("security.cert_allowed_cn");
                parseAllowedCN(verify_cns);
            }

            // redact_info_log = config.getBool("security.redact-info-log", false);
            // Mostly options name are combined with "_", keep this style
            if (config.has("security.redact_info_log"))
            {
                redact_info_log = config.getBool("security.redact_info_log");
=======
    explicit TiFlashSecurityConfig(const LoggerPtr & log_)
        : log(log_)
    {}

    void init(Poco::Util::AbstractConfiguration & config)
    {
        if (!inited)
        {
            update(config);
            inited = true;
        }
    }

    bool hasTlsConfig()
    {
        std::unique_lock lock(mu);
        return has_tls_config;
    }

    RedactMode redactInfoLog()
    {
        std::unique_lock lock(mu);
        return redact_info_log;
    }

    std::tuple<String, String, String> getPaths()
    {
        std::unique_lock lock(mu);
        return {ca_path, cert_path, key_path};
    }

    std::pair<String, String> getCertAndKeyPath()
    {
        std::unique_lock lock(mu);
        return {cert_path, key_path};
    }

    std::set<String> allowedCommonNames()
    {
        std::unique_lock lock(mu);
        return allowed_common_names;
    }

    // return value indicate whether the Ssl certificate path is changed.
    bool update(Poco::Util::AbstractConfiguration & config)
    {
        std::unique_lock lock(mu);
        if (!config.has("security"))
        {
            if (inited && has_security)
            {
                LOG_WARNING(log, "Can't remove security config online");
            }
            else
            {
                LOG_INFO(log, "security config is not set");
>>>>>>> 951e010ab8 (security: Allow empty security config for disabling tls (#9234))
            }
            return false;
        }
<<<<<<< HEAD
=======

        assert(config.has("security"));
        if (inited && !has_security)
        {
            LOG_WARNING(log, "Can't add security config online");
            return false;
        }
        has_security = true;

        bool cert_file_updated = updateCertPath(config);

        if (config.has("security.cert_allowed_cn") && has_tls_config)
        {
            String verify_cns = config.getString("security.cert_allowed_cn");
            allowed_common_names = parseAllowedCN(verify_cns);
        }

        // Mostly options name are combined with "_", keep this style
        if (config.has("security.redact_info_log"))
        {
            redact_info_log = parseRedactLog(config.getString("security.redact_info_log"));
        }
        return cert_file_updated;
>>>>>>> 951e010ab8 (security: Allow empty security config for disabling tls (#9234))
    }

    void parseAllowedCN(String verify_cns)
    {
        if (verify_cns.size() > 2 && verify_cns[0] == '[' && verify_cns[verify_cns.size() - 1] == ']')
        {
            verify_cns = verify_cns.substr(1, verify_cns.size() - 2);
        }
        Poco::StringTokenizer string_tokens(verify_cns, ",");
        for (const auto & string_token : string_tokens)
        {
            std::string cn = Poco::trim(string_token);
            if (cn.size() > 2 && cn[0] == '\"' && cn[cn.size() - 1] == '\"')
            {
                cn = cn.substr(1, cn.size() - 2);
            }
            allowed_common_names.insert(std::move(cn));
        }
    }


    bool checkGrpcContext(const grpc::ServerContext * grpc_context) const
    {
        if (allowed_common_names.empty() || grpc_context == nullptr)
        {
            return true;
        }
        auto auth_context = grpc_context->auth_context();
        for (auto && [property_name, common_name] : *auth_context)
        {
            if (property_name == GRPC_X509_CN_PROPERTY_NAME && allowed_common_names.count(String(common_name.data(), common_name.size())))
                return true;
        }
        return false;
    }

    grpc::SslCredentialsOptions readAndCacheSecurityInfo()
    {
        if (inited)
        {
            return options;
        }
        options.pem_root_certs = readFile(ca_path);
        options.pem_cert_chain = readFile(cert_path);
        options.pem_private_key = readFile(key_path);
        inited = true;
        return options;
    }

private:
    static String readFile(const String & filename)
    {
        if (filename.empty())
        {
            return "";
        }
        auto buffer = createReadBufferFromFileBase(filename, 1024, 0);
        String result;
        while (!buffer->eof())
        {
            char buf[1024];
            size_t len = buffer->read(buf, 1024);
            result.append(buf, len);
        }
        return result;
    }
<<<<<<< HEAD
=======

    bool updateCertPath(Poco::Util::AbstractConfiguration & config)
    {
        bool miss_ca_path = true;
        bool miss_cert_path = true;
        bool miss_key_path = true;
        String new_ca_path;
        String new_cert_path;
        String new_key_path;
        if (config.has("security.ca_path"))
        {
            new_ca_path = Poco::trim(config.getString("security.ca_path"));
            miss_ca_path = new_ca_path.empty();
        }
        if (config.has("security.cert_path"))
        {
            new_cert_path = Poco::trim(config.getString("security.cert_path"));
            miss_cert_path = new_cert_path.empty();
        }
        if (config.has("security.key_path"))
        {
            new_key_path = Poco::trim(config.getString("security.key_path"));
            miss_key_path = new_key_path.empty();
        }

        if (miss_ca_path && miss_cert_path && miss_key_path)
        {
            // all configs are not exist
            if (inited && has_tls_config)
            {
                LOG_WARNING(log, "Can't remove tls config online");
            }
            else
            {
                LOG_INFO(log, "No TLS config is set.");
            }
            return false;
        }
        else if (miss_ca_path || miss_cert_path || miss_key_path)
        {
            // any of these configs is not exist
            throw Exception(
                "ca_path, cert_path, key_path must be set at the same time.",
                ErrorCodes::INVALID_CONFIG_PARAMETER);
        }

        // all configs are exist
        assert(!miss_ca_path && !miss_cert_path && !miss_key_path);
        if (inited && !has_tls_config)
        {
            LOG_WARNING(log, "Can't add TLS config online");
            return false;
        }

        has_tls_config = true; // update this->has_tls_config
        if (new_ca_path != ca_path || new_cert_path != cert_path || new_key_path != key_path)
        {
            // any path is changed
            ca_path = new_ca_path;
            cert_path = new_cert_path;
            key_path = new_key_path;
            cert_files.files.clear();
            cert_files.addIfExists(ca_path);
            cert_files.addIfExists(cert_path);
            cert_files.addIfExists(key_path);
            ssl_cerd_options_cached = false;
            LOG_INFO(
                log,
                "Ssl certificate config path is updated: ca path is {} cert path is {} key path is {}",
                ca_path,
                cert_path,
                key_path);
            return true;
        }

        // whether the cert file content is updated
        if (!fileUpdated())
            return false;

        // update cert files
        FilesChangesTracker new_files;
        for (const auto & file : cert_files.files)
        {
            new_files.addIfExists(file.path);
        }
        cert_files = std::move(new_files);
        ssl_cerd_options_cached = false;
        return true;
    }

private:
    mutable std::mutex mu;
    String ca_path;
    String cert_path;
    String key_path;

    FilesChangesTracker cert_files;
    std::set<String> allowed_common_names;

    RedactMode redact_info_log = RedactMode::Disable;

    bool has_tls_config = false;
    bool has_security = false;
    bool inited = false;

    bool ssl_cerd_options_cached = false;
    grpc::SslCredentialsOptions options;

    LoggerPtr log;
>>>>>>> 951e010ab8 (security: Allow empty security config for disabling tls (#9234))
};

} // namespace DB
