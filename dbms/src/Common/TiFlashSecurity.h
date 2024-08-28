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
                ca_path = Poco::trim(config.getString("security.ca_path"));
                miss_ca_path = ca_path.empty();
            }
            if (config.has("security.cert_path"))
            {
                cert_path = Poco::trim(config.getString("security.cert_path"));
                miss_cert_path = cert_path.empty();
            }
            if (config.has("security.key_path"))
            {
                key_path = Poco::trim(config.getString("security.key_path"));
                miss_key_path = key_path.empty();
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
            }
        }
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
};

} // namespace DB
