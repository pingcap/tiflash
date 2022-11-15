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

#include <Common/config.h>

#include <memory>

#if Poco_NetSSL_FOUND

#include <Poco/Crypto/RSAKey.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Poco/Logger.h>
#include <Poco/Net/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/MultiVersion.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <ext/singleton.h>
#include <filesystem>
#include <string>

#include "Common/TiFlashSecurity.h"


namespace DB
{

class CertificateReloader : public ext::Singleton<CertificateReloader>
{
public:
    using stat_t = struct stat;
    void initSSLCallback(Poco::Net::Context::Ptr context);

    /// A callback for OpenSSL
    int setCertificate(SSL * ssl);
    std::shared_ptr<TiFlashSecurityConfig> config;
private:
    Poco::Logger * log = &Poco::Logger::get("CertificateReloader");
};

} // namespace DB

#endif
