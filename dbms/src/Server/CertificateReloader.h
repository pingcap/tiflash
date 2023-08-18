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

#include <Common/config.h>

#if Poco_NetSSL_FOUND

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Poco/Net/Context.h>
#include <openssl/ssl.h>

#include <ext/singleton.h>

namespace DB
{

class CertificateReloader : public ext::Singleton<CertificateReloader>
{
public:
    static void initSSLCallback(Poco::Net::Context::Ptr context, Context * global_context);
    /// A callback for OpenSSL
    int setCertificate(SSL * ssl, void * arg);

private:
    LoggerPtr log = Logger::get("CertificateReloader");
};
} // namespace DB

#endif
