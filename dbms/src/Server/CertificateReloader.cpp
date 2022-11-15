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

#include "CertificateReloader.h"

#include <cstddef>
#include <memory>

#if Poco_NetSSL_FOUND


#include <Common/Exception.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/Utility.h>
#include <common/logger_useful.h>

namespace DB
{
namespace
{
/// Call set process for certificate.
int callSetCertificate(SSL * ssl, [[maybe_unused]] void * arg)
{
    return CertificateReloader::instance().setCertificate(ssl);
}
} // namespace

/// This is callback for OpenSSL. It will be called on every connection to obtain a certificate and private key.
int CertificateReloader::setCertificate(SSL * ssl)
{
    LOG_INFO(log, "ywq test setCetificate callaback");

    // if (config->updated())
    // {
    Poco::Crypto::X509Certificate cert(config->cert_path);
    LOG_INFO(log, "ywq test cert common name: {}", cert.commonName());
    Poco::Crypto::EVPPKey key("", config->key_path);
    SSL_use_certificate(ssl, const_cast<X509 *>(cert.certificate()));
    SSL_use_PrivateKey(ssl, const_cast<EVP_PKEY *>(static_cast<const EVP_PKEY *>(key)));
    // }

    int err = SSL_check_private_key(ssl);
    if (err != 1)
    {
        std::string msg = Poco::Net::Utility::getLastError();
        LOG_ERROR(log, "Unusable ssl certificate key-pair {}", msg);
        return -1;
    }

    return 1;
}

void CertificateReloader::initSSLCallback(Poco::Net::Context::Ptr context)
{
    LOG_DEBUG(log, "ywq test Initializing certificate reloader for context");
    SSL_CTX_set_cert_cb(context->sslContext(), callSetCertificate, nullptr);
}
} // namespace DB

#endif
