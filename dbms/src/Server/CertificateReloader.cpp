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

#include <Common/TiFlashSecurity.h>
#include <Interpreters/Context.h>
#include <Server/CertificateReloader.h>

#if Poco_NetSSL_FOUND

#include <Poco/Net/Utility.h>

namespace DB
{
namespace
{
/// Call set process for certificate.
int callSetCertificate(SSL * ssl, void * arg)
{
    return CertificateReloader::instance().setCertificate(ssl, arg);
}
} // namespace

/// This is callback for OpenSSL. It will be called on every connection to obtain a certificate and private key.
int CertificateReloader::setCertificate(SSL * ssl, void * arg)
{
    RUNTIME_ASSERT(ssl);
    auto * context = static_cast<Context *>(arg);
    auto security_config = context->getSecurityConfig();
    auto [cert_path, key_path] = security_config->getCertAndKeyPath();
    LOG_DEBUG(log, "setCertificate callback called, cert_path: {}, key_path: {}", cert_path, key_path);
    Poco::Crypto::X509Certificate cert(cert_path);
    Poco::Crypto::EVPPKey key("", key_path);
    int err = SSL_use_certificate(ssl, const_cast<X509 *>(cert.certificate()));
    if (err != 1)
    {
        LOG_ERROR(log, "Unusable ssl certificate {}", Poco::Net::Utility::getLastError());
    }

    err = SSL_use_PrivateKey(ssl, const_cast<EVP_PKEY *>(static_cast<const EVP_PKEY *>(key)));
    if (err != 1)
    {
        LOG_ERROR(log, "Unusable ssl key {}", Poco::Net::Utility::getLastError());
    }

    err = SSL_check_private_key(ssl);
    if (err != 1)
    {
        LOG_ERROR(log, "Unusable ssl certificate key-pair {}", Poco::Net::Utility::getLastError());
        return -1;
    }

    return 1;
}

void CertificateReloader::initSSLCallback(Poco::Net::Context::Ptr context, Context * global_context)
{
    SSL_CTX_set_cert_cb(context->sslContext(), callSetCertificate, reinterpret_cast<void *>(global_context));
}
} // namespace DB

#endif
