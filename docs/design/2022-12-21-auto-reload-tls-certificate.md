# Auto reload TLS certificate for TiFlash

- Author: [Weiqi Yan](https://github.com/ywqzzy)

## Introduction

In TiFlash config, we can set certificate as follows:
```YAML
[security]
ca_path = "/path/to/tls/ca.crt"
cert_path = "/path/to/tls/tiflash.crt"
key_path = "/path/to/tls/tiflash.pem"
```
Then the TiFlash Server can use TLS certificates to enable secure transmission.

Since the TLS certificate has a valid period, in order not to affect the normal operation of online business, the TiFlash node should not be restarted manually when replacing the certificate, so it needs to support automatic rotation of TLS certificates.

By modifying the TiFlash configuration file(tiflash.toml) or the certificate content, TiFlash can dynamically load new TLS certificates.

## Desgin

### Overview

TiFlash uses TLS certificates in GRPC Server, TCP Server, HTTP Server, MetricsPrometheus HTTP Server, client-c (rpcClient, pdClient).

There are two ways to modify the certificate:

1. Modify the path of the certificate in the config file.

2. Directly overwrite the content of the certificate at the specified path.

For 1, we use `main_config_reloader` to monitor the change of the certificate file path, then update the certificate path information in `Context` after the change of the file path, then update the certificate path maintained in the `TiFlashSecurity` class.

For 2, the certificate can be dynamically loaded in the form of callback for various servers, and the client connection can be rebuilt for the client.

### Detailed Design

#### GRPC Server

The certificate used by GRPC server should change with the change of the certificate path in `TiFlashSecurity`.
At the same time, each time a new SSL connection is created, a new certificate can be loaded according to the certificate path.

We need to set one `ConfigFetcher `when building GRPC server in order to dynamically read new certificates when establishing a new SSL connection.

The `ConfigFetcher` will be set as follows:

```C++
builder.AddListeningPort(
    raft_config.flash_server_addr,
    sslServerCredentialsWithFetcher(context));
```

The `sslServerCredentialsWithFetcher` method will set the `ConfigFetcher` for `grpc::ServerBuilder`. As a callback function, `ConfigFetcher` obtains the certificate path from TiFlashSecurity and sets the certificate for each SSL connection.

#### HTTP/TCP Server/MetricPrometheus

These server use `Poco::Net`. To reload certificate dynamically, we can call `SSL_CTX_set_cert_cb` for `Poco::Net::Context::sslContext`. Then these servers can dynamically read new certificates and set new SSL certs when establishing SSL connections with others.

The setting process is as follows:

```C++
SSL_CTX_set_cert_cb(context->sslContext(), 
    callSetCertificate, 
    reinterpret_cast<void *>(global_context));
```

`callSetCertificate` will read the certificate path from `TiFlashSecurity`, then read the new certificate from certificate path in order to set the new certificate.

#### client-c

Judge whether the certificate file has changed (or whether the certificate path has changed) in `main_config_reloader`. When the certificate has changed, read `TiFlashSecurity` to get new certificate paths, and clear the existing client conn array (including `pdClient` and `rpcClient`), so that the new certificate can be read later to create a new connection.

#### ConfigReloader

The `ConfigReloader` should monitor whether the certificate file changed or the certificate file paths changed. When changes occur, the `ConfigReloader` should call `reload` to reload some of the configs that need to refresh. 
