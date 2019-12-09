#!/usr/bin/env bash

set -e

curl -s https://packagecloud.io/install/repositories/altinity/clickhouse/script.rpm.sh | bash
yum install -y openssl-altinity-devel openssl-altinity-static
yum remove -y openssl-static openssl-devel

# openssl-static
ln -sf /opt/openssl-1.1.0f/lib/libcrypto.a /usr/lib64/libcrypto.a
ln -sf /opt/openssl-1.1.0f/lib/libssl.a /usr/lib64/libssl.a

# openssl-devel
ln -sf /opt/openssl-1.1.0f/include/openssl /usr/include/openssl
ln -sf /opt/openssl-1.1.0f/lib/libcrypto.so /usr/lib64/libcrypto.so
ln -sf /opt/openssl-1.1.0f/lib/libssl.so /usr/lib64/libssl.so
ln -sf /opt/openssl-1.1.0f/lib/pkgconfig/libcrypto.pc /usr/lib64/pkgconfig/libcrypto.pc
ln -sf /opt/openssl-1.1.0f/lib/pkgconfig/libssl.pc /usr/lib64/pkgconfig/libssl.pc
ln -sf /opt/openssl-1.1.0f/lib/pkgconfig/openssl.pc /usr/lib64/pkgconfig/openssl.pc
