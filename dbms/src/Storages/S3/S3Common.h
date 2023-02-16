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

#include <Common/nocopyable.h>
#include <Server/StorageConfigParser.h>
#include <aws/core/Aws.h>
#include <aws/core/http/Scheme.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <common/types.h>

namespace DB::S3
{

class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    void init(const StorageS3Config & config_);
    void shutdown();
    std::unique_ptr<Aws::S3::S3Client> create() const;

    static std::unique_ptr<Aws::S3::S3Client> create(
        const String & endpoint,
        Aws::Http::Scheme scheme,
        bool verifySSL,
        const String & access_key_id,
        const String & secret_access_key);

    static Aws::Http::Scheme parseScheme(std::string_view endpoint);

private:
    ClientFactory() = default;
    DISALLOW_COPY_AND_MOVE(ClientFactory);

    Aws::SDKOptions aws_options;
    StorageS3Config config;
};

struct ObjectInfo
{
    size_t size = 0;
    time_t last_modification_time = 0;
};

bool isNotFoundError(Aws::S3::S3Errors error);

Aws::S3::Model::HeadObjectOutcome headObject(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id = "");

S3::ObjectInfo getObjectInfo(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool throw_on_error);

size_t getObjectSize(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id, bool throw_on_error);

bool objectExists(const Aws::S3::S3Client & client, const String & bucket, const String & key, const String & version_id = "");

void uploadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname);

void downloadFile(const Aws::S3::S3Client & client, const String & bucket, const String & local_fname, const String & remote_fname);

std::unordered_map<String, size_t> listPrefix(const Aws::S3::S3Client & client, const String & bucket, const String & prefix);
} // namespace DB::S3