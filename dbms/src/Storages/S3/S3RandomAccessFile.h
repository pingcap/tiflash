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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Encryption/RandomAccessFile.h>
#include <aws/s3/model/GetObjectResult.h>
#include <common/types.h>

namespace Aws::S3
{
class S3Client;
}

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB::S3
{
class S3RandomAccessFile final : public RandomAccessFile
{
public:
    static RandomAccessFilePtr create(const String & remote_fname);

    S3RandomAccessFile(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & remote_fname_);

    off_t seek(off_t offset, int whence) override;

    ssize_t read(char * buf, size_t size) override;

    std::string getFileName() const override
    {
        return fmt::format("{}/{}", bucket, remote_fname);
    }

    ssize_t pread(char * /*buf*/, size_t /*size*/, off_t /*offset*/) const override
    {
        throw Exception("S3RandomAccessFile not support pread", ErrorCodes::NOT_IMPLEMENTED);
    }

    int getFd() const override
    {
        return -1;
    }

    bool isClosed() const override
    {
        return is_close;
    }

    void close() override
    {
        is_close = true;
    }

private:
    void initialize();

    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    String bucket;
    String remote_fname;

    Aws::S3::Model::GetObjectResult read_result;

    DB::LoggerPtr log;
    bool is_close = false;
};

} // namespace DB::S3
