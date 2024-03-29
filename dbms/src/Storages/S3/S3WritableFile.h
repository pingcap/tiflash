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

#include <Common/Exception.h>
#include <IO/BaseFile/WritableFile.h>
#include <Storages/S3/S3Common.h>
#include <common/types.h>

namespace Aws::S3
{
class S3Client;
}

namespace Aws::S3::Model
{
class UploadPartRequest;
class PutObjectRequest;
} // namespace Aws::S3::Model

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB::S3
{
struct WriteSettings
{
    size_t upload_part_size = 16 * 1024 * 1024;
    size_t max_single_part_upload_size = 32 * 1024 * 1024;
    bool check_objects_after_upload = false;
    size_t max_unexpected_write_error_retries = 4;
};

class S3WritableFile final : public WritableFile
{
public:
    static std::shared_ptr<S3WritableFile> create(const String & remote_fname_);

    S3WritableFile(
        std::shared_ptr<TiFlashS3Client> client_ptr_,
        const String & remote_fname_,
        const WriteSettings & write_settings_);

    ~S3WritableFile() override;

    ssize_t write(char * buf, size_t size) override;

    // To ensure that the data is uploaded to S3, the caller must call fsync after all write is finished.
    int fsync() override;

    std::string getFileName() const override { return fmt::format("{}/{}", client_ptr->bucket(), remote_fname); }

    void close() override { is_close = true; }

    bool isClosed() const override { return is_close; }

    ssize_t pwrite(char * /*buf*/, size_t /*size*/, off_t /*offset*/) const override
    {
        throw Exception("S3WritableFile not support pwrite", ErrorCodes::NOT_IMPLEMENTED);
    }

    off_t seek(off_t /*offset*/, int /*whence*/) const override
    {
        throw Exception("S3WritableFile not support seek", ErrorCodes::NOT_IMPLEMENTED);
    }

    int getFd() const override { return -1; }

    void open() override { throw Exception("S3WritableFile not support open", ErrorCodes::NOT_IMPLEMENTED); }

    int ftruncate(off_t /*length*/) override
    {
        throw Exception("S3WritatbleFile not support ftruncate", ErrorCodes::NOT_IMPLEMENTED);
    }

    void hardLink(const std::string & /*existing_file*/) override
    {
        throw Exception("S3WritableFile not support hardLink", ErrorCodes::NOT_IMPLEMENTED);
    }

    // `getUploadInfo` is used for test.
    struct UploadInfo
    {
        size_t part_number;
        String multipart_upload_id;
        std::vector<String> part_tags;
        size_t total_write_bytes;
    };
    UploadInfo getUploadInfo() const
    {
        return UploadInfo{part_number, multipart_upload_id, part_tags, total_write_bytes};
    }

private:
    void allocateBuffer();

    void createMultipartUpload();
    void writePart();
    void completeMultipartUpload();

    void makeSinglepartUpload();

    void finalize();

    struct UploadPartTask;
    void fillUploadRequest(Aws::S3::Model::UploadPartRequest & req);
    void processUploadRequest(UploadPartTask & task);

    struct PutObjectTask;
    void fillPutRequest(Aws::S3::Model::PutObjectRequest & req);
    void processPutRequest(const PutObjectTask & task);

    template <typename T>
    void checkS3Outcome(const T & outcome)
    {
        if (!outcome.IsSuccess())
        {
            throw S3::fromS3Error(
                outcome.GetError(),
                "bucket={} root={} key={}",
                client_ptr->bucket(),
                client_ptr->root(),
                remote_fname);
        }
    }

    const String remote_fname;
    const std::shared_ptr<TiFlashS3Client> client_ptr;
    const WriteSettings write_settings;

    std::shared_ptr<Aws::StringStream> temporary_buffer; // Buffer to accumulate data.
    size_t last_part_size = 0;
    size_t part_number = 0;
    UInt64 total_write_bytes = 0;

    // Upload in S3 is made in parts.
    String multipart_upload_id;
    std::vector<String> part_tags;

    LoggerPtr log;

    bool is_close = false;
};

} // namespace DB::S3
