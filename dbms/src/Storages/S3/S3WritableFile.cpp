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

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3WritableFile.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include <ext/scope_guard.h>
#include <magic_enum.hpp>

namespace ProfileEvents
{
extern const Event S3WriteBytes;
extern const Event S3CreateMultipartUpload;
extern const Event S3UploadPart;
extern const Event S3CompleteMultipartUpload;
extern const Event S3PutObject;
} // namespace ProfileEvents

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
}

namespace DB::S3
{
struct S3WritableFile::UploadPartTask
{
    Aws::S3::Model::UploadPartRequest req;
    bool is_finished = false;
    std::string tag;
    std::exception_ptr exception;
};

struct S3WritableFile::PutObjectTask
{
    Aws::S3::Model::PutObjectRequest req;
    bool is_finished = false;
    std::exception_ptr exception;
};

S3WritableFile::S3WritableFile(
    std::shared_ptr<TiFlashS3Client> client_ptr_,
    const String & remote_fname_,
    const WriteSettings & write_settings_)
    : remote_fname(remote_fname_)
    , client_ptr(std::move(client_ptr_))
    , write_settings(write_settings_)
    , log(Logger::get("S3WritableFile"))
{
    allocateBuffer();
}

S3WritableFile::~S3WritableFile() = default;

ssize_t S3WritableFile::write(char * buf, size_t size)
{
    temporary_buffer->write(buf, size);
    if (!temporary_buffer->good())
    {
        LOG_ERROR(
            log,
            "write size={} failed: bucket={} root={} key={}",
            size,
            client_ptr->bucket(),
            client_ptr->root(),
            remote_fname);
        return -1;
    }
    ProfileEvents::increment(ProfileEvents::S3WriteBytes, size);
    last_part_size += size;
    total_write_bytes += size;

    // Data size exceeds singlepart upload threshold, need to use multipart upload.
    if (multipart_upload_id.empty() && last_part_size > write_settings.max_single_part_upload_size)
    {
        createMultipartUpload();
    }

    if (!multipart_upload_id.empty() && last_part_size > write_settings.upload_part_size)
    {
        writePart();
        allocateBuffer();
    }
    return size;
}

void S3WritableFile::allocateBuffer()
{
    temporary_buffer = Aws::MakeShared<Aws::StringStream>("temporary buffer");
    last_part_size = 0;
}

int S3WritableFile::fsync()
{
    if (multipart_upload_id.empty())
    {
        makeSinglepartUpload();
    }
    else
    {
        // Write rest of the data as last part.
        writePart();
    }
    finalize();
    return 0;
}

void S3WritableFile::finalize()
{
    if (!multipart_upload_id.empty())
    {
        completeMultipartUpload();
    }
    if (write_settings.check_objects_after_upload)
    {
        // TODO(jinhe): check checksums.
        auto resp = S3::headObject(*client_ptr, remote_fname);
        checkS3Outcome(resp);
    }
}

void S3WritableFile::createMultipartUpload()
{
    Stopwatch sw;
    SCOPE_EXIT({
        GET_METRIC(tiflash_storage_s3_request_seconds, type_create_multi_part_upload).Observe(sw.elapsedSeconds());
    });
    Aws::S3::Model::CreateMultipartUploadRequest req;
    client_ptr->setBucketAndKeyWithRoot(req, remote_fname);
    req.SetContentType("binary/octet-stream");
    ProfileEvents::increment(ProfileEvents::S3CreateMultipartUpload);
    auto outcome = client_ptr->CreateMultipartUpload(req);
    checkS3Outcome(outcome);
    multipart_upload_id = outcome.GetResult().GetUploadId();
}

void S3WritableFile::writePart()
{
    auto size = temporary_buffer->tellp();
    if (size < 0)
    {
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Buffer is in bad state. bucket={} root={} key={}",
            client_ptr->bucket(),
            client_ptr->root(),
            remote_fname);
    }
    if (size == 0)
    {
        LOG_DEBUG(log, "Skipping writing part. Buffer is empty.");
        return;
    }

    UploadPartTask task;
    fillUploadRequest(task.req);
    processUploadRequest(task);
    part_tags.push_back(task.tag);
}

void S3WritableFile::fillUploadRequest(Aws::S3::Model::UploadPartRequest & req)
{
    // Increase part number.
    ++part_number;
    // Setup request.
    client_ptr->setBucketAndKeyWithRoot(req, remote_fname);
    req.SetPartNumber(static_cast<int>(part_number));
    req.SetUploadId(multipart_upload_id);
    req.SetContentLength(temporary_buffer->tellp());
    req.SetBody(temporary_buffer);
    req.SetContentType("binary/octet-stream");
}

void S3WritableFile::processUploadRequest(UploadPartTask & task)
{
    Stopwatch sw;
    SCOPE_EXIT({ GET_METRIC(tiflash_storage_s3_request_seconds, type_upload_part).Observe(sw.elapsedSeconds()); });
    ProfileEvents::increment(ProfileEvents::S3UploadPart);
    auto outcome = client_ptr->UploadPart(task.req);
    checkS3Outcome(outcome);
    task.tag = outcome.GetResult().GetETag();
}

void S3WritableFile::completeMultipartUpload()
{
    RUNTIME_CHECK_MSG(
        !part_tags.empty(),
        "Failed to complete multipart upload. No parts have uploaded. bucket={} root={} key={}",
        client_ptr->bucket(),
        client_ptr->root(),
        remote_fname);

    Aws::S3::Model::CompleteMultipartUploadRequest req;
    client_ptr->setBucketAndKeyWithRoot(req, remote_fname);
    req.SetUploadId(multipart_upload_id);
    Aws::S3::Model::CompletedMultipartUpload multipart_upload;
    for (size_t i = 0; i < part_tags.size(); ++i)
    {
        Aws::S3::Model::CompletedPart part;
        multipart_upload.AddParts(part.WithETag(part_tags[i]).WithPartNumber(static_cast<int>(i + 1)));
    }
    req.SetMultipartUpload(multipart_upload);

    size_t max_retry = std::max(write_settings.max_unexpected_write_error_retries, 1UL);
    for (size_t i = 0; i < max_retry; ++i)
    {
        Stopwatch sw;
        SCOPE_EXIT({
            GET_METRIC(tiflash_storage_s3_request_seconds, type_complete_multi_part_upload)
                .Observe(sw.elapsedSeconds());
        });
        ProfileEvents::increment(ProfileEvents::S3CompleteMultipartUpload);
        auto outcome = client_ptr->CompleteMultipartUpload(req);
        if (outcome.IsSuccess())
        {
            LOG_DEBUG(
                log,
                "Multipart upload has completed. bucket={} root={} key={} upload_id={} parts={}",
                client_ptr->bucket(),
                client_ptr->root(),
                remote_fname,
                multipart_upload_id,
                part_tags.size());
            break;
        }
        if (i + 1 < max_retry)
        {
            const auto & e = outcome.GetError();
            LOG_INFO(
                log,
                "Multipart upload failed and need retry: bucket={} root={} key={} upload_id={} parts={} error={} "
                "message={} request_id={}",
                client_ptr->bucket(),
                client_ptr->root(),
                remote_fname,
                multipart_upload_id,
                part_tags.size(),
                magic_enum::enum_name(e.GetErrorType()),
                e.GetMessage(),
                e.GetRequestId());
        }
        else
        {
            throw fromS3Error(
                outcome.GetError(),
                "S3 CompleteMultipartUpload failed, bucket={} root={} key={} upload_id={} parts={}",
                client_ptr->bucket(),
                client_ptr->root(),
                remote_fname,
                multipart_upload_id,
                part_tags.size());
        }
    }
}

void S3WritableFile::makeSinglepartUpload()
{
    auto size = temporary_buffer->tellp();
    if (size < 0)
    {
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Buffer is in bad state. bucket={} root={} key={}",
            client_ptr->bucket(),
            client_ptr->root(),
            remote_fname);
    }
    PutObjectTask task;
    fillPutRequest(task.req);
    processPutRequest(task);
}

void S3WritableFile::fillPutRequest(Aws::S3::Model::PutObjectRequest & req)
{
    client_ptr->setBucketAndKeyWithRoot(req, remote_fname);
    req.SetContentLength(temporary_buffer->tellp());
    req.SetBody(temporary_buffer);
    req.SetContentType("binary/octet-stream");
}

void S3WritableFile::processPutRequest(const PutObjectTask & task)
{
    size_t max_retry = std::max(write_settings.max_unexpected_write_error_retries, 1UL);
    for (size_t i = 0; i < max_retry; ++i)
    {
        Stopwatch sw;
        SCOPE_EXIT({ GET_METRIC(tiflash_storage_s3_request_seconds, type_put_object).Observe(sw.elapsedSeconds()); });
        ProfileEvents::increment(ProfileEvents::S3PutObject);
        auto outcome = client_ptr->PutObject(task.req);
        if (outcome.IsSuccess())
        {
            LOG_DEBUG(
                log,
                "Single part upload has completed. bucket={} root={} key={}, size={}",
                client_ptr->bucket(),
                client_ptr->root(),
                remote_fname,
                task.req.GetContentLength());
            break;
        }
        if (i + 1 < max_retry)
        {
            const auto & e = outcome.GetError();
            LOG_INFO(
                log,
                "Single part upload failed: bucket={} root={} key={} error={} message={} request_id={}",
                client_ptr->bucket(),
                client_ptr->root(),
                remote_fname,
                magic_enum::enum_name(e.GetErrorType()),
                e.GetMessage(),
                e.GetRequestId());
        }
        else
        {
            throw fromS3Error(
                outcome.GetError(),
                "S3 PutObject failed, bucket={} root={} key={}",
                client_ptr->bucket(),
                client_ptr->root(),
                remote_fname);
        }
    }
}

std::shared_ptr<S3WritableFile> S3WritableFile::create(const String & remote_fname_)
{
    return std::make_shared<S3WritableFile>(
        S3::ClientFactory::instance().sharedTiFlashClient(),
        remote_fname_,
        WriteSettings{});
}
} // namespace DB::S3
