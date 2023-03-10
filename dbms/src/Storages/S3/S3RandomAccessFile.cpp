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

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <aws/s3/model/GetObjectRequest.h>

namespace ProfileEvents
{
extern const Event S3GetObject;
extern const Event S3ReadBytes;
} // namespace ProfileEvents

namespace DB::S3
{
S3RandomAccessFile::S3RandomAccessFile(
    std::shared_ptr<Aws::S3::S3Client> client_ptr_,
    const String & bucket_,
    const String & remote_fname_)
    : client_ptr(std::move(client_ptr_))
    , bucket(bucket_)
    , remote_fname(remote_fname_)
    , log(Logger::get("S3RandomAccessFile"))
{
    initialize();
}

ssize_t S3RandomAccessFile::read(char * buf, size_t size)
{
    auto & istr = read_result.GetBody();
    istr.read(buf, size);
    size_t gcount = istr.gcount();
    if (gcount == 0 && !istr.eof())
    {
        LOG_ERROR(log, "Cannot read from istream. bucket={} key={}", bucket, remote_fname);
        return -1;
    }
    ProfileEvents::increment(ProfileEvents::S3ReadBytes, gcount);
    return gcount;
}

off_t S3RandomAccessFile::seek(off_t offset_, int whence)
{
    if (unlikely(whence != SEEK_SET))
    {
        LOG_ERROR(log, "Only SEEK_SET mode is allowed, but {} is received", whence);
        return -1;
    }
    if (unlikely(offset_ < 0))
    {
        LOG_ERROR(log, "Seek position is out of bounds. Offset: {}", offset_);
        return -1;
    }
    auto & istr = read_result.GetBody();
    istr.seekg(offset_);
    return istr.tellg();
}

void S3RandomAccessFile::initialize()
{
    Stopwatch sw;
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(remote_fname);
    ProfileEvents::increment(ProfileEvents::S3GetObject);
    auto outcome = client_ptr->GetObject(req);
    if (!outcome.IsSuccess())
    {
        throw S3::fromS3Error(outcome.GetError(), "bucket={} key={}", bucket, remote_fname);
    }
    ProfileEvents::increment(ProfileEvents::S3ReadBytes, outcome.GetResult().GetContentLength());
    GET_METRIC(tiflash_storage_s3_request_seconds, type_get_object).Observe(sw.elapsedSeconds());
    read_result = outcome.GetResultWithOwnership();
}

RandomAccessFilePtr S3RandomAccessFile::create(const String & remote_fname)
{
    auto & ins = S3::ClientFactory::instance();
    return std::make_shared<S3RandomAccessFile>(ins.sharedClient(), ins.bucket(), remote_fname);
}
} // namespace DB::S3
