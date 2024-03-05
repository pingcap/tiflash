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

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/TiFlashMetrics.h>
#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <IO/BaseFile/RateLimiter.h>
#include <fcntl.h>
#include <unistd.h>

namespace ProfileEvents
{
extern const Event FileOpen;
extern const Event FileOpenFailed;
} // namespace ProfileEvents

namespace DB
{
namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_CLOSE_FILE;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int CANNOT_SELECT;
} // namespace ErrorCodes

RandomAccessFilePtr PosixRandomAccessFile::create(const String & file_name_)
{
    return std::make_shared<PosixRandomAccessFile>(file_name_, /*flags*/ -1, /*read_limiter_*/ nullptr);
}

PosixRandomAccessFile::PosixRandomAccessFile(
    const std::string & file_name_,
    int flags,
    const ReadLimiterPtr & read_limiter_,
    const FileSegmentPtr & file_seg_)
    : file_name{file_name_}
    , read_limiter(read_limiter_)
    , file_seg(file_seg_)
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

#ifdef __APPLE__
    bool o_direct = (flags != -1) && (flags & O_DIRECT);
    if (o_direct)
        flags = flags & ~O_DIRECT;
#endif
    fd = open(file_name.c_str(), flags == -1 ? O_RDONLY : flags);

    if (-1 == fd)
    {
        ProfileEvents::increment(ProfileEvents::FileOpenFailed);
        throwFromErrno(
            "Cannot open file " + file_name,
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    }
#ifdef __APPLE__
    if (o_direct)
    {
        if (fcntl(fd, F_NOCACHE, 1) == -1)
        {
            ProfileEvents::increment(ProfileEvents::FileOpenFailed);
            throwFromErrno("Cannot set F_NOCACHE on file " + file_name, ErrorCodes::CANNOT_OPEN_FILE);
        }
    }
#endif
}

PosixRandomAccessFile::~PosixRandomAccessFile()
{
    if (fd < 0)
        return;

    ::close(fd);
}

void PosixRandomAccessFile::close()
{
    if (fd < 0)
        return;
    while (::close(fd) != 0)
        if (errno != EINTR)
            throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);

    fd = -1;
    metric_increment.destroy();
}

off_t PosixRandomAccessFile::seek(off_t offset, int whence)
{
    return ::lseek(fd, offset, whence);
}

ssize_t PosixRandomAccessFile::read(char * buf, size_t size)
{
    if (read_limiter != nullptr)
    {
        read_limiter->request(size);
    }
    if (file_seg != nullptr)
    {
        GET_METRIC(tiflash_storage_remote_cache_bytes, type_dtfile_read_bytes).Increment(size);
    }
    return ::read(fd, buf, size);
}

ssize_t PosixRandomAccessFile::pread(char * buf, size_t size, off_t offset) const
{
    if (read_limiter != nullptr)
    {
        read_limiter->request(size);
    }
    if (file_seg != nullptr)
    {
        GET_METRIC(tiflash_storage_remote_cache_bytes, type_dtfile_read_bytes).Increment(size);
    }
    return ::pread(fd, buf, size, offset);
}

} // namespace DB
