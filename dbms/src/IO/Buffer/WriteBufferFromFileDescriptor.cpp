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
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <IO/Buffer/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <errno.h>
#include <port/unistd.h>

namespace ProfileEvents
{
extern const Event FileFSync;
extern const Event WriteBufferFromFileDescriptorWrite;
extern const Event WriteBufferFromFileDescriptorWriteBytes;
} // namespace ProfileEvents

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
extern const int CANNOT_FSYNC;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int CANNOT_TRUNCATE_FILE;
extern const int CANNOT_CLOSE_FILE;
} // namespace ErrorCodes


void WriteBufferFromFileDescriptor::nextImpl()
{
    if (offset() == 0)
        return;

    size_t bytes_written = 0;
    while (bytes_written != offset())
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWrite);

        ssize_t res = 0;
        {
            res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);
        }

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            throwFromErrno("Cannot write to file " + getFileName(), ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_written += res;
    }

    ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteBytes, bytes_written);
}


/// Name or some description of file.
std::string WriteBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(fd) + ")";
}


WriteBufferFromFileDescriptor::WriteBufferFromFileDescriptor(
    int fd_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromFileBase(buf_size, existing_memory, alignment)
    , fd(fd_)
{}


WriteBufferFromFileDescriptor::~WriteBufferFromFileDescriptor()
{
    try
    {
        if (fd >= 0)
            next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


off_t WriteBufferFromFileDescriptor::getPositionInFile()
{
    return seek(0, SEEK_CUR);
}

void WriteBufferFromFileDescriptor::close()
{
    next();

    if (0 != ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    fd = -1;
}

void WriteBufferFromFileDescriptor::sync()
{
    /// If buffer has pending data - write it.
    next();

    /// Request OS to sync data with storage medium.
    ProfileEvents::increment(ProfileEvents::FileFSync);
    Stopwatch sw;
    SCOPE_EXIT({ GET_METRIC(tiflash_system_seconds, type_fsync).Observe(sw.elapsedSeconds()); });
    int res = fsync(fd);
    if (-1 == res)
        throwFromErrno("Cannot fsync " + getFileName(), ErrorCodes::CANNOT_FSYNC);
}


off_t WriteBufferFromFileDescriptor::doSeek(off_t offset, int whence)
{
    off_t res = lseek(fd, offset, whence);
    if (-1 == res)
        throwFromErrno("Cannot seek through file " + getFileName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    return res;
}


void WriteBufferFromFileDescriptor::doTruncate(off_t length)
{
    int res = ftruncate(fd, length);
    if (-1 == res)
        throwFromErrno("Cannot truncate file " + getFileName(), ErrorCodes::CANNOT_TRUNCATE_FILE);
}

} // namespace DB
