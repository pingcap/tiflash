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
#include <IO/BaseFile/WritableFile.h>
#include <IO/Buffer/WriteBufferFromWritableFile.h>


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
} // namespace ErrorCodes

WriteBufferFromWritableFile::WriteBufferFromWritableFile(
    WritableFilePtr file_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromFileDescriptor(file_->getFd(), buf_size, existing_memory, alignment)
    , file(std::move(file_))
{}

void WriteBufferFromWritableFile::nextImpl()
{
    if (offset() == 0)
        return;

    size_t bytes_written = 0;
    while (bytes_written != offset())
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWrite);

        ssize_t res = 0;
        {
            res = file->write(working_buffer.begin() + bytes_written, offset() - bytes_written);
        }

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            throwFromErrno(
                "Cannot write to file " + getFileName(),
                ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR); // NOLINT
        }

        if (res > 0)
            bytes_written += res;
    }

    ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteBytes, bytes_written);
}

WriteBufferFromWritableFile::~WriteBufferFromWritableFile()
{
    if (file->isClosed())
        return;

    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    file->close();
}

void WriteBufferFromWritableFile::sync()
{
    next();

    int res = file->fsync();
    if (-1 == res)
        throwFromErrno("Cannot fsync " + getFileName(), ErrorCodes::CANNOT_FSYNC);
}

std::string WriteBufferFromWritableFile::getFileName() const
{
    return file->getFileName();
}

int WriteBufferFromWritableFile::getFD() const
{
    return file->getFd();
}

void WriteBufferFromWritableFile::close()
{
    file->close();
}

off_t WriteBufferFromWritableFile::doSeek(off_t offset, int whence)
{
    off_t res = file->seek(offset, whence);
    if (-1 == res)
        throwFromErrno("Cannot seek through file " + getFileName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    return res;
}

void WriteBufferFromWritableFile::doTruncate(off_t length)
{
    int res = file->ftruncate(length);
    if (-1 == res)
        throwFromErrno("Cannot truncate file " + getFileName(), ErrorCodes::CANNOT_TRUNCATE_FILE);
}
} // namespace DB
