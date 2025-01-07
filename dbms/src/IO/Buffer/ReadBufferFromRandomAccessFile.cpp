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

#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <IO/BaseFile/RandomAccessFile.h>
#include <IO/Buffer/ReadBufferFromRandomAccessFile.h>

namespace ProfileEvents
{
extern const Event ReadBufferFromFileDescriptorRead;
extern const Event ReadBufferFromFileDescriptorReadFailed;
extern const Event ReadBufferFromFileDescriptorReadBytes;
} // namespace ProfileEvents

namespace CurrentMetrics
{
extern const Metric OpenFileForRead;
}

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int CANNOT_SELECT;
} // namespace ErrorCodes

ReadBufferFromRandomAccessFile::ReadBufferFromRandomAccessFile(
    RandomAccessFilePtr file_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : ReadBufferFromFileDescriptor(file_->getFd(), buf_size, existing_memory, alignment)
    , file(std::move(file_))
{}

bool ReadBufferFromRandomAccessFile::nextImpl()
{
    size_t bytes_read = 0;
    while (!bytes_read)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorRead);

        ssize_t res = 0;
        {
            res = file->read(internal_buffer.begin(), internal_buffer.size());
        }
        if (!res)
            break;

        if (-1 == res && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
            throwFromErrno("Cannot read from file " + getFileName(), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_read += res;
    }

    pos_in_file += bytes_read;

    if (bytes_read)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, bytes_read);
        working_buffer.resize(bytes_read);
    }
    else
        return false;

    return true;
}

ReadBufferFromRandomAccessFile::~ReadBufferFromRandomAccessFile()
{
    if (file->isClosed())
        return;

    file->close();
}

std::string ReadBufferFromRandomAccessFile::getFileName() const
{
    return file->getFileName();
}

std::string ReadBufferFromRandomAccessFile::getInitialFileName() const
{
    return file->getInitialFileName();
}

int ReadBufferFromRandomAccessFile::getFD() const
{
    return file->getFd();
}

off_t ReadBufferFromRandomAccessFile::doSeekInFile(off_t offset, int whence)
{
    return file->seek(offset, whence);
}

} // namespace DB
