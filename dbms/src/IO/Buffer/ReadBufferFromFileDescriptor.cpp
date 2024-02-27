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
#include <IO/Buffer/ReadBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <errno.h>
#include <time.h>

#include <optional>


namespace ProfileEvents
{
extern const Event ReadBufferFromFileDescriptorRead;
extern const Event ReadBufferFromFileDescriptorReadFailed;
extern const Event ReadBufferFromFileDescriptorReadBytes;
extern const Event Seek;
} // namespace ProfileEvents

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int CANNOT_SELECT;
} // namespace ErrorCodes


std::string ReadBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(fd) + ")";
}


bool ReadBufferFromFileDescriptor::nextImpl()
{
    size_t bytes_read = 0;
    while (!bytes_read)
    {
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorRead);

        std::optional<Stopwatch> watch;
        if (profile_callback)
            watch.emplace(clock_type);

        ssize_t res = 0;
        {
            res = ::read(fd, internal_buffer.begin(), internal_buffer.size());
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

        if (profile_callback)
        {
            ProfileInfo info; // NOLINT
            info.bytes_requested = internal_buffer.size();
            info.bytes_read = res;
            info.nanoseconds = watch->elapsed();
            profile_callback(info);
        }
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


/// If 'offset' is small enough to stay in buffer after seek, then true seek in file does not happen.
off_t ReadBufferFromFileDescriptor::doSeek(off_t offset, int whence)
{
    off_t new_pos = offset;
    if (whence == SEEK_CUR)
        new_pos = pos_in_file - (working_buffer.end() - pos) + offset;
    else if (whence != SEEK_SET)
        throw Exception(
            "ReadBufferFromFileDescriptor::seek expects SEEK_SET or SEEK_CUR as whence",
            ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    /// Position is unchanged.
    if (new_pos + (working_buffer.end() - pos) == pos_in_file)
        return new_pos;

    if (hasPendingData() && new_pos <= pos_in_file
        && new_pos >= pos_in_file - static_cast<off_t>(working_buffer.size()))
    {
        /// Position is still inside buffer.
        pos = working_buffer.begin() + (new_pos - (pos_in_file - working_buffer.size()));
        return new_pos;
    }
    else
    {
        pos = working_buffer.end();
        off_t res = doSeekInFile(new_pos, SEEK_SET);
        if (-1 == res)
            throwFromErrno("Cannot seek through file " + getFileName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
        pos_in_file = new_pos;
        return res;
    }
}

off_t ReadBufferFromFileDescriptor::doSeekInFile(off_t offset, int whence)
{
    return ::lseek(fd, offset, whence);
}


/// Assuming file descriptor supports 'select', check that we have data to read or wait until timeout.
bool ReadBufferFromFileDescriptor::poll(size_t timeout_microseconds) const
{
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(fd, &fds);
    timeval timeout
        = {static_cast<time_t>(timeout_microseconds / 1000000),
           static_cast<suseconds_t>(timeout_microseconds % 1000000)};

    int res = select(1, &fds, nullptr, nullptr, &timeout);

    if (-1 == res)
        throwFromErrno("Cannot select", ErrorCodes::CANNOT_SELECT);

    return res > 0;
}

} // namespace DB
