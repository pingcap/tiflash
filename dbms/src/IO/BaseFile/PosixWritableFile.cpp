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
#include <IO/BaseFile/PosixWritableFile.h>
#include <fcntl.h>
#include <unistd.h>

namespace ProfileEvents
{
extern const Event FileOpen;
extern const Event FileOpenFailed;
extern const Event FileFSync;
} // namespace ProfileEvents

namespace DB
{
namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_CLOSE_FILE;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

PosixWritableFile::PosixWritableFile(
    const std::string & file_name_,
    bool truncate_when_exists_,
    int flags,
    mode_t mode,
    const WriteLimiterPtr & write_limiter_)
    : file_name{file_name_}
    , write_limiter{write_limiter_}
{
    doOpenFile(truncate_when_exists_, flags, mode);
}

PosixWritableFile::~PosixWritableFile()
{
    metric_increment.destroy();
    if (fd < 0)
        return;

    ::close(fd);
}

void PosixWritableFile::open()
{
    if (fd != -1)
        return;
    // The mode is only valid when creating the file, the `0666` is ignored.
    doOpenFile(/*truncate_when_exists=*/false, -1, 0666);
}

void PosixWritableFile::close()
{
    if (fd < 0)
        return;
    while (0 != ::close(fd))
        if (errno != EINTR)
            throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);

    metric_increment.changeTo(0); // Subtract metrics for `CurrentMetrics::OpenFileForWrite`

    fd = -1;
}

ssize_t PosixWritableFile::write(char * buf, size_t size)
{
    if (write_limiter)
        write_limiter->request(size);
    return ::write(fd, buf, size);
}

ssize_t PosixWritableFile::pwrite(char * buf, size_t size, off_t offset) const
{
    if (write_limiter)
        write_limiter->request(size);
    return ::pwrite(fd, buf, size, offset);
}

off_t PosixWritableFile::seek(off_t offset, int whence) const
{
    return ::lseek(fd, offset, whence);
}

void PosixWritableFile::doOpenFile(bool truncate_when_exists_, int flags, mode_t mode)
{
    ProfileEvents::increment(ProfileEvents::FileOpen);
#ifdef __APPLE__
    bool o_direct = (flags != -1) && (flags & O_DIRECT);
    if (o_direct)
        flags = flags & ~O_DIRECT;
#endif

    if (flags == -1)
    {
        if (truncate_when_exists_)
            flags = O_WRONLY | O_TRUNC | O_CREAT;
        else
            flags = O_WRONLY | O_CREAT;
    }

    fd = ::open(file_name.c_str(), flags, mode);

    if (-1 == fd)
    {
        ProfileEvents::increment(ProfileEvents::FileOpenFailed);
        throwFromErrno(
            "Cannot open file " + file_name,
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    }

    metric_increment.changeTo(1); // Add metrics for `CurrentMetrics::OpenFileForWrite`

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

int PosixWritableFile::fsync()
{
    ProfileEvents::increment(ProfileEvents::FileFSync);
    Stopwatch sw;
    SCOPE_EXIT({ GET_METRIC(tiflash_system_seconds, type_fsync).Observe(sw.elapsedSeconds()); });
    return ::fsync(fd);
}

int PosixWritableFile::ftruncate(off_t length)
{
    return ::ftruncate(fd, length);
}

void PosixWritableFile::hardLink(const std::string & existing_file)
{
    if (existing_file.empty())
    {
        throw Exception("Failed to create hard link for empty file name", ErrorCodes::LOGICAL_ERROR);
    }

    if (file_name.empty())
    {
        throw Exception(
            "Failed to create hard link for:" + existing_file + " to an empty path",
            ErrorCodes::LOGICAL_ERROR);
    }

    close();
    int rc = ::remove(file_name.c_str());
    if (rc != 0)
    {
        throwFromErrno("Can't remove file : " + file_name);
    }

    // The link() function shall create a new link (directory entry) for the existing file, `existing_file`. The second path argument
    // points to a pathname naming the new directory entry to be created. The link() function shall atomically create a new link
    // for the existing file and the link count of the file shall be incremented by one.
    // Reference: https://linux.die.net/man/3/link
    rc = ::link(existing_file.c_str(), file_name.c_str());
    if (rc != 0)
    {
        throw Exception(
            "Failed to create hard link for:" + existing_file + " to an empty path",
            ErrorCodes::LOGICAL_ERROR);
    }
}

} // namespace DB
