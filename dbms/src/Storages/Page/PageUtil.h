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

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashException.h>
#include <IO/BaseFile/RateLimiter.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Storages/Page/Page.h>
#include <boost_wrapper/string_split.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <ext/scope_guard.h>

#ifndef __APPLE__
#include <fcntl.h>
#endif


namespace ProfileEvents
{
extern const Event Seek;
extern const Event FileOpen;
extern const Event FileOpenFailed;
extern const Event PSMWritePages;
extern const Event PSMWriteIOCalls;
extern const Event PSMWriteBytes;
extern const Event PSMBackgroundWriteBytes;
extern const Event PSMReadPages;
extern const Event PSMBackgroundReadBytes;
extern const Event PSMReadIOCalls;
extern const Event PSMReadBytes;
extern const Event PSMWriteFailed;
extern const Event PSMReadFailed;
} // namespace ProfileEvents

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_FORMAT_VERSION;
extern const int CHECKSUM_DOESNT_MATCH;
extern const int FILE_DOESNT_EXIST;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_FSYNC;
extern const int CANNOT_FTRUNCATE;
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int FILE_SIZE_NOT_MATCH;
} // namespace ErrorCodes


namespace FailPoints
{
extern const char force_set_page_file_write_errno[];
extern const char force_split_io_size_4k[];
} // namespace FailPoints

namespace PageUtil
{
// https://man7.org/linux/man-pages/man2/write.2.html pwrite() will call the write()
// According to POSIX.1, if count is greater than SSIZE_MAX, the result is implementation-defined;
// SSIZE_MAX usually is (2^31 -1), Use SSIZE_MAX still will failed.
// (2G - 4k) is the best value. The reason for subtracting 4k instead of 1byte is that both vfs and disks need to be aligned.
#define MAX_IO_SIZE ((2ULL * 1024 * 1024 * 1024) - (1024 * 4))

UInt32 randInt(UInt32 min, UInt32 max);

// =========================================================
// Helper functions
// =========================================================

template <bool read, bool must_exist = true>
int openFile(const std::string & path)
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

    int flags;
    if constexpr (read)
    {
        flags = O_RDONLY;
    }
    else
    {
        flags = O_WRONLY | O_CREAT;
    }

    int fd = ::open(path.c_str(), flags, 0666);
    if (-1 == fd)
    {
        ProfileEvents::increment(ProfileEvents::FileOpenFailed);
        if constexpr (!must_exist)
        {
            if (errno == ENOENT)
            {
                return 0;
            }
        }
        DB::throwFromErrno(
            fmt::format("Cannot open file {}. ", path),
            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    }

    return fd;
}

inline void touchFile(const std::string & path)
{
    auto fd = openFile<false>(path);
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForWrite};
    if (fd > 0)
        ::close(fd);
    else
        throw Exception(fmt::format("Touch file failed: {}. ", path));
}

template <typename T>
void syncFile(T & file)
{
    if (-1 == file->fsync())
        DB::throwFromErrno(fmt::format("Cannot fsync file: {}. ", file->getFileName()), ErrorCodes::CANNOT_FSYNC);
}

template <typename T>
void ftruncateFile(T & file, off_t length)
{
    if (-1 == file->ftruncate(length))
        DB::throwFromErrno(
            fmt::format("Cannot truncate file: {}. ", file->getFileName()),
            ErrorCodes::CANNOT_FTRUNCATE);
}

// TODO: split current api into V2 and V3.
// Too many args in this function.
// Also split read
template <typename T>
void writeFile(
    T & file,
    UInt64 offset,
    char * data,
    size_t to_write,
    const WriteLimiterPtr & write_limiter = nullptr,
    const bool background = false,
    const bool truncate_if_failed = true,
    [[maybe_unused]] bool enable_failpoint = false)
{
    if (write_limiter)
        write_limiter->request(to_write);

    size_t bytes_written = 0;
    size_t write_io_calls = 0;
    size_t split_bytes = to_write > MAX_IO_SIZE ? MAX_IO_SIZE : 0;

    fiu_do_on(FailPoints::force_split_io_size_4k, { split_bytes = 4 * 1024; });

    while (bytes_written != to_write)
    {
        write_io_calls += 1;
        ssize_t res = 0;
        {
            size_t bytes_need_write
                = split_bytes == 0 ? (to_write - bytes_written) : std::min(to_write - bytes_written, split_bytes);
            res = file->pwrite(data + bytes_written, bytes_need_write, offset + bytes_written);
#ifndef NDEBUG
            fiu_do_on(FailPoints::force_set_page_file_write_errno, {
                if (enable_failpoint)
                {
                    res = -1;
                    errno = ENOSPC;
                }
            });
#endif
            if ((-1 == res || 0 == res) && errno != EINTR)
            {
                ProfileEvents::increment(ProfileEvents::PSMWriteFailed);
                auto saved_errno = errno;

                int truncate_res = 0;
                // If write failed in V3, Don't do truncate
                if (truncate_if_failed)
                {
                    // If error occurs, apply `ftruncate` try to truncate the broken bytes we have written.
                    // Note that the result of this ftruncate is ignored, there is nothing we can do to
                    // handle ftruncate error. The errno may change after ftruncate called.
                    truncate_res = ::ftruncate(file->getFd(), offset);
                }

                DB::throwFromErrno(
                    fmt::format(
                        "Cannot write to file {},[truncate_res = {}],[errno_after_truncate = {}],"
                        "[bytes_written={},to_write={},offset = {}]",
                        file->getFileName(),
                        truncate_if_failed ? DB::toString(truncate_res) : "no need truncate",
                        strerror(errno),
                        bytes_written,
                        to_write,
                        offset),
                    ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
                    saved_errno);
            }
        }

        if (res > 0)
            bytes_written += res;
    }
    ProfileEvents::increment(ProfileEvents::PSMWriteIOCalls, write_io_calls);
    ProfileEvents::increment(ProfileEvents::PSMWriteBytes, bytes_written);

    if (background)
    {
        ProfileEvents::increment(ProfileEvents::PSMBackgroundWriteBytes, bytes_written);
    }
}

template <typename T>
void readFile(
    T & file,
    const off_t offset,
    const char * buf,
    size_t expected_bytes,
    const ReadLimiterPtr & read_limiter = nullptr,
    const bool background = false)
{
    if (unlikely(expected_bytes == 0))
        return;

    if (read_limiter != nullptr)
    {
        read_limiter->request(expected_bytes);
    }
    size_t bytes_read = 0;
    size_t read_io_calls = 0;
    size_t split_bytes = expected_bytes > MAX_IO_SIZE ? MAX_IO_SIZE : 0;

    fiu_do_on(FailPoints::force_split_io_size_4k, { split_bytes = 4 * 1024; });

    while (bytes_read < expected_bytes)
    {
        read_io_calls += 1;

        ssize_t res = 0;
        {
            size_t bytes_need_read
                = split_bytes == 0 ? (expected_bytes - bytes_read) : std::min(expected_bytes - bytes_read, split_bytes);
            res = file->pread(const_cast<char *>(buf + bytes_read), bytes_need_read, offset + bytes_read);
        }
        if (!res)
            break;

        if (-1 == res && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::PSMReadFailed);
            DB::throwFromErrno(
                fmt::format("Cannot read from file {}.", file->getFileName()),
                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_read += res;
    }
    ProfileEvents::increment(ProfileEvents::PSMReadIOCalls, read_io_calls);
    ProfileEvents::increment(ProfileEvents::PSMReadBytes, bytes_read);
    if (background)
    {
        ProfileEvents::increment(ProfileEvents::PSMBackgroundReadBytes, bytes_read);
    }

    if (unlikely(bytes_read != expected_bytes))
        throw DB::TiFlashException(
            fmt::format(
                "No enough data in file {}, read bytes: {}, expected bytes: {}, offset: {}",
                file->getFileName(),
                bytes_read,
                expected_bytes,
                offset),
            Errors::PageStorage::FileSizeNotMatch);
}

/// Write and advance sizeof(T) bytes.
template <typename T>
inline void put(char *& pos, const T & v)
{
    std::memcpy(pos, reinterpret_cast<const char *>(&v), sizeof(T));
    pos += sizeof(T);
}

/// Read and advance sizeof(T) bytes.
template <typename T, bool advance = true>
inline T get(std::conditional_t<advance, char *&, const char *> pos)
{
    T v;
    std::memcpy(reinterpret_cast<char *>(&v), pos, sizeof(T));
    if constexpr (advance)
        pos += sizeof(T);
    return v;
}

std::vector<size_t> getFieldSizes(const std::set<FieldOffsetInsidePage> & field_offsets, size_t data_size);

} // namespace PageUtil

} // namespace DB
