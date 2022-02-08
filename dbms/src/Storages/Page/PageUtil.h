#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashException.h>
#include <Encryption/FileProvider.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <random>

#ifndef __APPLE__
#include <fcntl.h>
#endif
#include <Encryption/RandomAccessFile.h>
#include <Encryption/RateLimiter.h>
#include <Encryption/WritableFile.h>
#include <Encryption/WriteReadableFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>

#include <ext/scope_guard.h>


namespace ProfileEvents
{
extern const Event Seek;
extern const Event FileOpen;
extern const Event FileOpenFailed;
extern const Event PSMWritePages;
extern const Event PSMWriteIOCalls;
extern const Event PSMWriteBytes;
extern const Event PSMReadPages;
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
        DB::throwFromErrno("Cannot open file " + path, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
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
        throw Exception("Touch file failed: " + path);
}

template <typename T>
void syncFile(T & file)
{
    if (-1 == file->fsync())
        DB::throwFromErrno("Cannot fsync file: " + file->getFileName(), ErrorCodes::CANNOT_FSYNC);
}

template <typename T>
void ftruncateFile(T & file, off_t length)
{
    if (-1 == file->ftruncate(length))
        DB::throwFromErrno("Cannot truncate file: " + file->getFileName(), ErrorCodes::CANNOT_FTRUNCATE);
}


template <typename T>
void writeFile(
    T & file,
    UInt64 offset,
    char * data,
    size_t to_write,
    const WriteLimiterPtr & write_limiter = nullptr,
    [[maybe_unused]] bool enable_failpoint = false)
{
    ProfileEvents::increment(ProfileEvents::PSMWriteBytes, to_write);

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
            size_t bytes_need_write = split_bytes == 0 ? (to_write - bytes_written) : std::min(to_write - bytes_written, split_bytes);
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
                // If error occurs, apply `ftruncate` try to truncate the broken bytes we have written.
                // Note that the result of this ftruncate is ignored, there is nothing we can do to
                // handle ftruncate error. The errno may change after ftruncate called.
                int truncate_res = ::ftruncate(file->getFd(), offset);

                DB::throwFromErrno(fmt::format("Cannot write to file {},[truncate_res = {}],[errno_after_truncate = {}],"
                                               "[bytes_written={},to_write={},offset = {}]",
                                               file->getFileName(),
                                               truncate_res,
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
}

template <typename T>
void readFile(T & file,
              const off_t offset,
              const char * buf,
              size_t expected_bytes,
              const ReadLimiterPtr & read_limiter = nullptr)
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
            size_t bytes_need_read = split_bytes == 0 ? (expected_bytes - bytes_read) : std::min(expected_bytes - bytes_read, split_bytes);
            res = file->pread(const_cast<char *>(buf + bytes_read), bytes_need_read, offset + bytes_read);
        }
        if (!res)
            break;

        if (-1 == res && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::PSMReadFailed);
            DB::throwFromErrno("Cannot read from file " + file->getFileName(), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_read += res;
    }
    ProfileEvents::increment(ProfileEvents::PSMReadIOCalls, read_io_calls);
    ProfileEvents::increment(ProfileEvents::PSMReadBytes, bytes_read);

    if (unlikely(bytes_read != expected_bytes))
        throw DB::TiFlashException("Not enough data in file " + file->getFileName(), Errors::PageStorage::FileSizeNotMatch);
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

} // namespace PageUtil

} // namespace DB
