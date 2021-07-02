#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashException.h>
#include <Encryption/FileProvider.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#ifndef __APPLE__
#include <fcntl.h>
#endif

#include <Encryption/RandomAccessFile.h>
#include <Encryption/WritableFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <Storages/Page/PageFile.h>

#include <ext/scope_guard.h>

namespace ProfileEvents
{
extern const Event FileOpen;
extern const Event FileOpenFailed;
extern const Event Seek;
extern const Event PSMWritePages;
extern const Event PSMWriteCalls;
extern const Event PSMWriteIOCalls;
extern const Event PSMWriteBytes;
extern const Event PSMReadPages;
extern const Event PSMReadCalls;
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
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int FILE_SIZE_NOT_MATCH;
} // namespace ErrorCodes

namespace PageUtil
{
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
    auto                      fd = openFile<false>(path);
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForWrite};
    if (fd > 0)
        ::close(fd);
    else
        throw Exception("Touch file failed: " + path);
}

void syncFile(WritableFilePtr & file);

#ifndef NDEBUG
void writeFile(
    WritableFilePtr & file, UInt64 offset, char * data, size_t to_write, const RateLimiterPtr & rate_limiter, bool enable_failpoint);
#else
void writeFile(WritableFilePtr & file, UInt64 offset, char * data, size_t to_write, const RateLimiterPtr & rate_limiter);
#endif

void readFile(RandomAccessFilePtr & file, const off_t offset, const char * buf, size_t expected_bytes);

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

/// Read and advance sizeof(T) bytes by read buffer.
/// Return false if there are no enough data.
/// It may throw an exception, if something is wrong when invoking ReadBuffer::next
/// How to deal with the case get throws exception in moveNext(), which happens more often than other case?
template <typename T, bool advance = true>
inline bool get(const ReadBufferPtr & read_buffer, T * const result)
{
    if constexpr (!advance)
    {
        if (unlikely(read_buffer->remain() < sizeof(T)))
            return false;

        std::memcpy(reinterpret_cast<char *>(result), read_buffer->position(), sizeof(T));
        return true;
    }

    size_t read_bytes = read_buffer->read(reinterpret_cast<char *>(result), sizeof(T));
    return read_bytes == sizeof(T);
}

// template <typename T, bool advance = true>
// inline std::tuple<T, bool> get(const ReadBufferPtr & read_buffer)
// {
//     T v;
//     if constexpr (!advance) {
//         if (unlikely(read_buffer->remain() < sizeof(T)))
//             return std::make_tuple(v, false);

//         std::memcpy(reinterpret_cast<char *>(&v), read_buffer->position(), sizeof(T));
//         return std::make_tuple(v, true);
//     }

//     size_t bytes_need_read_remain = sizeof(T);

//     do {
//         if (unlikely(!read_buffer->hasPendingData())) {
//             if (unlikely(!read_buffer->next())) {
//                 // if next return false, bad case, had better log it, because you only read partial data and lose the working buffer.
//                 return std::make_tuple(v, false);
//             }
//         }
//         size_t bytes_to_read = std::min(read_buffer->remain(), bytes_need_read_remain);
//         std::memcpy(reinterpret_cast<char *>(&v) + sizeof(T) - bytes_need_read_remain, read_buffer->position(), bytes_to_read);
//         read_buffer->position() += bytes_to_read;
//         bytes_need_read_remain -= bytes_to_read;
//     }
//     while (unlikely(bytes_need_read_remain > 0))
//     return std::make_tuple(v, true);
// }
} // namespace PageUtil

} // namespace DB
