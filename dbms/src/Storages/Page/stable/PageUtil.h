#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashException.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#ifndef __APPLE__
#include <fcntl.h>
#endif

#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <Storages/Page/stable/PageFile.h>

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

namespace CurrentMetrics
{
extern const Metric Write;
extern const Metric Read;
} // namespace CurrentMetrics

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

namespace stable
{

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
    auto fd = openFile<false>(path);
    if (fd > 0)
        ::close(fd);
    else
        throw Exception("Touch file failed: " + path);
}

void syncFile(int fd, const std::string & path);

void writeFile(int fd, UInt64 offset, const char * data, size_t to_write, const std::string & path);

void readFile(int fd, const off_t offset, const char * buf, size_t expected_bytes, const std::string & path);

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

template <typename C, typename T = typename C::value_type>
std::unique_ptr<C> readValuesFromFile(const std::string & path, Allocator<false> & allocator)
{
    Poco::File file(path);
    if (!file.exists())
        return {};

    size_t file_size = file.getSize();
    int    file_fd   = openFile<true>(path);
    SCOPE_EXIT({ ::close(file_fd); });
    char * data = (char *)allocator.alloc(file_size);
    SCOPE_EXIT({ allocator.free(data, file_size); });
    char * pos = data;

    readFile(file_fd, 0, data, file_size, path);

    auto               size   = get<UInt64>(pos);
    std::unique_ptr<C> values = std::make_unique<C>();
    for (size_t i = 0; i < size; ++i)
    {
        T v = get<T>(pos);
        values->push_back(v);
    }

    if (unlikely(pos != data + file_size))
        throw DB::TiFlashException("pos not match", TiFlashErrorRegistry::simpleGet(ErrorClass::PageStorage, "FileSizeNotMatch"));

    return values;
}

} // namespace PageUtil

} // namespace stable
} // namespace DB
