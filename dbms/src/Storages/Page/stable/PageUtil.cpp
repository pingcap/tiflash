#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashException.h>
#include <IO/WriteHelpers.h>

#include <boost/algorithm/string/classification.hpp>

#ifndef __APPLE__
#include <fcntl.h>
#endif

#include <Storages/Page/stable/PageUtil.h>

#include <ext/scope_guard.h>

namespace DB::stable::PageUtil
{

void syncFile(int fd, const std::string & path)
{
    if (-1 == ::fsync(fd))
        DB::throwFromErrno("Cannot fsync " + path, ErrorCodes::CANNOT_FSYNC);
}

void writeFile(int fd, UInt64 offset, const char * data, size_t to_write, const std::string & path)
{
    ProfileEvents::increment(ProfileEvents::PSMWriteCalls);
    ProfileEvents::increment(ProfileEvents::PSMWriteBytes, to_write);

    size_t bytes_written = 0;
    while (bytes_written != to_write)
    {
        ProfileEvents::increment(ProfileEvents::PSMWriteIOCalls);
        ssize_t res = 0;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Write};
            res = ::pwrite(fd, data + bytes_written, to_write - bytes_written, offset + bytes_written);
        }

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::PSMWriteFailed);
            DB::throwFromErrno("Cannot write to file " + path, ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_written += res;
    }
}


void readFile(int fd, const off_t offset, const char * buf, size_t expected_bytes, const std::string & path)
{
    if (unlikely(expected_bytes == 0))
        return;

    ProfileEvents::increment(ProfileEvents::PSMReadCalls);

    size_t bytes_read = 0;
    while (bytes_read < expected_bytes)
    {
        ProfileEvents::increment(ProfileEvents::PSMReadIOCalls);

        ssize_t res = 0;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};
            res = ::pread(fd, const_cast<char *>(buf + bytes_read), expected_bytes - bytes_read, offset + bytes_read);
        }
        if (!res)
            break;

        if (-1 == res && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::PSMReadFailed);
            DB::throwFromErrno("Cannot read from file " + path, ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_read += res;
    }
    ProfileEvents::increment(ProfileEvents::PSMReadBytes, bytes_read);

    if (unlikely(bytes_read != expected_bytes))
        throw DB::TiFlashException("Not enough data in file " + path, TiFlashErrorRegistry::simpleGet(ErrorClass::PageStorage, "FileSizeNotMatch"));
}

} // namespace DB::stable::PageUtil
