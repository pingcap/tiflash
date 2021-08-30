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

namespace ProfileEvents
{
extern const Event FileFSync;
} // namespace ProfileEvents

namespace DB::stable::PageUtil
{
void syncFile(WritableFilePtr & file)
{
    if (-1 == file->fsync())
        DB::throwFromErrno("Cannot fsync file: " + file->getFileName(), ErrorCodes::CANNOT_FSYNC);
}

void writeFile(WritableFilePtr & file, UInt64 offset, char * data, size_t to_write)
{
    ProfileEvents::increment(ProfileEvents::PSMWriteBytes, to_write);

    size_t bytes_written = 0;
    size_t write_io_calls = 0;
    while (bytes_written != to_write)
    {
        write_io_calls += 1;
        ssize_t res = 0;
        {
            res = file->pwrite(data + bytes_written, to_write - bytes_written, offset + bytes_written);
        }

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::PSMWriteFailed);
            DB::throwFromErrno("Cannot write to file " + file->getFileName(), ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_written += res;
    }
    ProfileEvents::increment(ProfileEvents::PSMWriteIOCalls, write_io_calls);
}


void readFile(RandomAccessFilePtr & file, const off_t offset, const char * buf, size_t expected_bytes)
{
    if (unlikely(expected_bytes == 0))
        return;

    size_t bytes_read = 0;
    size_t read_io_calls = 0;
    while (bytes_read < expected_bytes)
    {
        read_io_calls += 1;

        ssize_t res = 0;
        {
            res = file->pread(const_cast<char *>(buf + bytes_read), expected_bytes - bytes_read, offset + bytes_read);
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

} // namespace DB::stable::PageUtil
