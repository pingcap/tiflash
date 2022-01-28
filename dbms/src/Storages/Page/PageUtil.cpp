#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashException.h>
#include <Encryption/RateLimiter.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageUtil.h>

#include <boost/algorithm/string/classification.hpp>

#ifndef __APPLE__
#include <fcntl.h>
#endif

#include <ext/scope_guard.h>

namespace ProfileEvents
{
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
namespace FailPoints
{
extern const char force_set_page_file_write_errno[];
} // namespace FailPoints

namespace PageUtil
{

void syncFile(WritableFilePtr & file)
{
    if (-1 == file->fsync())
        DB::throwFromErrno("Cannot fsync file: " + file->getFileName(), ErrorCodes::CANNOT_FSYNC);
}

#ifndef NDEBUG
void writeFile(
    WritableFilePtr & file, UInt64 offset, char * data, size_t to_write, const RateLimiterPtr & rate_limiter, bool enable_failpoint [[ maybe_unused ]])
#else
void writeFile(WritableFilePtr & file, UInt64 offset, char * data, size_t to_write, const RateLimiterPtr & rate_limiter)
#endif
{
    ProfileEvents::increment(ProfileEvents::PSMWriteCalls);
    ProfileEvents::increment(ProfileEvents::PSMWriteBytes, to_write);

    if (rate_limiter)
        rate_limiter->request(to_write);
    size_t bytes_written = 0;
    while (bytes_written != to_write)
    {
        ProfileEvents::increment(ProfileEvents::PSMWriteIOCalls);
        ssize_t res = 0;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Write};
            res = file->pwrite(data + bytes_written, to_write - bytes_written, offset + bytes_written);
        }

#ifndef NDEBUG
        // Can inject failpoint under debug mode
        fiu_do_on(FailPoints::force_set_page_file_write_errno, {
            if (enable_failpoint)
            {
                res   = -1;
                errno = ENOSPC;
            }
        });
#endif
        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::PSMWriteFailed);
            auto saved_errno = errno; // save errno before `ftruncate`
            // If error occurs, apply `ftruncate` try to truncate the broken bytes we have written.
            // Note that the result of this ftruncate is ignored, there is nothing we can do to
            // handle ftruncate error. The errno may change after ftruncate called.
            int truncate_res = ::ftruncate(file->getFd(), offset);
            DB::throwFromErrno("Cannot write to file " + file->getFileName() + " [truncate_res=" + DB::toString(truncate_res)
                                   + "] [errno_after_truncate=" + strerror(errno) + "]",
                               ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
                               saved_errno);
        }

        if (res > 0)
            bytes_written += res;
    }
}


void readFile(RandomAccessFilePtr & file, const off_t offset, const char * buf, size_t expected_bytes)
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
    ProfileEvents::increment(ProfileEvents::PSMReadBytes, bytes_read);

    if (unlikely(bytes_read != expected_bytes))
        throw DB::TiFlashException("Not enough data in file " + file->getFileName(), Errors::PageStorage::FileSizeNotMatch);
}

} // namespace PageUtil
} // namespace DB
