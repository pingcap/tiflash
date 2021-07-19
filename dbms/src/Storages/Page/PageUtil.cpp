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
    WritableFilePtr & file, UInt64 offset, char * data, size_t to_write, const WriteLimiterPtr & write_limiter, bool enable_failpoint)
#else
void writeFile(WritableFilePtr & file, UInt64 offset, char * data, size_t to_write, const WriteLimiterPtr & write_limiter)
#endif
{
    ProfileEvents::increment(ProfileEvents::PSMWriteCalls);
    ProfileEvents::increment(ProfileEvents::PSMWriteBytes, to_write);

    if (write_limiter)
        write_limiter->request(to_write);
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
#ifdef FIU_ENABLE
        // Can inject failpoint under debug mode
        fiu_do_on(FailPoints::force_set_page_file_write_errno, {
            if (enable_failpoint)
            {
                res   = -1;
                errno = ENOSPC;
            }
        });
#else
        (void)(enable_failpoint); // unused parameter
#endif
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


void readFile(RandomAccessFilePtr & file, const off_t offset, const char * buf, size_t expected_bytes, const ReadLimiterPtr & read_limiter)
{
    if (unlikely(expected_bytes == 0))
        return;

    ProfileEvents::increment(ProfileEvents::PSMReadCalls);

    if (read_limiter != nullptr)
    {
        read_limiter->request(expected_bytes);
    }
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

void readChecksumFramedFile(
    RandomAccessFilePtr & file, off_t offset, char * buf, size_t expected_bytes, DM::DMConfiguration & configuration)
{
    DM::FrameUnion headerStorage;
    auto           digest = configuration.createUnifiedDigest();

    auto  frameNo    = offset / configuration.getChecksumFrameLength();
    auto  frameShift = offset % configuration.getChecksumFrameLength();
    off_t realOffset = static_cast<off_t>(frameNo) * static_cast<off_t>(configuration.getChecksumFrameLength() + digest->headerSize());

    auto loadHeader = [&]() -> size_t {
        readFile(file, realOffset, reinterpret_cast<char *>(&headerStorage), digest->headerSize());
        realOffset = realOffset + static_cast<off_t>(digest->headerSize());
        return *reinterpret_cast<size_t *>(&headerStorage);
    };

    auto examineChecksum = [&](const char * data, size_t size) {
        digest->update(data, size);
        if (unlikely(!digest->compare_frame(headerStorage)))
        {
            throw DB::Exception("Data corruption detected! checksum mismatch in file " + file->getFileName());
        }
        digest->reset();
    };

    if (frameShift)
    {
        // in this case, we allocate memory for the first frame on ourselves and then examine the checksum
        auto tmpBuffer = reinterpret_cast<char *>(::operator new (configuration.getChecksumFrameLength(), std::align_val_t{512}));

        // read frame
        auto frameSize = loadHeader();
        readFile(file, realOffset, tmpBuffer, frameSize);

        // examine checksum
        examineChecksum(tmpBuffer, frameSize);

        // transfer data
        auto bytesToCopy = std::min(frameSize - frameShift, expected_bytes);
        std::memcpy(buf, tmpBuffer + frameShift, bytesToCopy);

        // update statistics
        realOffset     = realOffset + static_cast<off_t>(frameSize); // we are now at frame end
        expected_bytes = expected_bytes - bytesToCopy;
        buf            = buf + bytesToCopy;
        assert(realOffset % (configuration.getChecksumFrameLength() + digest->headerSize()) == 0
               || frameSize < configuration.getChecksumFrameLength());

        // clean up
        ::operator delete (tmpBuffer, std::align_val_t{512});
    }

    auto frameSize = expected_bytes ? loadHeader() : 0;
    while (expected_bytes && expected_bytes >= frameSize)
    {
        // read body
        readFile(file, realOffset, buf, frameSize);

        // examine checksum
        examineChecksum(buf, frameSize);

        // update for next turn
        realOffset     = realOffset + static_cast<off_t>(frameSize);
        expected_bytes = expected_bytes - frameSize;
        buf            = buf + frameSize;
        frameSize      = expected_bytes ? loadHeader() : 0;
    }

    if (expected_bytes)
    {
        // there are still some bytes in current frame. However, the frame is larger than the remaining buffer,
        // so we have to read the data to a temporal buffer.
        auto tmpBuffer = reinterpret_cast<char *>(::operator new (frameSize, std::align_val_t{512}));

        // read body
        readFile(file, realOffset, tmpBuffer, frameSize);

        // examine checksum
        examineChecksum(tmpBuffer, frameSize);

        // transfer data
        std::memcpy(buf, tmpBuffer, expected_bytes);
        ::operator delete (tmpBuffer, std::align_val_t{512});
    }
}

} // namespace PageUtil
} // namespace DB
