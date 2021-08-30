#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <errno.h>
#include <port/unistd.h>


namespace ProfileEvents
{
extern const Event FileFSync;
extern const Event WriteBufferFromFileDescriptorWrite;
extern const Event WriteBufferFromFileDescriptorWriteFailed;
extern const Event WriteBufferFromFileDescriptorWriteBytes;
} // namespace ProfileEvents

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
extern const int CANNOT_FSYNC;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int CANNOT_TRUNCATE_FILE;
} // namespace ErrorCodes


void WriteBufferFromFileDescriptor::nextImpl()
{
    if (!offset())
        return;

    size_t bytes_written = 0;
    while (bytes_written != offset())
    {
        ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWrite);

        ssize_t res = 0;
        {
            res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);
        }

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteFailed);
            throwFromErrno("Cannot write to file " + getFileName(), ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_written += res;
    }

    ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteBytes, bytes_written);
}


/// Name or some description of file.
std::string WriteBufferFromFileDescriptor::getFileName() const
{
    return "(fd = " + toString(fd) + ")";
}


WriteBufferFromFileDescriptor::WriteBufferFromFileDescriptor(
    int fd_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : WriteBufferFromFileBase(buf_size, existing_memory, alignment)
    , fd(fd_)
{}


WriteBufferFromFileDescriptor::~WriteBufferFromFileDescriptor()
{
    try
    {
        if (fd >= 0)
            next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


off_t WriteBufferFromFileDescriptor::getPositionInFile()
{
    return seek(0, SEEK_CUR);
}


void WriteBufferFromFileDescriptor::sync()
{
    /// If buffer has pending data - write it.
    next();

    /// Request OS to sync data with storage medium.
    ProfileEvents::increment(ProfileEvents::FileFSync);
    int res = fsync(fd);
    if (-1 == res)
        throwFromErrno("Cannot fsync " + getFileName(), ErrorCodes::CANNOT_FSYNC);
}


off_t WriteBufferFromFileDescriptor::doSeek(off_t offset, int whence)
{
    off_t res = lseek(fd, offset, whence);
    if (-1 == res)
        throwFromErrno("Cannot seek through file " + getFileName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    return res;
}


void WriteBufferFromFileDescriptor::doTruncate(off_t length)
{
    int res = ftruncate(fd, length);
    if (-1 == res)
        throwFromErrno("Cannot truncate file " + getFileName(), ErrorCodes::CANNOT_TRUNCATE_FILE);
}

} // namespace DB
