#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Encryption/PosixWritableFile.h>
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
} // namespace ErrorCodes

PosixWritableFile::PosixWritableFile(
    const std::string & file_name_, bool truncate_when_exists_, int flags, mode_t mode, const RateLimiterPtr & rate_limiter_)
    : file_name{file_name_}, rate_limiter{rate_limiter_}
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
    if (0 != ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    metric_increment.changeTo(0); // Subtract metrics for `CurrentMetrics::OpenFileForWrite`

    fd = -1;
}

ssize_t PosixWritableFile::write(char * buf, size_t size)
{
    if (rate_limiter)
        rate_limiter->request((UInt64)size);
    return ::write(fd, buf, size);
}

ssize_t PosixWritableFile::pwrite(char * buf, size_t size, off_t offset) const
{
    if (rate_limiter)
        rate_limiter->request((UInt64)size);
    return ::pwrite(fd, buf, size, offset);
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
        throwFromErrno("Cannot open file " + file_name, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
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
    return ::fsync(fd);
}

} // namespace DB
