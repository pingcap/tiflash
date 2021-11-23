#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Encryption/PosixWriteReadableFile.h>
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
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

PosixWriteReadableFile::PosixWriteReadableFile(const String & file_name_,
                                               bool truncate_when_exists_,
                                               int flags,
                                               mode_t mode,
                                               const WriteLimiterPtr & write_limiter_,
                                               const ReadLimiterPtr & read_limiter_)
    : file_name{file_name_}
    , write_limiter{write_limiter_}
    , read_limiter{read_limiter_}
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
            flags = O_RDWR | O_TRUNC | O_CREAT;
        else
            flags = O_RDWR | O_CREAT;
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

PosixWriteReadableFile::~PosixWriteReadableFile()
{
    metric_increment.destroy();
    if (fd < 0)
        return;

    ::close(fd);
}


void PosixWriteReadableFile::close()
{
    if (fd < 0)
    {
        return;
    }

    while (0 != ::close(fd))
    {
        if (errno != EINTR)
        {
            throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);
        }
    }


    metric_increment.changeTo(0); // Subtract metrics for `CurrentMetrics::OpenFileForWrite`

    fd = -1;
    metric_increment.destroy();
}

ssize_t PosixWriteReadableFile::pwrite(char * buf, size_t size, off_t offset) const
{
    if (write_limiter)
    {
        write_limiter->request(size);
    }

    return ::pwrite(fd, buf, size, offset);
}

ssize_t PosixWriteReadableFile::pread(char * buf, size_t size, off_t offset) const
{
    if (read_limiter != nullptr)
    {
        read_limiter->request(size);
    }
    return ::pread(fd, buf, size, offset);
}

int PosixWriteReadableFile::fsync()
{
    ProfileEvents::increment(ProfileEvents::FileFSync);
    return ::fsync(fd);
}

} // namespace DB