#include <fcntl.h>
#include <unistd.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <IO/PosixWritableFile.h>

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

PosixWritableFile::PosixWritableFile(const std::string & file_name_, bool create_new_file_, int flags, mode_t mode) : file_name{file_name_}
{
    doOpenFile(create_new_file_, flags, mode);
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
    doOpenFile(false, -1, 0666);
}

void PosixWritableFile::close()
{
    if (0 != ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    fd = -1;
}

ssize_t PosixWritableFile::write(char * buf, size_t size) { return ::write(fd, buf, size); }

ssize_t PosixWritableFile::pwrite(char * buf, size_t size, off_t offset) const { return ::pwrite(fd, buf, size, offset); }

void PosixWritableFile::doOpenFile(bool create_new_file_, int flags, mode_t mode)
{
    ProfileEvents::increment(ProfileEvents::FileOpen);
#ifdef __APPLE__
    bool o_direct = (flags != -1) && (flags & O_DIRECT);
    if (o_direct)
        flags = flags & ~O_DIRECT;
#endif

    if (flags == -1)
    {
        if (create_new_file_)
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

int PosixWritableFile::fsync() {
    ProfileEvents::increment(ProfileEvents::FileFSync);
    return ::fsync(fd);
}

} // namespace DB
