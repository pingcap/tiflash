#include <Common/Exception.h>
#include <IO/PosixWritableFile.h>
#include <fcntl.h>
#include <unistd.h>


namespace DB
{

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_CLOSE_FILE;
} // namespace ErrorCodes

PosixWritableFile::PosixWritableFile(const std::string & file_name_, int flags, mode_t mode) : file_name{file_name_}
{
#ifdef __APPLE__
    bool o_direct = (flags != -1) && (flags & O_DIRECT);
    if (o_direct)
        flags = flags & ~O_DIRECT;
#endif

    fd = open(file_name.c_str(), flags == -1 ? O_WRONLY | O_TRUNC | O_CREAT : flags, mode);

    if (-1 == fd)
    {
        throwFromErrno("Cannot open file " + file_name, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    }

#ifdef __APPLE__
    if (o_direct)
    {
        if (fcntl(fd, F_NOCACHE, 1) == -1)
        {
            throwFromErrno("Cannot set F_NOCACHE on file " + file_name, ErrorCodes::CANNOT_OPEN_FILE);
        }
    }
#endif
}

PosixWritableFile::~PosixWritableFile()
{
    if (fd < 0)
        return;

    ::close(fd);
}

void PosixWritableFile::close()
{
    if (0 != ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    fd = -1;
}

ssize_t PosixWritableFile::write(char * buf, size_t size) { return ::write(fd, buf, size); }

ssize_t PosixWritableFile::pwrite(char *buf, size_t size, off_t offset) const {
    return ::pwrite(fd, buf, size, offset);
}

} // namespace DB
