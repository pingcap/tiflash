#include <unistd.h>
#include <fcntl.h>
#include <Common/Exception.h>
#include <IO/PosixRandomAccessFile.h>

namespace DB {

namespace ErrorCodes {
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_SELECT;
}

PosixRandomAccessFile::PosixRandomAccessFile(const std::string &file_name_, int flags)
        : file_name{file_name_} {
#ifdef __APPLE__
    bool o_direct = (flags != -1) && (flags & O_DIRECT);
    if (o_direct)
        flags = flags & ~O_DIRECT;
#endif
    fd = open(file_name.c_str(), flags == -1 ? O_RDONLY : flags);

    if (-1 == fd) {
        throwFromErrno("Cannot open file " + file_name,
                       errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    }
#ifdef __APPLE__
    if (o_direct) {
        if (fcntl(fd, F_NOCACHE, 1) == -1) {
            throwFromErrno("Cannot set F_NOCACHE on file " + file_name, ErrorCodes::CANNOT_OPEN_FILE);
        }
    }
#endif
}

PosixRandomAccessFile::~PosixRandomAccessFile() {
    if (fd < 0)
        return;

    ::close(fd);
}

void PosixRandomAccessFile::close() {
    if (0 != ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    fd = -1;
}

ssize_t PosixRandomAccessFile::read(char *buf, size_t size) const {
    size_t bytes_read = 0;
    size_t bytes_left = size;
    while (bytes_left > 0)
    {
        ssize_t res = 0;
        {
            res = ::read(fd, buf, bytes_left);
        }
        if (!res)
            break;

        if (-1 == res && errno != EINTR)
        {
            throwFromErrno("Cannot read from file " + getFileName(), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        bytes_read += res;
        bytes_left -= res;
    }
    return bytes_read;
}

ssize_t PosixRandomAccessFile::pread(char *buf, size_t size, off_t offset) const {
    return ::pread(fd, buf, size, offset);
}

}
