#include <IO/PosixFileProvider.h>
#include <IO/PosixRandomAccessFile.h>
#include <IO/PosixWritableFile.h>

namespace DB
{
RandomAccessFilePtr PosixFileProvider::newRandomAccessFileImpl(const std::string & file_name_, int flags)
{
    return std::make_shared<PosixRandomAccessFile>(file_name_, flags);
}

WritableFilePtr PosixFileProvider::newWritableFileImpl(const std::string & file_name_, int flags, mode_t mode)
{
    return std::make_shared<PosixWritableFile>(file_name_, flags, mode);
}
} // namespace DB
