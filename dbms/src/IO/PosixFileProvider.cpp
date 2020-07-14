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

void PosixFileProvider::renameFile(const std::string & src_fname, const std::string & dst_fname)
{
    std::ignore = src_fname;
    std::ignore = dst_fname;
}
} // namespace DB
