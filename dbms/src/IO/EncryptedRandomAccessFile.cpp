#include <Common/Exception.h>
#include <IO/EncryptedRandomAccessFile.h>
#include <fcntl.h>
#include <unistd.h>

namespace DB
{

void EncryptedRandomAccessFile::close() { file->close(); }

ssize_t EncryptedRandomAccessFile::read(char * buf, size_t size) const
{
    ssize_t bytes_read = file->read(buf, size);
    // decrypt data in buf
    return bytes_read;
}

ssize_t EncryptedRandomAccessFile::pread(char * buf, size_t size, off_t offset) const
{
    ssize_t bytes_read = file->pread(buf, size, offset);
    // decrypt data in buf
    return bytes_read;
}

} // namespace DB