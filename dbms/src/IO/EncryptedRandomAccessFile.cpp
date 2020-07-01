#include <Common/Exception.h>
#include <IO/EncryptedRandomAccessFile.h>
#include <fcntl.h>
#include <unistd.h>

namespace DB
{

void EncryptedRandomAccessFile::close() { file->close(); }

off_t EncryptedRandomAccessFile::seek(off_t offset, int whence) const { return file->seek(offset, whence); }

ssize_t EncryptedRandomAccessFile::read(char * buf, size_t size) const
{
    ssize_t bytes_read = file->read(buf, size);
    // TODO: decrypt data in buf
    return bytes_read;
}

ssize_t EncryptedRandomAccessFile::pread(char * buf, size_t size, off_t offset) const
{
    ssize_t bytes_read = file->pread(buf, size, offset);
    // TODO: decrypt data in buf
    return bytes_read;
}

} // namespace DB
