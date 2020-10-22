#include <Encryption/EncryptedRandomAccessFile.h>
#include <fcntl.h>
#include <unistd.h>

namespace DB
{

void EncryptedRandomAccessFile::close() { file->close(); }

off_t EncryptedRandomAccessFile::seek(off_t offset, int whence)
{
    file_offset = file->seek(offset, whence);
    return file_offset;
}

ssize_t EncryptedRandomAccessFile::read(char * buf, size_t size)
{
    ssize_t bytes_read = file->read(buf, size);
    stream->decrypt(file_offset, buf, bytes_read);
    file_offset += bytes_read;
    return bytes_read;
}

ssize_t EncryptedRandomAccessFile::pread(char * buf, size_t size, off_t offset) const
{
    ssize_t bytes_read = file->pread(buf, size, offset);
    stream->decrypt(offset, buf, bytes_read);
    return bytes_read;
}

} // namespace DB
