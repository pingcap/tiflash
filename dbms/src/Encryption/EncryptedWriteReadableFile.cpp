#include <Encryption/EncryptedWriteReadableFile.h>


namespace DB
{
ssize_t EncryptedWriteReadableFile::pwrite(char * buf, size_t size, off_t offset) const
{
    stream->encrypt(offset, buf, size);
    return file->pwrite(buf, size, offset);
}

ssize_t EncryptedWriteReadableFile::pread(char * buf, size_t size, off_t offset) const
{
    ssize_t bytes_read = file->pread(buf, size, offset);
    stream->decrypt(offset, buf, bytes_read);
    return bytes_read;
}

} // namespace DB