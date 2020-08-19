#include <Encryption/EncryptedWritableFile.h>

namespace DB
{

void EncryptedWritableFile::open() { file->open(); }

void EncryptedWritableFile::close() { file->close(); }

ssize_t EncryptedWritableFile::write(char * buf, size_t size)
{
    stream->encrypt(file_offset, buf, size);
    file_offset += size;
    return file->write(buf, size);
}

ssize_t EncryptedWritableFile::pwrite(char * buf, size_t size, off_t offset) const
{
    stream->encrypt(offset, buf, size);
    return file->pwrite(buf, size, offset);
}

} // namespace DB
