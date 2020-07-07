#include <IO/EncryptedWritableFile.h>

namespace DB
{

void EncryptedWritableFile::close() { file->close(); }

ssize_t EncryptedWritableFile::write(char * buf, size_t size)
{
    stream->encrypt(file_offset, buf, size);
    file_offset += size;
    return file->write(buf, size);
}

} // namespace DB
