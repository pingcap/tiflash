#include <IO/EncryptedWritableFile.h>

namespace DB
{

void EncryptedWritableFile::close() { file->close(); }

ssize_t EncryptedWritableFile::write(const char * buf, size_t size) const { return file->write(buf, size); }

} // namespace DB