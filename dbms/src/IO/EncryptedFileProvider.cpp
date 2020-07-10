#include <IO/EncryptedFileProvider.h>
#include <IO/EncryptedRandomAccessFile.h>
#include <IO/EncryptedWritableFile.h>
#include <IO/PosixRandomAccessFile.h>
#include <IO/PosixWritableFile.h>

namespace DB
{
RandomAccessFilePtr EncryptedFileProvider::newRandomAccessFileImpl(const std::string & file_name_, int flags)
{
    RandomAccessFilePtr underlying = std::make_shared<PosixRandomAccessFile>(file_name_, flags);
    return std::make_shared<EncryptedRandomAccessFile>(underlying, encryption_provider->createCipherStream(file_name_, false));
}

WritableFilePtr EncryptedFileProvider::newWritableFileImpl(const std::string & file_name_, int flags, mode_t mode)
{
    WritableFilePtr underlying = std::make_shared<PosixWritableFile>(file_name_, flags, mode);
    return std::make_shared<EncryptedWritableFile>(underlying, encryption_provider->createCipherStream(file_name_, true));
}
} // namespace DB
