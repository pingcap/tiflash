#include <IO/EncryptedFileProvider.h>
#include <IO/EncryptedRandomAccessFile.h>
#include <IO/EncryptedWritableFile.h>
#include <IO/PosixRandomAccessFile.h>
#include <IO/PosixWritableFile.h>
#include <Poco/File.h>

namespace DB
{
RandomAccessFilePtr EncryptedFileProvider::newRandomAccessFileImpl(
    const std::string & file_path_, const EncryptionPath & encryption_path_, int flags) const
{
    RandomAccessFilePtr underlying = std::make_shared<PosixRandomAccessFile>(file_path_, flags);
    return std::make_shared<EncryptedRandomAccessFile>(underlying, encryption_provider->createCipherStream(encryption_path_, false));
}

WritableFilePtr EncryptedFileProvider::newWritableFileImpl(const std::string & file_path_, const EncryptionPath & encryption_path_,
    bool create_new_file_, bool create_new_encryption_info_, int flags, mode_t mode) const
{
    WritableFilePtr underlying = std::make_shared<PosixWritableFile>(file_path_, create_new_file_, flags, mode);
    return std::make_shared<EncryptedWritableFile>(
        underlying, encryption_provider->createCipherStream(encryption_path_, create_new_encryption_info_));
}

void EncryptedFileProvider::deleteFile(const std::string & file_path_, const EncryptionPath & encryption_path_) const
{
    Poco::File data_file(file_path_);
    if (data_file.exists())
    {
        data_file.remove();
    }
    key_manager->deleteFile(encryption_path_.dir_name);
}

void EncryptedFileProvider::createEncryptionInfo(const std::string & file_path_) const { key_manager->newFile(file_path_); }
} // namespace DB
