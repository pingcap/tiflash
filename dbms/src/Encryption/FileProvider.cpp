#include <Common/TiFlashException.h>
#include <Encryption/EncryptedRandomAccessFile.h>
#include <Encryption/EncryptedWritableFile.h>
#include <Encryption/FileProvider.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <Encryption/PosixWritableFile.h>
#include <Poco/File.h>
#include <common/likely.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int DATA_ENCRYPTION_ERROR;
} // namespace ErrorCodes

RandomAccessFilePtr FileProvider::newRandomAccessFile(const String & file_path_, const EncryptionPath & encryption_path_, int flags) const
{
    RandomAccessFilePtr file = std::make_shared<PosixRandomAccessFile>(file_path_, flags);
    auto encryption_info = key_manager->getFile(encryption_path_.dir_name);
    if (encryption_info.res != FileEncryptionRes::Disabled && encryption_info.method != EncryptionMethod::Plaintext)
    {
        file = std::make_shared<EncryptedRandomAccessFile>(file, AESCTRCipherStream::createCipherStream(encryption_info, encryption_path_));
    }
    return file;
}

WritableFilePtr FileProvider::newWritableFile(const String & file_path_, const EncryptionPath & encryption_path_, bool create_new_file_,
    bool create_new_encryption_info_, int flags, mode_t mode) const
{
    WritableFilePtr file = std::make_shared<PosixWritableFile>(file_path_, create_new_file_, flags, mode);
    if (encryption_enabled)
    {
        if (create_new_encryption_info_)
        {
            auto encryption_info = key_manager->newFile(encryption_path_.dir_name);
            file = std::make_shared<EncryptedWritableFile>(file, AESCTRCipherStream::createCipherStream(encryption_info, encryption_path_));
        }
        else
        {
            auto encryption_info = key_manager->getFile(encryption_path_.dir_name);
            if (unlikely(encryption_info.method == EncryptionMethod::Plaintext))
            {
                throw DB::TiFlashException(
                    "Cannot get encryption info for file: " + encryption_path_.dir_name, Errors::Encryption::Internal);
            }
            file = std::make_shared<EncryptedWritableFile>(file, AESCTRCipherStream::createCipherStream(encryption_info, encryption_path_));
        }
    }
    return file;
}

void FileProvider::deleteFile(const String & file_path_, const EncryptionPath & encryption_path_) const
{
    Poco::File data_file(file_path_);
    if (data_file.exists())
    {
        data_file.remove();
    }
    key_manager->deleteFile(encryption_path_.dir_name);
}

void FileProvider::createEncryptionInfo(const EncryptionPath & encryption_path_) const
{
    if (encryption_enabled)
    {
        key_manager->newFile(encryption_path_.dir_name);
    }
}

void FileProvider::deleteEncryptionInfo(const EncryptionPath & encryption_path_) const
{
    key_manager->deleteFile(encryption_path_.dir_name);
}

bool FileProvider::isFileEncrypted(const EncryptionPath & encryption_path_) const
{
    auto encryption_info = key_manager->getFile(encryption_path_.dir_name);
    return (encryption_info.res != FileEncryptionRes::Disabled) && (encryption_info.method != EncryptionMethod::Plaintext);
}

bool FileProvider::isEncryptionEnabled() const { return encryption_enabled; }

} // namespace DB
