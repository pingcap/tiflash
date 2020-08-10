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
    auto encryption_info = key_manager->getFile(encryption_path_.full_path);
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
            auto encryption_info = key_manager->newFile(encryption_path_.full_path);
            file = std::make_shared<EncryptedWritableFile>(file, AESCTRCipherStream::createCipherStream(encryption_info, encryption_path_));
        }
        else
        {
            auto encryption_info = key_manager->getFile(encryption_path_.full_path);
            if (unlikely(encryption_info.method == EncryptionMethod::Plaintext))
            {
                throw DB::TiFlashException(
                    "Cannot get encryption info for file: " + encryption_path_.full_path, Errors::Encryption::Internal);
            }
            file = std::make_shared<EncryptedWritableFile>(file, AESCTRCipherStream::createCipherStream(encryption_info, encryption_path_));
        }
    }
    return file;
}

void FileProvider::deleteFile(const String & file_path_, const EncryptionPath & encryption_path_, bool recursive) const
{
    Poco::File data_file(file_path_);
    if (data_file.exists())
    {
        data_file.remove(recursive);
    }
    key_manager->deleteFile(encryption_path_.full_path);
}

void FileProvider::createEncryptionInfo(const EncryptionPath & encryption_path_) const
{
    if (encryption_enabled)
    {
        key_manager->newFile(encryption_path_.full_path);
    }
}

void FileProvider::deleteEncryptionInfo(const EncryptionPath & encryption_path_) const
{
    key_manager->deleteFile(encryption_path_.full_path);
}

void FileProvider::linkEncryptionInfo(const EncryptionPath & src_encryption_path_, const EncryptionPath & dst_encryption_path_) const
{
    key_manager->linkFile(src_encryption_path_.full_path, dst_encryption_path_.full_path);
}

bool FileProvider::isFileEncrypted(const EncryptionPath & encryption_path_) const
{
    auto encryption_info = key_manager->getFile(encryption_path_.full_path);
    // FileEncryptionRes::Disabled means encryption feature has never been enabled, so no file will be encrypted
    return (encryption_info.res != FileEncryptionRes::Disabled) && (encryption_info.method != EncryptionMethod::Plaintext);
}

bool FileProvider::isEncryptionEnabled() const { return encryption_enabled; }

void FileProvider::renameFile(const String &src_file_path_, const EncryptionPath &src_encryption_path_,
                              const String &dst_file_path_, const EncryptionPath &dst_encryption_path_) const
{
    Poco::File data_file(src_file_path_);
    if (unlikely(!data_file.exists()))
    {
        throw DB::TiFlashException(
                "Src file: " + src_file_path_ + " doesn't exist", Errors::Encryption::Internal);
    }
    if (unlikely(src_encryption_path_.file_name != dst_encryption_path_.file_name))
    {
        throw DB::TiFlashException(
                "The src file name: " + src_encryption_path_.file_name + " should be identical to dst file name: "
                + dst_encryption_path_.file_name, Errors::Encryption::Internal);
    }
    // rename encryption info(if any) before rename the underlying file
    if (isFileEncrypted(src_encryption_path_))
    {
        key_manager->renameFile(src_encryption_path_.full_path, dst_encryption_path_.full_path);
    }
    data_file.renameTo(dst_file_path_);
}

} // namespace DB
