#include <Common/TiFlashException.h>
#include <Encryption/EncryptedRandomAccessFile.h>
#include <Encryption/EncryptedWritableFile.h>
#include <Encryption/FileProvider.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <Encryption/PosixWritableFile.h>
#include <Poco/File.h>
#include <Storages/Transaction/FileEncryption.h>
#include <common/likely.h>

namespace DB
{

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

WritableFilePtr FileProvider::newWritableFile(const String & file_path_, const EncryptionPath & encryption_path_, bool truncate_if_exists_,
    bool create_new_encryption_info_, const RateLimiterPtr & rate_limiter_, int flags, mode_t mode) const
{
    WritableFilePtr file = std::make_shared<PosixWritableFile>(file_path_, truncate_if_exists_, flags, mode, rate_limiter_);
    if (encryption_enabled && create_new_encryption_info_)
    {
        auto encryption_info = key_manager->newFile(encryption_path_.full_path);
        file = std::make_shared<EncryptedWritableFile>(file, AESCTRCipherStream::createCipherStream(encryption_info, encryption_path_));
    }
    else if (!create_new_encryption_info_)
    {
        auto encryption_info = key_manager->getFile(encryption_path_.full_path);
        if (encryption_info.method != EncryptionMethod::Unknown && encryption_info.method != EncryptionMethod::Plaintext)
        {
            file = std::make_shared<EncryptedWritableFile>(file, AESCTRCipherStream::createCipherStream(encryption_info, encryption_path_));
        }
    }
    return file;
}

void FileProvider::deleteDirectory(const String & dir_path_, bool dir_path_as_encryption_path, bool recursive) const
{
    Poco::File dir_file(dir_path_);
    if (dir_file.exists())
    {
        if (dir_path_as_encryption_path)
        {
            key_manager->deleteFile(dir_path_, true);
            dir_file.remove(recursive);
        }
        else if (recursive)
        {
            std::vector<Poco::File> files;
            dir_file.list(files);
            for (auto & file : files)
            {
                if (file.isFile())
                {
                    key_manager->deleteFile(file.path(), true);
                }
                else if (file.isDirectory())
                {
                    deleteDirectory(file.path(), false, recursive);
                }
                else
                {
                    throw DB::TiFlashException("Unknown file type: " + file.path(), Errors::Encryption::Internal);
                }
            }
            dir_file.remove(recursive);
        }
        else
        {
            // recursive must be false here
            dir_file.remove(false);
        }
    }
}

void FileProvider::deleteRegularFile(const String & file_path_, const EncryptionPath & encryption_path_) const
{
    Poco::File data_file(file_path_);
    if (data_file.exists())
    {
        if (unlikely(!data_file.isFile()))
        {
            throw DB::TiFlashException("File: " + data_file.path() + " is not a regular file", Errors::Encryption::Internal);
        }
        key_manager->deleteFile(encryption_path_.full_path, true);
        data_file.remove(false);
    }
}

void FileProvider::createEncryptionInfo(const EncryptionPath & encryption_path_) const
{
    if (encryption_enabled)
    {
        key_manager->newFile(encryption_path_.full_path);
    }
}

void FileProvider::deleteEncryptionInfo(const EncryptionPath & encryption_path_, bool throw_on_error) const
{
    key_manager->deleteFile(encryption_path_.full_path, throw_on_error);
}

void FileProvider::linkEncryptionInfo(const EncryptionPath & src_encryption_path_, const EncryptionPath & dst_encryption_path_) const
{
    // delete the encryption info for dst_path if any
    if (isFileEncrypted(dst_encryption_path_))
        key_manager->deleteFile(dst_encryption_path_.full_path, true);
    key_manager->linkFile(src_encryption_path_.full_path, dst_encryption_path_.full_path);
}

bool FileProvider::isFileEncrypted(const EncryptionPath & encryption_path_) const
{
    auto encryption_info = key_manager->getFile(encryption_path_.full_path);
    // FileEncryptionRes::Disabled means encryption feature has never been enabled, so no file will be encrypted
    return (encryption_info.res != FileEncryptionRes::Disabled) && (encryption_info.method != EncryptionMethod::Plaintext);
}

bool FileProvider::isEncryptionEnabled() const { return encryption_enabled; }

void FileProvider::renameFile(const String & src_file_path_, const EncryptionPath & src_encryption_path_, const String & dst_file_path_,
    const EncryptionPath & dst_encryption_path_, bool rename_encryption_info_) const
{
    Poco::File data_file(src_file_path_);
    if (unlikely(!data_file.exists()))
    {
        throw DB::TiFlashException("Src file: " + src_file_path_ + " doesn't exist", Errors::Encryption::Internal);
    }
    if (unlikely(src_encryption_path_.file_name != dst_encryption_path_.file_name))
    {
        throw DB::TiFlashException("The src file name: " + src_encryption_path_.file_name
                + " should be identical to dst file name: " + dst_encryption_path_.file_name,
            Errors::Encryption::Internal);
    }

    if (!rename_encryption_info_)
    {
        if (unlikely(src_encryption_path_.full_path != dst_encryption_path_.full_path))
        {
            throw DB::TiFlashException("Src file encryption full path: " + src_encryption_path_.full_path
                    + " must be same with dst file encryption full path" + dst_encryption_path_.full_path,
                Errors::Encryption::Internal);
        }
        data_file.renameTo(dst_file_path_);
        return;
    }

    // delete the encryption info for dst_path if any
    if (isFileEncrypted(dst_encryption_path_))
        key_manager->deleteFile(dst_encryption_path_.full_path, true);

    // rename encryption info(if any) before rename the underlying file
    bool is_file_encrypted = isFileEncrypted(src_encryption_path_);
    if (is_file_encrypted)
        key_manager->linkFile(src_encryption_path_.full_path, dst_encryption_path_.full_path);

    data_file.renameTo(dst_file_path_);

    if (is_file_encrypted)
        key_manager->deleteFile(src_encryption_path_.full_path, false);
}

} // namespace DB
