// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/TiFlashException.h>
#include <Encryption/EncryptedRandomAccessFile.h>
#include <Encryption/EncryptedWritableFile.h>
#include <Encryption/EncryptedWriteReadableFile.h>
#include <Encryption/FileProvider.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <Encryption/PosixWritableFile.h>
#include <Encryption/PosixWriteReadableFile.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <Storages/S3/S3WritableFile.h>
#include <Storages/Transaction/FileEncryption.h>
#include <common/likely.h>

namespace DB
{
RandomAccessFilePtr FileProvider::newRandomAccessFile(
    const String & file_path_,
    const EncryptionPath & encryption_path_,
    const ReadLimiterPtr & read_limiter,
    int flags) const
{
    RandomAccessFilePtr file;
    if (auto view = S3::S3FilenameView::fromKeyWithPrefix(file_path_); view.isValid())
    {
        file = S3::S3RandomAccessFile::create(view.toFullKey());
    }
    else
    {
        // Unrecognized xx:// protocol.
        RUNTIME_CHECK_MSG(
            file_path_.find("://") == std::string::npos,
            "Unsupported protocol in path {}",
            file_path_);
        file = std::make_shared<PosixRandomAccessFile>(file_path_, flags, read_limiter);
    }
    auto encryption_info = key_manager->getFile(encryption_path_.full_path);
    if (encryption_info.res != FileEncryptionRes::Disabled && encryption_info.method != EncryptionMethod::Plaintext)
    {
        file = std::make_shared<EncryptedRandomAccessFile>(file, AESCTRCipherStream::createCipherStream(encryption_info, encryption_path_));
    }
    return file;
}

WritableFilePtr FileProvider::newWritableFile(
    const String & file_path_,
    const EncryptionPath & encryption_path_,
    bool truncate_if_exists_,
    bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_,
    int flags,
    mode_t mode) const
{
    WritableFilePtr file;
    if (auto view = S3::S3FilenameView::fromKeyWithPrefix(file_path_); view.isValid())
    {
        file = S3::S3WritableFile::create(view.toFullKey());
    }
    else
    {
        // Unrecognized xx:// protocol.
        RUNTIME_CHECK_MSG(
            file_path_.find("://") == std::string::npos,
            "Unsupported protocol in path {}",
            file_path_);
        file = std::make_shared<PosixWritableFile>(file_path_, truncate_if_exists_, flags, mode, write_limiter_);
    }
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

WriteReadableFilePtr FileProvider::newWriteReadableFile(
    const String & file_path_,
    const EncryptionPath & encryption_path_,
    bool truncate_if_exists_,
    bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_,
    const ReadLimiterPtr & read_limiter,
    int flags,
    mode_t mode) const
{
    WriteReadableFilePtr file = std::make_shared<PosixWriteReadableFile>(file_path_, truncate_if_exists_, flags, mode, write_limiter_, read_limiter);
    if (encryption_enabled && create_new_encryption_info_)
    {
        auto encryption_info = key_manager->newFile(encryption_path_.full_path);
        file = std::make_shared<EncryptedWriteReadableFile>(file, AESCTRCipherStream::createCipherStream(encryption_info, encryption_path_));
    }
    else if (!create_new_encryption_info_)
    {
        auto encryption_info = key_manager->getFile(encryption_path_.full_path);
        if (encryption_info.method != EncryptionMethod::Unknown && encryption_info.method != EncryptionMethod::Plaintext)
        {
            file = std::make_shared<EncryptedWriteReadableFile>(file, AESCTRCipherStream::createCipherStream(encryption_info, encryption_path_));
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
        // Remove the file on disk before removing the encryption key. Or we may leave an encrypted file without the encryption key
        // and the encrypted file can not be read.
        // In the worst case that TiFlash crash between removing the file on disk and removing the encryption key, we may leave
        // the encryption key not deleted. However, this is a rare case and won't cause serious problem.
        data_file.remove(false);
        key_manager->deleteFile(encryption_path_.full_path, true);
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

void FileProvider::linkEncryptionInfo(const EncryptionPath & src_encryption_path_, const EncryptionPath & link_encryption_name_) const
{
    // delete the encryption info for dst_path if any
    if (isFileEncrypted(link_encryption_name_))
        key_manager->deleteFile(link_encryption_name_.full_path, true);
    key_manager->linkFile(src_encryption_path_.full_path, link_encryption_name_.full_path);
}

bool FileProvider::isFileEncrypted(const EncryptionPath & encryption_path_) const
{
    auto encryption_info = key_manager->getFile(encryption_path_.full_path);
    // FileEncryptionRes::Disabled means encryption feature has never been enabled, so no file will be encrypted
    return (encryption_info.res != FileEncryptionRes::Disabled) && (encryption_info.method != EncryptionMethod::Plaintext);
}

bool FileProvider::isEncryptionEnabled() const
{
    return encryption_enabled;
}

void FileProvider::renameFile(
    const String & src_file_path_,
    const EncryptionPath & src_encryption_path_,
    const String & dst_file_path_,
    const EncryptionPath & dst_encryption_path_,
    bool rename_encryption_info_) const
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
