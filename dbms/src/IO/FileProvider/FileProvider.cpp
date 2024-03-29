// Copyright 2023 PingCAP, Inc.
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
#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <IO/BaseFile/PosixWritableFile.h>
#include <IO/BaseFile/PosixWriteReadableFile.h>
#include <IO/Encryption/EncryptedRandomAccessFile.h>
#include <IO/Encryption/EncryptedWritableFile.h>
#include <IO/Encryption/EncryptedWriteReadableFile.h>
#include <IO/Encryption/KeyspacesKeyManager.h>
#include <IO/FileProvider/FileProvider.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/KVStore/FFI/FileEncryption.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <Storages/S3/S3WritableFile.h>
#include <common/likely.h>


namespace DB
{
RandomAccessFilePtr FileProvider::newRandomAccessFile(
    const String & file_path_,
    const EncryptionPath & encryption_path_,
    const ReadLimiterPtr & read_limiter,
    int flags) const
{
    // S3 file always does not encrypt.
    if (auto view = S3::S3FilenameView::fromKeyWithPrefix(file_path_); view.isValid())
        return S3::S3RandomAccessFile::create(view.toFullKey());

    // Unrecognized xx:// protocol.
    RUNTIME_CHECK_MSG(file_path_.find("://") == std::string::npos, "Unsupported protocol in path {}", file_path_);
    RandomAccessFilePtr file = std::make_shared<PosixRandomAccessFile>(file_path_, flags, read_limiter);
    auto encryption_info = key_manager->getInfo(encryption_path_);
    if (auto stream = encryption_info.createCipherStream(encryption_path_); stream)
    {
        file = std::make_shared<EncryptedRandomAccessFile>(file, stream);
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
    // S3 file always does not encrypt.
    if (auto view = S3::S3FilenameView::fromKeyWithPrefix(file_path_); view.isValid())
        return S3::S3WritableFile::create(view.toFullKey());

    // Unrecognized xx:// protocol.
    RUNTIME_CHECK_MSG(file_path_.find("://") == std::string::npos, "Unsupported protocol in path {}", file_path_);
    WritableFilePtr file
        = std::make_shared<PosixWritableFile>(file_path_, truncate_if_exists_, flags, mode, write_limiter_);
    if (encryption_enabled && create_new_encryption_info_)
    {
        auto encryption_info = key_manager->newInfo(encryption_path_);
        if (auto stream = encryption_info.createCipherStream(encryption_path_, true); stream)
        {
            file = std::make_shared<EncryptedWritableFile>(file, stream);
        }
    }
    else if (!create_new_encryption_info_)
    {
        auto encryption_info = key_manager->getInfo(encryption_path_);
        if (auto stream = encryption_info.createCipherStream(encryption_path_); stream)
        {
            file = std::make_shared<EncryptedWritableFile>(file, stream);
        }
    }
    return file;
}

WriteReadableFilePtr FileProvider::newWriteReadableFile(
    const String & file_path_,
    const EncryptionPath & encryption_path_,
    bool truncate_if_exists_,
    bool create_new_encryption_info_,
    bool skip_encryption_,
    const WriteLimiterPtr & write_limiter_,
    const ReadLimiterPtr & read_limiter,
    int flags,
    mode_t mode) const
{
    WriteReadableFilePtr file = std::make_shared<PosixWriteReadableFile>(
        file_path_,
        truncate_if_exists_,
        flags,
        mode,
        write_limiter_,
        read_limiter);
    if (skip_encryption_)
        return file;

    if (encryption_enabled && create_new_encryption_info_)
    {
        auto encryption_info = key_manager->newInfo(encryption_path_);
        if (auto stream = encryption_info.createCipherStream(encryption_path_, true); stream)
        {
            file = std::make_shared<EncryptedWriteReadableFile>(file, stream);
        }
    }
    else if (!create_new_encryption_info_)
    {
        auto encryption_info = key_manager->getInfo(encryption_path_);
        if (auto stream = encryption_info.createCipherStream(encryption_path_); stream)
        {
            file = std::make_shared<EncryptedWriteReadableFile>(file, stream);
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
            key_manager->deleteInfo(EncryptionPath(dir_path_, ""), true);
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
                    key_manager->deleteInfo(EncryptionPath(file.path(), ""), true);
                }
                else if (file.isDirectory())
                {
                    deleteDirectory(file.path(), false, recursive);
                }
                else
                {
                    throw DB::TiFlashException(Errors::Encryption::Internal, "Unknown file type: {}", file.path());
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
            throw DB::TiFlashException(
                Errors::Encryption::Internal,
                "File: {} is not a regular file",
                data_file.path());
        }
        // Remove the file on disk before removing the encryption key. Or we may leave an encrypted file without the encryption key
        // and the encrypted file can not be read.
        // In the worst case that TiFlash crash between removing the file on disk and removing the encryption key, we may leave
        // the encryption key not deleted. However, this is a rare case and won't cause serious problem.
        data_file.remove(false);
        key_manager->deleteInfo(encryption_path_, true);
    }
}

void FileProvider::createEncryptionInfo(const EncryptionPath & encryption_path_) const
{
    if (encryption_enabled)
    {
        key_manager->newInfo(encryption_path_);
    }
}

void FileProvider::deleteEncryptionInfo(const EncryptionPath & encryption_path_, bool throw_on_error) const
{
    key_manager->deleteInfo(encryption_path_, throw_on_error);
}

void FileProvider::dropEncryptionInfo(KeyspaceID keyspace_id) const
{
    if (auto keyspaces_key_manager
        = std::dynamic_pointer_cast<KeyspacesKeyManager<TiFlashRaftProxyHelper>>(key_manager);
        keyspaces_key_manager)
        keyspaces_key_manager->deleteKey(keyspace_id);
    if (auto keyspaces_key_manager
        = std::dynamic_pointer_cast<KeyspacesKeyManager<MockProxyEncryptionFFI>>(key_manager);
        keyspaces_key_manager)
        keyspaces_key_manager->deleteKey(keyspace_id);
}

void FileProvider::encryptPage(KeyspaceID keyspace_id, char * data, size_t data_size, PageIdU64 page_id)
{
    // pass page_id as file_path to genarate unique IV.
    const auto info = key_manager->getInfo(EncryptionPath(std::to_string(page_id), "", keyspace_id));
    info.cipherData<FileEncryptionInfo::Encrypt>(data, data_size);
}

void FileProvider::decryptPage(KeyspaceID keyspace_id, char * data, size_t data_size, PageIdU64 page_id)
{
    const auto info = key_manager->getInfo(EncryptionPath(std::to_string(page_id), "", keyspace_id));
    info.cipherData<FileEncryptionInfo::Decrypt>(data, data_size);
}

void FileProvider::linkEncryptionInfo(
    const EncryptionPath & src_encryption_path_,
    const EncryptionPath & link_encryption_name_) const
{
    // delete the encryption info for dst_path if any
    if (isFileEncrypted(link_encryption_name_))
        key_manager->deleteInfo(link_encryption_name_, true);
    key_manager->linkInfo(src_encryption_path_, link_encryption_name_);
}

bool FileProvider::isFileEncrypted(const EncryptionPath & encryption_path_) const
{
    auto encryption_info = key_manager->getInfo(encryption_path_);
    return encryption_info.isEncrypted();
}

bool FileProvider::isEncryptionEnabled() const
{
    return encryption_enabled;
}

bool FileProvider::isKeyspaceEncryptionEnabled() const
{
    return encryption_enabled && keyspace_encryption_enabled;
}

bool FileProvider::isEncryptionEnabled(KeyspaceID keyspace_id) const
{
    return encryption_enabled && keyspace_encryption_enabled && key_manager->isEncryptionEnabled(keyspace_id);
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
        throw DB::TiFlashException(Errors::Encryption::Internal, "Src file: {} doesn't exist", src_file_path_);
    }
    if (unlikely(src_encryption_path_.file_name != dst_encryption_path_.file_name))
    {
        throw DB::TiFlashException(
            Errors::Encryption::Internal,
            "The src file name: {} should be identical to dst file name: {}",
            src_encryption_path_.file_name,
            dst_encryption_path_.file_name);
    }

    if (!rename_encryption_info_)
    {
        if (unlikely(src_encryption_path_.full_path != dst_encryption_path_.full_path))
        {
            throw DB::TiFlashException(
                Errors::Encryption::Internal,
                "Src file encryption full path: {} must be same with dst file encryption full path: {}",
                src_encryption_path_.full_path,
                dst_encryption_path_.full_path);
        }
        data_file.renameTo(dst_file_path_);
        return;
    }

    // delete the encryption info for dst_path if any
    if (isFileEncrypted(dst_encryption_path_))
        key_manager->deleteInfo(dst_encryption_path_, true);

    // rename encryption info(if any) before rename the underlying file
    bool is_file_encrypted = isFileEncrypted(src_encryption_path_);
    if (is_file_encrypted)
        key_manager->linkInfo(src_encryption_path_, dst_encryption_path_);

    data_file.renameTo(dst_file_path_);

    if (is_file_encrypted)
        key_manager->deleteInfo(src_encryption_path_, false);
}

void FileProvider::setPageStoragePtrForKeyManager(const UniversalPageStoragePtr & page_storage_ptr_)
{
    if (auto keyspaces_key_manager
        = std::dynamic_pointer_cast<KeyspacesKeyManager<TiFlashRaftProxyHelper>>(key_manager);
        keyspaces_key_manager)
        keyspaces_key_manager->setUniversalPageStorage(page_storage_ptr_);
    if (auto keyspaces_key_manager
        = std::dynamic_pointer_cast<KeyspacesKeyManager<MockProxyEncryptionFFI>>(key_manager);
        keyspaces_key_manager)
        keyspaces_key_manager->setUniversalPageStorage(page_storage_ptr_);
}

} // namespace DB