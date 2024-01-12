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

#pragma once

#include <Core/Types.h>
#include <Encryption/BlockAccessCipherStream.h>
#include <Encryption/FileProvider_fwd.h>
#include <Encryption/KeyManager.h>
#include <Encryption/RandomAccessFile.h>
#include <Encryption/WritableFile.h>
#include <Encryption/WriteReadableFile.h>
#include <Storages/Page/PageDefinesBase.h>


namespace DB
{
using String = std::string;

class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;
class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

class FileProvider
{
public:
    FileProvider(KeyManagerPtr key_manager_, bool encryption_enabled_, bool keyspace_encryption_enabled_ = false)
        : key_manager{std::move(key_manager_)}
        , encryption_enabled{encryption_enabled_}
        , keyspace_encryption_enabled{keyspace_encryption_enabled_}
    {}

    RandomAccessFilePtr newRandomAccessFile(
        const String & file_path_,
        const EncryptionPath & encryption_path_,
        const ReadLimiterPtr & read_limiter = nullptr,
        int flags = -1) const;

    WritableFilePtr newWritableFile(
        const String & file_path_,
        const EncryptionPath & encryption_path_,
        bool truncate_if_exists_ = true,
        bool create_new_encryption_info_ = true,
        const WriteLimiterPtr & write_limiter_ = nullptr,
        int flags = -1,
        mode_t mode = 0666) const;

    WriteReadableFilePtr newWriteReadableFile(
        const String & file_path_,
        const EncryptionPath & encryption_path_,
        bool truncate_if_exists_ = true,
        bool create_new_encryption_info_ = true,
        bool skip_encryption_ = false,
        const WriteLimiterPtr & write_limiter_ = nullptr,
        const ReadLimiterPtr & read_limiter = nullptr,
        int flags = -1,
        mode_t mode = 0666) const;

    // If dir_path_as_encryption_path is true, use dir_path_ as EncryptionPath
    // If false, use every file's path inside dir_path_ as EncryptionPath
    // Note this method is not atomic, and after calling it, the files in dir_path_ cannot be read again.
    void deleteDirectory(const String & dir_path_, bool dir_path_as_encryption_path = false, bool recursive = false)
        const;

    void deleteRegularFile(const String & file_path_, const EncryptionPath & encryption_path_) const;

    void createEncryptionInfo(const EncryptionPath & encryption_path_) const;

    void deleteEncryptionInfo(const EncryptionPath & encryption_path_, bool throw_on_error = true) const;

    // Encrypt/Decrypt page data in place, using encryption_path_ to find the encryption info
    void encryptPage(const EncryptionPath & encryption_path_, char * data, size_t data_size, PageIdU64 page_id);
    void decryptPage(const EncryptionPath & encryption_path_, char * data, size_t data_size, PageIdU64 page_id);

    // Please check `ln -h`
    // It will be link_encryption_name_ link to src_encryption_path_
    // For example: file0 have some data, file1 want to keep same data as file0
    //  Then call linkEncryptionInfo(file0,file1);
    void linkEncryptionInfo(const EncryptionPath & src_encryption_path_, const EncryptionPath & link_encryption_name_)
        const;

    bool isFileEncrypted(const EncryptionPath & encryption_path_) const;

    bool isEncryptionEnabled() const;
    bool isKeyspaceEncryptionEnabled() const;

    // `renameFile` includes two steps,
    // 1. rename encryption info
    // 2. rename file
    // so it's not an atomic operation
    //
    // for the case that you want the atomicity, and the dst file already exist and is encrypted,
    // you can reuse the encryption info of dst_file for src_file,
    // and call `renameFile` with `rename_encryption_info_` set to false
    void renameFile(
        const String & src_file_path_,
        const EncryptionPath & src_encryption_path_,
        const String & dst_file_path_,
        const EncryptionPath & dst_encryption_path_,
        bool rename_encryption_info_) const;

    ~FileProvider() = default;

private:
    KeyManagerPtr key_manager;
    const bool encryption_enabled;
    // always false, only allow set to true when keyspace feature is GA in On-Promise.
    const bool keyspace_encryption_enabled = false;
};

} // namespace DB
