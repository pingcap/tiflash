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

#pragma once

#include <Core/Types.h>
#include <Encryption/BlockAccessCipherStream.h>
#include <Encryption/KeyManager.h>
#include <Encryption/RandomAccessFile.h>
#include <Encryption/WritableFile.h>
#include <Encryption/WriteReadableFile.h>

#include <string>

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
    FileProvider(KeyManagerPtr key_manager_, bool encryption_enabled_)
        : key_manager{std::move(key_manager_)}
        , encryption_enabled{encryption_enabled_}
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
        const WriteLimiterPtr & write_limiter_ = nullptr,
        const ReadLimiterPtr & read_limiter = nullptr,
        int flags = -1,
        mode_t mode = 0666) const;

    // If dir_path_as_encryption_path is true, use dir_path_ as EncryptionPath
    // If false, use every file's path inside dir_path_ as EncryptionPath
    // Note this method is not atomic, and after calling it, the files in dir_path_ cannot be read again.
    void deleteDirectory(
        const String & dir_path_,
        bool dir_path_as_encryption_path = false,
        bool recursive = false) const;

    void deleteRegularFile(const String & file_path_, const EncryptionPath & encryption_path_) const;

    void createEncryptionInfo(const EncryptionPath & encryption_path_) const;

    void deleteEncryptionInfo(const EncryptionPath & encryption_path_, bool throw_on_error = true) const;

    // Please check `ln -h`
    // It will be link_encryption_name_ link to src_encryption_path_
    // For example: file0 have some data, file1 want to keep same data as file0
    //  Then call linkEncryptionInfo(file0,file1);
    void linkEncryptionInfo(const EncryptionPath & src_encryption_path_, const EncryptionPath & link_encryption_name_) const;

    bool isFileEncrypted(const EncryptionPath & encryption_path_) const;

    bool isEncryptionEnabled() const;

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
    bool encryption_enabled;
};

using FileProviderPtr = std::shared_ptr<FileProvider>;
} // namespace DB
