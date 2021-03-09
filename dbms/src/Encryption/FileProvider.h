#pragma once

#include <Core/Types.h>
#include <Encryption/BlockAccessCipherStream.h>
#include <Encryption/KeyManager.h>
#include <Encryption/RandomAccessFile.h>
#include <Encryption/WritableFile.h>

#include <string>

namespace DB
{
using String = std::string;

class RateLimiter;
using RateLimiterPtr = std::shared_ptr<RateLimiter>;

class FileProvider
{
public:
    FileProvider(KeyManagerPtr key_manager_, bool encryption_enabled_)
        : key_manager{std::move(key_manager_)}, encryption_enabled{encryption_enabled_}
    {}

    RandomAccessFilePtr newRandomAccessFile(const String & file_path_, const EncryptionPath & encryption_path_, int flags = -1) const;

    WritableFilePtr newWritableFile(const String & file_path_, const EncryptionPath & encryption_path_, bool truncate_if_exists_ = true,
        bool create_new_encryption_info_ = true, const RateLimiterPtr & rate_limiter_ = nullptr, int flags = -1, mode_t mode = 0666) const;

    // If dir_path_as_encryption_path is true, use dir_path_ as EncryptionPath
    // If false, use every file's path inside dir_path_ as EncryptionPath
    void deleteDirectory(const String & dir_path_, bool dir_path_as_encryption_path = false, bool recursive = false) const;

    void deleteRegularFile(const String & file_path_, const EncryptionPath & encryption_path_) const;

    void createEncryptionInfo(const EncryptionPath & encryption_path_) const;

    void deleteEncryptionInfo(const EncryptionPath & encryption_path_, bool throw_on_error = true) const;

    void linkEncryptionInfo(const EncryptionPath & src_encryption_path_, const EncryptionPath & dst_encryption_path_) const;

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
    void renameFile(const String & src_file_path_, const EncryptionPath & src_encryption_path_, const String & dst_file_path_,
        const EncryptionPath & dst_encryption_path_, bool rename_encryption_info_) const;

    ~FileProvider() = default;

private:
    KeyManagerPtr key_manager;
    bool encryption_enabled;
};

using FileProviderPtr = std::shared_ptr<FileProvider>;
} // namespace DB
