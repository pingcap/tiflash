#pragma once

#include <Encryption/BlockAccessCipherStream.h>
#include <Encryption/KeyManager.h>
#include <Encryption/RandomAccessFile.h>
#include <Encryption/WritableFile.h>
#include <string>

namespace DB
{
using String = std::string;

class FileProvider
{
public:
    FileProvider(KeyManagerPtr key_manager_, bool encryption_enabled_)
        : key_manager{std::move(key_manager_)}, encryption_enabled{encryption_enabled_}
    {}

    RandomAccessFilePtr newRandomAccessFile(const String & file_path_, const EncryptionPath & encryption_path_, int flags = -1) const;

    WritableFilePtr newWritableFile(const String & file_path_, const EncryptionPath & encryption_path_, bool create_new_file_ = true,
        bool create_new_encryption_info_ = true, int flags = -1, mode_t mode = 0666) const;

    // If dir_path_as_encryption_path is true, use dir_path_ as EncryptionPath
    // If false, use every file's path inside dir_path_ as EncryptionPath
    void deleteDirectory(const String & dir_path_, bool dir_path_as_encryption_path = false, bool recursive = false) const;

    void deleteRegularFile(const String & file_path_, const EncryptionPath & encryption_path_) const;

    void createEncryptionInfo(const EncryptionPath & encryption_path_) const;

    void deleteEncryptionInfo(const EncryptionPath & encryption_path_, bool throw_on_error = true) const;

    void linkEncryptionInfo(const EncryptionPath & src_encryption_path_, const EncryptionPath & dst_encryption_path_) const;

    bool isFileEncrypted(const EncryptionPath & encryption_path_) const;

    bool isEncryptionEnabled() const;

    void renameFile(const String & src_file_path_, const EncryptionPath & src_encryption_path_,
            const String & dst_file_path_, const EncryptionPath & dst_encryption_path_) const;

    ~FileProvider() = default;

private:
    KeyManagerPtr key_manager;
    bool encryption_enabled;
};

using FileProviderPtr = std::shared_ptr<FileProvider>;
} // namespace DB
