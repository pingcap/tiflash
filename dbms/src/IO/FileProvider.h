#pragma once

#include <common/likely.h>
#include <Common/Exception.h>
#include <Encryption/KeyManager.h>
#include <IO/EncryptedRandomAccessFile.h>
#include <IO/EncryptedWritableFile.h>
#include <IO/PosixRandomAccessFile.h>
#include <IO/PosixWritableFile.h>
#include <IO/RandomAccessFile.h>
#include <IO/WritableFile.h>
#include <Poco/File.h>
#include <string>

namespace DB
{
struct EncryptionPath
{
    EncryptionPath(const std::string & dir_name_, const std::string & file_name_) : dir_name{dir_name_}, file_name{file_name_} {}
    const std::string dir_name;
    const std::string file_name;
};

class FileProvider
{
public:
    FileProvider(KeyManagerPtr key_manager_, bool encryption_enabled_)
        : key_manager{std::move(key_manager_)},
          encryption_enabled{encryption_enabled_}
    {}

    RandomAccessFilePtr newRandomAccessFile(const std::string & file_path_, const EncryptionPath & encryption_path_, int flags = -1) const;

    WritableFilePtr newWritableFile(const std::string & file_path_, const EncryptionPath & encryption_path_, bool create_new_file_ = true,
        bool create_new_encryption_info_ = true, int flags = -1, mode_t mode = 0666) const;

    void deleteFile(const std::string & file_path_, const EncryptionPath & encryption_path_) const;

    void deleteEncryptionInfo(const EncryptionPath & encryption_path_) const;

    ~FileProvider() = default;

private:
    KeyManagerPtr key_manager;
    bool encryption_enabled;
};

using FileProviderPtr = std::shared_ptr<FileProvider>;
} // namespace DB
