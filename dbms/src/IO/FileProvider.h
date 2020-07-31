#pragma once

#include <IO/RandomAccessFile.h>
#include <IO/WritableFile.h>
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
protected:
    virtual RandomAccessFilePtr newRandomAccessFileImpl(
        const std::string & file_path_, const EncryptionPath & encryption_path_, int flags) const = 0;

    virtual WritableFilePtr newWritableFileImpl(const std::string & file_path_, const EncryptionPath & encryption_path_,
        bool create_new_file_, bool create_new_encryption_info_, int flags, mode_t mode) const = 0;

public:
    RandomAccessFilePtr newRandomAccessFile(const std::string & file_path_, const EncryptionPath & encryption_path_, int flags = -1) const
    {
        return newRandomAccessFileImpl(file_path_, encryption_path_, flags);
    }

    WritableFilePtr newWritableFile(const std::string & file_path_, const EncryptionPath & encryption_path_, bool create_new_file_ = true,
        bool create_new_encryption_info_ = true, int flags = -1, mode_t mode = 0666) const
    {
        return newWritableFileImpl(file_path_, encryption_path_, create_new_file_, create_new_encryption_info_, flags, mode);
    }

    virtual void deleteFile(const std::string & file_path_, const EncryptionPath & encryption_path_) const = 0;

    virtual void createEncryptionInfo(const std::string & file_path_) const = 0;

    virtual ~FileProvider() = default;
};

using FileProviderPtr = std::shared_ptr<FileProvider>;
} // namespace DB
