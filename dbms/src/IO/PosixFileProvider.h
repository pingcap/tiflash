#pragma once

#include <IO/FileProvider.h>

namespace DB
{
class PosixFileProvider : public FileProvider
{
protected:
    RandomAccessFilePtr newRandomAccessFileImpl(
        const std::string & file_path_, const EncryptionPath & encryption_path_, int flags) const override;

    WritableFilePtr newWritableFileImpl(const std::string & file_path_, const EncryptionPath & encryption_path_, bool create_new_file_,
        bool create_new_encryption_info_, int flags, mode_t mode) const override;

public:
    PosixFileProvider() = default;
    ~PosixFileProvider() override = default;

    void deleteFile(const std::string & file_path_, const EncryptionPath & encryption_path_) const override;

    void createEncryptionInfo(const std::string & file_path_) const override;
};
} // namespace DB
