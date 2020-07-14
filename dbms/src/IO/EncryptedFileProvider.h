#pragma once

#include <IO/FileProvider.h>

namespace DB
{
class EncryptedFileProvider : public FileProvider
{
protected:
    RandomAccessFilePtr newRandomAccessFileImpl(const std::string & file_name_, int flags) override;

    WritableFilePtr newWritableFileImpl(const std::string & file_name_, int flags, mode_t mode) override;

public:
    EncryptedFileProvider(FileProviderPtr & file_provider_) : file_provider{file_provider_} {}
    ~EncryptedFileProvider() override = default;

private:
    FileProviderPtr file_provider;
};
} // namespace DB
