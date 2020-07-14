#pragma once

#include <Encryption/AESEncryptionProvider.h>
#include <Encryption/KeyManager.h>
#include <IO/FileProvider.h>

namespace DB
{
class EncryptedFileProvider : public FileProvider
{
protected:
    RandomAccessFilePtr newRandomAccessFileImpl(const std::string & file_name_, int flags) override;

    WritableFilePtr newWritableFileImpl(const std::string & file_name_, int flags, mode_t mode) override;

public:
    EncryptedFileProvider(FileProviderPtr & file_provider_, KeyManagerPtr key_manager_)
        : file_provider{file_provider_}, key_manager{std::move(key_manager_)}
    {
        encryption_provider = std::make_shared<AESEncryptionProvider>(key_manager);
    }

    ~EncryptedFileProvider() override = default;

private:
    FileProviderPtr file_provider;
    KeyManagerPtr key_manager;
    EncryptionProviderPtr encryption_provider;
};
} // namespace DB
