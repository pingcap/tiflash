#pragma once

#include <Encryption/AESEncryptionProvider.h>
#include <Encryption/KeyManager.h>
#include <IO/FileProvider.h>

namespace DB
{
class EncryptedFileProvider : public FileProvider
{
protected:
    RandomAccessFilePtr newRandomAccessFileImpl(
        const std::string & file_path_, const EncryptionPath & encryption_path_, int flags) const override;

    WritableFilePtr newWritableFileImpl(const std::string & file_path_, const EncryptionPath & encryption_path_, bool create_new_file_,
        bool create_new_encryption_info_, int flags, mode_t mode) const override;

public:
    EncryptedFileProvider(FileProviderPtr & file_provider_, KeyManagerPtr key_manager_)
        : file_provider{file_provider_}, key_manager{std::move(key_manager_)}
    {
        encryption_provider = std::make_shared<AESEncryptionProvider>(key_manager);
    }

    ~EncryptedFileProvider() override = default;

    void deleteFile(const std::string & file_path_, const EncryptionPath & encryption_path_) const override;

    void createEncryptionInfo(const std::string & file_path_) const override;

private:
    FileProviderPtr file_provider;
    KeyManagerPtr key_manager;
    EncryptionProviderPtr encryption_provider;
};
} // namespace DB
