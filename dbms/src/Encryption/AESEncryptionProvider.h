#pragma once

#include <Encryption/AESCTRCipherStream.h>
#include <Encryption/EncryptionProvider.h>
#include <Encryption/KeyManager.h>

namespace DB
{
class AESEncryptionProvider : public EncryptionProvider
{
public:
    explicit AESEncryptionProvider(KeyManagerPtr & key_manager_) : key_manager(key_manager_) {}

    ~AESEncryptionProvider() override = default;

    BlockAccessCipherStreamPtr createCipherStream(const std::string & fname, bool new_file) override;

private:
    KeyManagerPtr key_manager;
};
} // namespace DB
