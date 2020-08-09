#pragma once

#include <Encryption/KeyManager.h>

namespace DB
{
class MockKeyManager : public KeyManager
{
public:
    ~MockKeyManager() = default;

    MockKeyManager(bool encryption_enabled_ = true);

    MockKeyManager(EncryptionMethod method_, const String & key_, const String & iv, bool encryption_enabled_ = true);

    FileEncryptionInfo getFile(const String & fname) override;

    FileEncryptionInfo newFile(const String & fname) override { return getFile(fname); }

    void deleteFile(const String & fname) override { std::ignore = fname; }

private:
    const static EncryptionMethod default_method;
    const static unsigned char default_key[33];
    const static unsigned char default_iv[17];

private:
    EncryptionMethod method;
    String key;
    String iv;
    bool encryption_enabled;
};
} // namespace DB
