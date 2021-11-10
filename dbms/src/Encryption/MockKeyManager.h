#pragma once

#include <Encryption/KeyManager.h>

#include <vector>

namespace DB
{
class MockKeyManager : public KeyManager
{
public:
    ~MockKeyManager() = default;

    MockKeyManager(bool encryption_enabled_ = true);

    MockKeyManager(EncryptionMethod method_, const String & key_, const String & iv, bool encryption_enabled_ = true);

    FileEncryptionInfo getFile(const String & fname) override;

    FileEncryptionInfo newFile(const String & fname) override;

    void deleteFile(const String & fname, bool /*throw_on_error*/) override;

    void linkFile(const String & src_fname, const String & dst_fname) override;

private:
    bool fileExist(const String & fname) const;

private:
    const static EncryptionMethod default_method;
    const static unsigned char default_key[33];
    const static unsigned char default_iv[17];
    std::vector<String> files;

    EncryptionMethod method;
    String key;
    String iv;
    bool encryption_enabled;
};
} // namespace DB
