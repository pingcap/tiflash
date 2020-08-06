#pragma once

#include <Encryption/KeyManager.h>

namespace DB
{
class MockKeyManager : public KeyManager
{
public:
    ~MockKeyManager() = default;
    
    MockKeyManager(EncryptionMethod method_, const String & key_, const String & iv) : method{method_}, key{key_}, iv{iv} {}

    FileEncryptionInfo getFile(const String & fname) override
    {
        std::ignore = fname;
        auto * file_key = new String(key);
        auto * file_iv = new String(iv);
        FileEncryptionInfo file_info{
            FileEncryptionRes::Ok,
            method,
            file_key,
            file_iv,
            nullptr,
        };
        return file_info;
    }

    FileEncryptionInfo newFile(const String & fname) override { return getFile(fname); }

    void deleteFile(const String & fname) override { std::ignore = fname; }

private:
    EncryptionMethod method;
    String key;
    String iv;
};
} // namespace DB
