#pragma once

#include <Encryption/KeyManager.h>

namespace DB
{
class MockKeyManager : public KeyManager
{
public:
    ~MockKeyManager() = default;
    
    MockKeyManager(EncryptionMethod method_, const std::string & key_, const std::string & iv) : method{method_}, key{key_}, iv{iv} {}

    FileEncryptionInfo getFile(const std::string & fname) override
    {
        std::ignore = fname;
        auto * file_key = new std::string(key);
        auto * file_iv = new std::string(iv);
        FileEncryptionInfo file_info{
            FileEncryptionRes::Ok,
            method,
            file_key,
            file_iv,
            nullptr,
        };
        return file_info;
    }

    FileEncryptionInfo newFile(const std::string & fname) override { return getFile(fname); }

    void deleteFile(const std::string & fname) override { std::ignore = fname; }

    void linkFile(const std::string & src_fname, const std::string & dst_fname) override
    {
        std::ignore = src_fname;
        std::ignore = dst_fname;
    }

    void renameFile(const std::string & src_fname, const std::string & dst_fname) override
    {
        std::ignore = src_fname;
        std::ignore = dst_fname;
    }

private:
    EncryptionMethod method;
    std::string key;
    std::string iv;
};
} // namespace DB
