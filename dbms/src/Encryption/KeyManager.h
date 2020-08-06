#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <Storages/Transaction/ProxyFFIType.h>

namespace DB
{
using String = std::string;

struct EncryptionPath
{
    EncryptionPath(const std::string & dir_name_, const std::string & file_name_) : dir_name{dir_name_}, file_name{file_name_} {}
    const std::string dir_name;
    const std::string file_name;
};

inline size_t KeySize(EncryptionMethod method)
{
    switch (method)
    {
        case EncryptionMethod::Aes128Ctr:
            return 16;
        case EncryptionMethod::Aes192Ctr:
            return 24;
        case EncryptionMethod::Aes256Ctr:
            return 32;
        default:
            return 0;
    }
}

class KeyManager
{
public:
    virtual ~KeyManager() = default;

    virtual FileEncryptionInfo getFile(const String & fname) = 0;

    virtual FileEncryptionInfo newFile(const String & fname) = 0;

    virtual void deleteFile(const String & fname) = 0;
};

using KeyManagerPtr = std::shared_ptr<KeyManager>;
} // namespace DB
