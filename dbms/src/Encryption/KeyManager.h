#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include <Storages/Transaction/ProxyFFIType.h>

namespace DB
{
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

using FileEncryptionInfoPtr = std::shared_ptr<FileEncryptionInfo>;

class KeyManager
{
public:
    virtual ~KeyManager() = default;

    virtual FileEncryptionInfo getFile(const std::string & fname) = 0;

    virtual FileEncryptionInfo newFile(const std::string & fname) = 0;

    virtual void deleteFile(const std::string & fname) = 0;

    virtual void linkFile(const std::string & src_fname, const std::string & dst_fname) = 0;

    virtual void renameFile(const std::string & src_fname, const std::string & dst_fname) = 0;
};

using KeyManagerPtr = std::shared_ptr<KeyManager>;
} // namespace DB
