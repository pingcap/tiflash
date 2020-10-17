#pragma once

#include <Encryption/EncryptionPath.h>
#include <Storages/Transaction/FileEncryption.h>

#include <cstddef>
#include <memory>
#include <string>

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

class KeyManager
{
public:
    virtual ~KeyManager() = default;

    virtual FileEncryptionInfo getFile(const String & fname) = 0;

    virtual FileEncryptionInfo newFile(const String & fname) = 0;

    virtual void deleteFile(const String & fname, bool throw_on_error) = 0;

    virtual void linkFile(const String & src_fname, const String & dst_fname) = 0;

    virtual void renameFile(const String & src_fname, const String & dst_fname) = 0;
};

using KeyManagerPtr = std::shared_ptr<KeyManager>;
} // namespace DB
