#pragma once

#include <Encryption/EncryptionPath.h>

#include <cstddef>
#include <memory>
#include <string>

namespace DB
{

enum class EncryptionMethod : uint8_t;
struct FileEncryptionInfo;

size_t KeySize(EncryptionMethod method);

class KeyManager
{
public:
    virtual ~KeyManager() = default;

    virtual FileEncryptionInfo getFile(const String & fname) = 0;

    virtual FileEncryptionInfo newFile(const String & fname) = 0;

    virtual void deleteFile(const String & fname, bool throw_on_error) = 0;

    virtual void linkFile(const String & src_fname, const String & dst_fname) = 0;
};

using KeyManagerPtr = std::shared_ptr<KeyManager>;
} // namespace DB
