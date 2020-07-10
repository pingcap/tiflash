#pragma once

#include <Encryption/BlockAccessCipherStream.h>
#include <cstddef>

namespace DB
{
// EncryptionProvider is used to create a cipher stream for a specific
// file. The returned cipher stream will be used for actual
// encryption/decryption actions.
class EncryptionProvider
{
public:
    virtual ~EncryptionProvider() = default;

    virtual BlockAccessCipherStreamPtr createCipherStream(const std::string & fname, bool new_file) = 0;
};

using EncryptionProviderPtr = std::shared_ptr<EncryptionProvider>;
} // namespace DB
