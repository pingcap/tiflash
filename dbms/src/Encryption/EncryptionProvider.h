#pragma once

#include <cstddef>

#include <Encryption/BlockAccessCipherStream.h>

namespace DB
{

// The encryption provider is used to create a cipher stream for a specific
// file. The returned cipher stream will be used for actual
// encryption/decryption actions.
class EncryptionProvider
{
public:
    virtual ~EncryptionProvider() = default;

    // CreateCipherStream creates a block access cipher stream for a file given name.
    virtual BlockAccessCipherStreamPtr createCipherStream(const std::string & fname) = 0;
};

using EncryptionProviderPtr = std::shared_ptr<EncryptionProvider>;

} // namespace DB
