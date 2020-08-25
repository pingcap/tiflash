#pragma once

#include <stdint.h>
#include <cstddef>
#include <memory>
#include <string>

namespace DB
{
// BlockAccessCipherStream is the base class for any cipher stream that
// supports random access at block level (without requiring data from other
// blocks). E.g. CTR (Counter operation mode) supports this requirement.
class BlockAccessCipherStream
{
public:
    virtual ~BlockAccessCipherStream() = default;

    // BlockSize returns the size of each block supported by this cipher stream.
    virtual size_t blockSize() = 0;

    // Encrypt one or more (partial) blocks of data at the file offset.
    // Length of data is given in dataSize.
    virtual void encrypt(uint64_t fileOffset, char * data, size_t dataSize) = 0;

    // Decrypt one or more (partial) blocks of data at the file offset.
    // Length of data is given in dataSize.
    virtual void decrypt(uint64_t fileOffset, char * data, size_t dataSize) = 0;
};

using BlockAccessCipherStreamPtr = std::shared_ptr<BlockAccessCipherStream>;
} // namespace DB
