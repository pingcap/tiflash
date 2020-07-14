#pragma once

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <Encryption/BlockAccessCipherStream.h>
#include <IO/Endian.h>

namespace DB
{
#if OPENSSL_VERSION_NUMBER < 0x01010000f

#define InitCipherContext(ctx) \
    EVP_CIPHER_CTX ctx##_var;  \
    ctx = &ctx##_var;          \
    EVP_CIPHER_CTX_init(ctx);

// do nothing
#define FreeCipherContext(ctx)

#else

#define InitCipherContext(ctx)              \
    ctx = EVP_CIPHER_CTX_new();             \
    if (ctx != nullptr)                     \
    {                                       \
        if (EVP_CIPHER_CTX_reset(ctx) != 1) \
        {                                   \
            ctx = nullptr;                  \
        }                                   \
    }

#define FreeCipherContext(ctx) EVP_CIPHER_CTX_free(ctx);

#endif

class AESCTRCipherStream : public BlockAccessCipherStream
{
public:
    AESCTRCipherStream(const EVP_CIPHER * cipher, const std::string & key, uint64_t iv_high, uint64_t iv_low)
        : cipher_(cipher), key_(key), initial_iv_high_(iv_high), initial_iv_low_(iv_low)
    {}

    ~AESCTRCipherStream() override = default;

    size_t blockSize() override
    {
        return AES_BLOCK_SIZE; // 16
    }

    void encrypt(uint64_t file_offset, char * data, size_t data_size) override
    {
        cipher(file_offset, data, data_size, true /*is_encrypt*/);
    }

    void decrypt(uint64_t file_offset, char * data, size_t data_size) override
    {
        cipher(file_offset, data, data_size, false /*is_encrypt*/);
    }

private:
    void cipher(uint64_t file_offset, char * data, size_t data_size, bool is_encrypt);

    const EVP_CIPHER * cipher_;
    const std::string key_;
    const uint64_t initial_iv_high_;
    const uint64_t initial_iv_low_;
};
} // namespace DB
