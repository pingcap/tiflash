#include <Common/Exception.h>
#include <Encryption/AESCTRCipherStream.h>
#include <cstddef>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int DATA_ENCRYPTION_ERROR;
} // namespace ErrorCodes

void AESCTRCipherStream::cipher(uint64_t file_offset, char * data, size_t data_size, bool is_encrypt)
{
#if OPENSSL_VERSION_NUMBER < 0x01000200f
    (void)file_offset;
    (void)data;
    (void)data_size;
    (void)is_encrypt;
    throw Exception("OpenSSL version < 1.0.2", ErrorCodes::NOT_IMPLEMENTED);
#else
    int ret = 1;
    EVP_CIPHER_CTX * ctx = nullptr;
    InitCipherContext(ctx);
    if (ctx == nullptr)
    {
        throw Exception("Failed to create cipher context.", ErrorCodes::DATA_ENCRYPTION_ERROR);
    }

    uint64_t block_index = file_offset / AES_BLOCK_SIZE;
    uint64_t block_offset = file_offset % AES_BLOCK_SIZE;

    // In CTR mode, OpenSSL EVP API treat the IV as a 128-bit big-endian, and
    // increase it by 1 for each block.
    uint64_t iv_high = initial_iv_high_;
    uint64_t iv_low = initial_iv_low_ + block_index;
    if (std::numeric_limits<uint64_t>::max() - block_index < initial_iv_low_)
    {
        iv_high++;
    }
    iv_high = toBigEndian(iv_high);
    iv_low = toBigEndian(iv_low);
    unsigned char iv[AES_BLOCK_SIZE];
    memcpy(iv, &iv_high, sizeof(uint64_t));
    memcpy(iv + sizeof(uint64_t), &iv_low, sizeof(uint64_t));

    ret = EVP_CipherInit(ctx, cipher_, reinterpret_cast<const unsigned char *>(key_.data()), iv, (is_encrypt ? 1 : 0));
    if (ret != 1)
    {
        throw Exception("Failed to create cipher context.", ErrorCodes::DATA_ENCRYPTION_ERROR);
    }

    // Disable padding. After disabling padding, data size should always be
    // multiply of block size.
    ret = EVP_CIPHER_CTX_set_padding(ctx, 0);
    if (ret != 1)
    {
        throw Exception("Failed to disable padding for cipher context.", ErrorCodes::DATA_ENCRYPTION_ERROR);
    }

    uint64_t data_offset = 0;
    size_t remaining_data_size = data_size;
    int output_size = 0;
    unsigned char partial_block[AES_BLOCK_SIZE];

    // In the following we assume EVP_CipherUpdate allow in and out buffer are
    // the same, to save one memcpy. This is not specified in official man page.

    // Handle partial block at the beginning. The partial block is copied to
    // buffer to fake a full block.
    if (block_offset > 0)
    {
        size_t partial_block_size = std::min<size_t>(AES_BLOCK_SIZE - block_offset, remaining_data_size);
        memcpy(partial_block + block_offset, data, partial_block_size);
        ret = EVP_CipherUpdate(ctx, partial_block, &output_size, partial_block, AES_BLOCK_SIZE);
        if (ret != 1)
        {
            throw Exception("Crypter failed for first block, offset " + std::to_string(file_offset), ErrorCodes::DATA_ENCRYPTION_ERROR);
        }
        if (output_size != AES_BLOCK_SIZE)
        {
            throw Exception("Unexpected crypter output size for first block, expected " + std::to_string(AES_BLOCK_SIZE) + " vs actual "
                    + std::to_string(output_size),
                ErrorCodes::DATA_ENCRYPTION_ERROR);
        }
        memcpy(data, partial_block + block_offset, partial_block_size);
        data_offset += partial_block_size;
        remaining_data_size -= partial_block_size;
    }

    // Handle full blocks in the middle.
    if (remaining_data_size >= AES_BLOCK_SIZE)
    {
        size_t actual_data_size = remaining_data_size - remaining_data_size % AES_BLOCK_SIZE;
        unsigned char * full_blocks = reinterpret_cast<unsigned char *>(data) + data_offset;
        ret = EVP_CipherUpdate(ctx, full_blocks, &output_size, full_blocks, static_cast<int>(actual_data_size));
        if (ret != 1)
        {
            throw Exception("Crypter failed at offset " + std::to_string(file_offset + data_offset), ErrorCodes::DATA_ENCRYPTION_ERROR);
        }
        if (output_size != static_cast<int>(actual_data_size))
        {
            throw Exception("Unexpected crypter output size, expected " + std::to_string(actual_data_size) + " vs actual "
                    + std::to_string(output_size),
                ErrorCodes::DATA_ENCRYPTION_ERROR);
        }
        data_offset += actual_data_size;
        remaining_data_size -= actual_data_size;
    }

    // Handle partial block at the end. The partial block is copied to buffer to
    // fake a full block.
    if (remaining_data_size > 0)
    {
        assert(remaining_data_size < AES_BLOCK_SIZE);
        memcpy(partial_block, data + data_offset, remaining_data_size);
        ret = EVP_CipherUpdate(ctx, partial_block, &output_size, partial_block, AES_BLOCK_SIZE);
        if (ret != 1)
        {
            throw Exception(
                "Crypter failed for last block, offset " + std::to_string(file_offset + data_offset), ErrorCodes::DATA_ENCRYPTION_ERROR);
        }
        if (output_size != AES_BLOCK_SIZE)
        {
            throw Exception("Unexpected crypter output size for last block, expected " + std::to_string(AES_BLOCK_SIZE) + " vs actual "
                    + std::to_string(output_size),
                ErrorCodes::DATA_ENCRYPTION_ERROR);
        }
        memcpy(data + data_offset, partial_block, remaining_data_size);
    }
    FreeCipherContext(ctx);
#endif
}
} // namespace DB
