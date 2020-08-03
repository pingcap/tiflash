#include <common/likely.h>
#include <Common/Exception.h>
#include <Encryption/AESEncryptionProvider.h>
#include <IO/FileProvider.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int DATA_ENCRYPTION_ERROR;
} // namespace ErrorCodes

BlockAccessCipherStreamPtr AESEncryptionProvider::createCipherStream(const EncryptionPath & encryption_path_, bool new_file)
{
    EncryptionMethod method;
    std::string key;
    std::string iv;
    if (new_file)
    {
        auto file_info = key_manager->newFile(encryption_path_.dir_name);
        method = file_info.method;
        key = *file_info.key;
        iv = *file_info.iv;
    }
    else
    {
        auto file_info = key_manager->getFile(encryption_path_.dir_name);
        if (unlikely(file_info.method == EncryptionMethod::Plaintext))
        {
            throw Exception("Cannot get encryption info for file: " + encryption_path_.dir_name);
        }
        method = file_info.method;
        key = *file_info.key;
        iv = *file_info.iv;
    }

    const EVP_CIPHER * cipher = nullptr;
    switch (method)
    {
        case EncryptionMethod::Aes128Ctr:
            cipher = EVP_aes_128_ctr();
            break;
        case EncryptionMethod::Aes192Ctr:
            cipher = EVP_aes_192_ctr();
            break;
        case EncryptionMethod::Aes256Ctr:
            cipher = EVP_aes_256_ctr();
            break;
        default:
            throw Exception("Unsupported encryption method: " + std::to_string(static_cast<int>(method)), ErrorCodes::NOT_IMPLEMENTED);
    }
    if (key.size() != KeySize(method))
    {
        throw Exception("Encryption key size mismatch. " + std::to_string(key.size()) + "(actual) vs. " + std::to_string(KeySize(method))
                + "(expected).",
            ErrorCodes::DATA_ENCRYPTION_ERROR);
    }
    if (iv.size() != AES_BLOCK_SIZE)
    {
        throw Exception("iv size not equal to block cipher block size: " + std::to_string(iv.size()) + "(actual) vs. "
                + std::to_string(AES_BLOCK_SIZE) + "(expected).",
            ErrorCodes::DATA_ENCRYPTION_ERROR);
    }
    auto iv_high = readBigEndian<uint64_t>(reinterpret_cast<const char *>(iv.data()));
    auto iv_low = readBigEndian<uint64_t>(reinterpret_cast<const char *>(iv.data() + sizeof(uint64_t)));
    // Currently all encryption info are stored in one file called file.dict.
    // Every update of file.dict will sync the whole file.
    // So when the file is too large, the update cost increases.
    // To keep the file size as small as possible, we reuse the encryption info among a group of related files.(e.g. the files of a DMFile)
    // For security reason, the same `iv` is not allowed to encrypt two different files,
    // so we combine the `iv` fetched from file.dict with the hash value of the file name to calculate the real `iv` for every file.
    if (!encryption_path_.file_name.empty())
    {
        std::size_t file_name_hash = std::hash<std::string>{}(encryption_path_.file_name);
        iv_high ^= file_name_hash;
    }
    return std::make_shared<AESCTRCipherStream>(cipher, key, iv_high, iv_low);
}
} // namespace DB
