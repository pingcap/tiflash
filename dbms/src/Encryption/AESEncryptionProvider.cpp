#include <Common/Exception.h>
#include <Encryption/AESEncryptionProvider.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int DATA_ENCRYPTION_ERROR;
} // namespace ErrorCodes

BlockAccessCipherStreamPtr AESEncryptionProvider::createCipherStream(const std::string & fname, bool new_file)
{
    FileEncryptionInfo file_info;
    if (new_file)
    {
        file_info = key_manager->newFile(fname);
    }
    else
    {
        file_info = key_manager->getFile(fname);
    }
    auto & method = file_info.method;
    auto & key = file_info.key;
    auto & iv = file_info.iv;

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
    if (key->size() != KeySize(method))
    {
        throw Exception("Encryption key size mismatch. " + std::to_string(key->size()) + "(actual) vs. " + std::to_string(KeySize(method))
                + "(expected).",
            ErrorCodes::DATA_ENCRYPTION_ERROR);
    }
    if (iv->size() != AES_BLOCK_SIZE)
    {
        throw Exception("iv size not equal to block cipher block size: " + std::to_string(iv->size()) + "(actual) vs. "
                + std::to_string(AES_BLOCK_SIZE) + "(expected).",
            ErrorCodes::DATA_ENCRYPTION_ERROR);
    }
    auto iv_high = readBigEndian<uint64_t>(reinterpret_cast<const char *>(iv->data()));
    auto iv_low = readBigEndian<uint64_t>(reinterpret_cast<const char *>(iv->data() + sizeof(uint64_t)));
    return std::make_shared<AESCTRCipherStream>(cipher, *key, iv_high, iv_low);
}
} // namespace DB
