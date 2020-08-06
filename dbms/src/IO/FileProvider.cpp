#include <common/likely.h>
#include <Common/TiFlashException.h>
#include <IO/EncryptedRandomAccessFile.h>
#include <IO/EncryptedWritableFile.h>
#include <IO/FileProvider.h>
#include <IO/PosixRandomAccessFile.h>
#include <IO/PosixWritableFile.h>
#include <Poco/File.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int DATA_ENCRYPTION_ERROR;
} // namespace ErrorCodes

RandomAccessFilePtr
FileProvider::newRandomAccessFile(const String &file_path_, const EncryptionPath &encryption_path_,
                                  int flags) const
{
    RandomAccessFilePtr file = std::make_shared<PosixRandomAccessFile>(file_path_, flags);
    auto encryption_info = key_manager->getFile(encryption_path_.dir_name);
    if (encryption_info.res != FileEncryptionRes::Disabled && encryption_info.method != EncryptionMethod::Plaintext)
    {
        file = std::make_shared<EncryptedRandomAccessFile>(file, createCipherStream(encryption_info, encryption_path_));
    }
    return file;
}

WritableFilePtr FileProvider::newWritableFile(const String &file_path_, const EncryptionPath &encryption_path_,
                                              bool create_new_file_, bool create_new_encryption_info_, int flags,
                                              mode_t mode) const
{
    WritableFilePtr file = std::make_shared<PosixWritableFile>(file_path_, create_new_file_, flags, mode);
    if (encryption_enabled)
    {
        if (create_new_encryption_info_)
        {
            auto encryption_info = key_manager->newFile(encryption_path_.dir_name);
            file = std::make_shared<EncryptedWritableFile>(file, DB::createCipherStream(encryption_info, encryption_path_));
        }
        else
        {
            auto encryption_info = key_manager->getFile(encryption_path_.dir_name);
            if (unlikely(encryption_info.method == EncryptionMethod::Plaintext))
            {
                throw DB::TiFlashException("Cannot get encryption info for file: " + encryption_path_.dir_name, Errors::Encryption::Internal);
            }
            file = std::make_shared<EncryptedWritableFile>(file, createCipherStream(encryption_info, encryption_path_));
        }
    }
    return file;
}

void FileProvider::deleteFile(const String &file_path_, const EncryptionPath &encryption_path_) const
{
    Poco::File data_file(file_path_);
    if (data_file.exists())
    {
        data_file.remove();
    }
    key_manager->deleteFile(encryption_path_.dir_name);
}

void FileProvider::createEncryptionInfo(const EncryptionPath &encryption_path_) const {
    if (encryption_enabled)
    {
        key_manager->newFile(encryption_path_.dir_name);
    }
}

void FileProvider::deleteEncryptionInfo(const EncryptionPath &encryption_path_) const
{
    key_manager->deleteFile(encryption_path_.dir_name);
}

bool FileProvider::isFileEncrypted(const EncryptionPath &encryption_path_) const {
    auto encryption_info = key_manager->getFile(encryption_path_.dir_name);
    return (encryption_info.res != FileEncryptionRes::Disabled) && (encryption_info.method != EncryptionMethod::Plaintext);
}

bool FileProvider::isEncryptionEnabled() const {
    return encryption_enabled;
}

BlockAccessCipherStreamPtr createCipherStream(const FileEncryptionInfo & encryption_info_, const EncryptionPath & encryption_path_)
{
    std::string key = *(encryption_info_.key);

    const EVP_CIPHER * cipher = nullptr;
    switch (encryption_info_.method)
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
            throw Exception("Unsupported encryption method: " + std::to_string(static_cast<int>(encryption_info_.method)), ErrorCodes::NOT_IMPLEMENTED);
    }
    if (key.size() != KeySize(encryption_info_.method))
    {
        throw Exception("Encryption key size mismatch. " + std::to_string(key.size()) + "(actual) vs. " + std::to_string(KeySize(encryption_info_.method))
                + "(expected).",
            ErrorCodes::DATA_ENCRYPTION_ERROR);
    }
    if (encryption_info_.iv->size() != AES_BLOCK_SIZE)
    {
        throw Exception("iv size not equal to block cipher block size: " + std::to_string(encryption_info_.iv->size()) + "(actual) vs. "
                + std::to_string(AES_BLOCK_SIZE) + "(expected).",
            ErrorCodes::DATA_ENCRYPTION_ERROR);
    }
    auto iv_high = readBigEndian<uint64_t>(reinterpret_cast<const char *>(encryption_info_.iv->data()));
    auto iv_low = readBigEndian<uint64_t>(reinterpret_cast<const char *>(encryption_info_.iv->data() + sizeof(uint64_t)));
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

}
