#pragma once

#include <cstring>

namespace DB
{

using TiFlashRawString = std::string *;

struct TiFlashServer;

enum class FileEncryptionRes : uint8_t
{
    Disabled = 0,
    Ok,
    Error,
};

enum class EncryptionMethod : uint8_t
{
    Unknown = 0,
    Plaintext = 1,
    Aes128Ctr = 2,
    Aes192Ctr = 3,
    Aes256Ctr = 4,
};

const char * IntoEncryptionMethodName(EncryptionMethod);

struct FileEncryptionInfo
{
    FileEncryptionRes res;
    EncryptionMethod method;
    TiFlashRawString key;
    TiFlashRawString iv;
    TiFlashRawString erro_msg;

    ~FileEncryptionInfo()
    {
        if (key)
        {
            delete key;
            key = nullptr;
        }
        if (iv)
        {
            delete iv;
            iv = nullptr;
        }
        if (erro_msg)
        {
            delete erro_msg;
            erro_msg = nullptr;
        }
    }

    FileEncryptionInfo(const FileEncryptionRes & res_,
        const EncryptionMethod & method_,
        TiFlashRawString key_,
        TiFlashRawString iv_,
        TiFlashRawString erro_msg_)
        : res{res_}, method{method_}, key{key_}, iv{iv_}, erro_msg{erro_msg_}
    {}
    FileEncryptionInfo(const FileEncryptionInfo &) = delete;
    FileEncryptionInfo(FileEncryptionInfo && src)
    {
        std::memcpy(this, &src, sizeof(src));
        std::memset(&src, 0, sizeof(src));
    }
    FileEncryptionInfo & operator=(FileEncryptionInfo && src)
    {
        if (this == &src)
            return *this;
        this->~FileEncryptionInfo();
        std::memcpy(this, &src, sizeof(src));
        std::memset(&src, 0, sizeof(src));
        return *this;
    }
};

} // namespace DB
