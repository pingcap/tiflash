#pragma once

#include <Storages/Transaction/RaftStoreProxyFFI/EncryptionFFI.h>

#include <cstring>

namespace DB
{
using RawCppStringPtr = std::string *;
const char * IntoEncryptionMethodName(EncryptionMethod);
struct TiFlashServer;

struct FileEncryptionInfo
{
    FileEncryptionRes res;
    EncryptionMethod method;
    RawCppStringPtr key;
    RawCppStringPtr iv;
    RawCppStringPtr erro_msg;

public:
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

    FileEncryptionInfo(const FileEncryptionInfoRaw & src)
        : FileEncryptionInfo(src.res, src.method, static_cast<RawCppStringPtr>(src.key), static_cast<RawCppStringPtr>(src.iv),
            static_cast<RawCppStringPtr>(src.erro_msg))
    {}
    FileEncryptionInfo(const FileEncryptionRes & res_,
        const EncryptionMethod & method_,
        RawCppStringPtr key_,
        RawCppStringPtr iv_,
        RawCppStringPtr erro_msg_)
        : res(res_), method(method_), key(key_), iv(iv_), erro_msg(erro_msg_)
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
