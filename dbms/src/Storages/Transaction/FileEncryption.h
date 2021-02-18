#pragma once

#include <RaftStoreProxyFFI/EncryptionFFI.h>
#include <Storages/Transaction/ProxyFFICommon.h>

namespace DB
{

const char * IntoEncryptionMethodName(EncryptionMethod);
struct EngineStoreServerWrap;

struct FileEncryptionInfo : FileEncryptionInfoRaw
{
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
        if (error_msg)
        {
            delete error_msg;
            error_msg = nullptr;
        }
    }

    FileEncryptionInfo(const FileEncryptionInfoRaw & src) : FileEncryptionInfoRaw(src) {}
    FileEncryptionInfo(const FileEncryptionRes & res_,
        const EncryptionMethod & method_,
        RawCppStringPtr key_,
        RawCppStringPtr iv_,
        RawCppStringPtr error_msg_)
        : FileEncryptionInfoRaw{res_, method_, key_, iv_, error_msg_}
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
