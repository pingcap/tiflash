#pragma once

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <IO/Encryption/KeyManager.h>
#include <common/likely.h>

namespace DB
{
namespace ErrorCodes
{
extern const int DATA_ENCRYPTION_ERROR;
} // namespace ErrorCodes
class DataKeyManager : public KeyManager
{
public:
    DataKeyManager(TiFlashServer * tiflash_instance_wrap_) : tiflash_instance_wrap{tiflash_instance_wrap_} {}

    ~DataKeyManager() = default;

    FileEncryptionInfo getFile(const String & fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->getFile(fname);
        if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled))
        {
            throw DB::TiFlashException("Get encryption info for file: " + fname + " meet error: " + *r.erro_msg, Errors::Encryption::Internal);
        }
        return r;
    }

    FileEncryptionInfo newFile(const String & fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->newFile(fname);
        if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled))
        {
            throw DB::TiFlashException("Create encryption info for file: " + fname + " meet error: " + *r.erro_msg, Errors::Encryption::Internal);
        }
        return r;
    }

    void deleteFile(const String & fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->deleteFile(fname);
        if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled))
        {
            throw DB::TiFlashException("Delete encryption info for file: " + fname + " meet error: " + *r.erro_msg, Errors::Encryption::Internal);
        }
    }

private:
    TiFlashServer * tiflash_instance_wrap;
};
} // namespace DB
