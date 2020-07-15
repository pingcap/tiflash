#pragma once

#include <Common/Exception.h>
#include <Encryption/KeyManager.h>
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

    FileEncryptionInfo getFile(const std::string & fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->getFile(fname);
        if (unlikely(r.res != FileEncryptionRes::Ok))
        {
            throw Exception("Get encryption info for file: " + fname + " meet error: " + *r.erro_msg, ErrorCodes::DATA_ENCRYPTION_ERROR);
        }
        return r;
    }

    FileEncryptionInfo newFile(const std::string & fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->newFile(fname);
        if (unlikely(r.res != FileEncryptionRes::Ok))
        {
            throw Exception("Create encryption info for file: " + fname + " meet error: " + *r.erro_msg, ErrorCodes::DATA_ENCRYPTION_ERROR);
        }
        return r;
    }

    void deleteFile(const std::string & fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->deleteFile(fname);
        if (unlikely(r.res != FileEncryptionRes::Ok))
        {
            throw Exception("Delete encryption info for file: " + fname + " meet error: " + *r.erro_msg, ErrorCodes::DATA_ENCRYPTION_ERROR);
        }
    }

    void linkFile(const std::string & src_fname, const std::string & dst_fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->linkFile(src_fname, dst_fname);
        if (unlikely(r.res != FileEncryptionRes::Ok))
        {
            throw Exception("Link encryption info from file: " + src_fname + " to file: " + dst_fname + " meet error: " + *r.erro_msg,
                ErrorCodes::DATA_ENCRYPTION_ERROR);
        }
    }

    void renameFile(const std::string & src_fname, const std::string & dst_fname) override
    {
        auto r = tiflash_instance_wrap->proxy_helper->renameFile(src_fname, dst_fname);
        if (unlikely(r.res != FileEncryptionRes::Ok))
        {
            throw Exception("Move encryption info from file: " + src_fname + " to file: " + dst_fname + " meet error: " + *r.erro_msg,
                ErrorCodes::DATA_ENCRYPTION_ERROR);
        }
    }

private:
    TiFlashServer * tiflash_instance_wrap;
};
} // namespace DB
