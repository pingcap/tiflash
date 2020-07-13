#pragma once

#include <Encryption/KeyManager.h>

namespace DB {

class DataKeyManager : public KeyManager
{
public:
    DataKeyManager(TiFlashServer &tiflash_instance_wrap_) : tiflash_instance_wrap{tiflash_instance_wrap_} {

    }

    ~DataKeyManager() = default;

    FileEncryptionInfo getFile(const std::string & fname) override
    {
        auto r = tiflash_instance_wrap.proxy_helper->getFile(fname);
        assert(r.res == FileEncryptionRes::Ok);
        return r;
    }

    FileEncryptionInfo newFile(const std::string & fname) override
    {
        auto r = tiflash_instance_wrap.proxy_helper->newFile(fname);
        assert(r.res == FileEncryptionRes::Ok);
        return r;
    }

    void deleteFile(const std::string & fname) override
    {
        auto r = tiflash_instance_wrap.proxy_helper->deleteFile(fname);
        assert(r.res == FileEncryptionRes::Ok);
    }

    void linkFile(const std::string & src_fname, const std::string & dst_fname) override
    {
        auto r = tiflash_instance_wrap.proxy_helper->linkFile(src_fname, dst_fname);
        assert(r.res == FileEncryptionRes::Ok);
    }

    void renameFile(const std::string & src_fname, const std::string & dst_fname) override
    {
        auto r = tiflash_instance_wrap.proxy_helper->renameFile(src_fname, dst_fname);
        assert(r.res == FileEncryptionRes::Ok);
    }

private:
    TiFlashServer & tiflash_instance_wrap;
};
}
