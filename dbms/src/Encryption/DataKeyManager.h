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
        auto r = tiflash_instance_wrap.proxy_helper->fn_handle_get_file(tiflash_instance_wrap.proxy_helper->proxy_ptr, BaseBuffView(fname));
        assert(r.res == FileEncryptionRes::Ok);
        return r;
    }

    FileEncryptionInfo newFile(const std::string & fname) override
    {
        auto r = tiflash_instance_wrap.proxy_helper->fn_handle_new_file(tiflash_instance_wrap.proxy_helper->proxy_ptr, BaseBuffView(fname));
        assert(r.res == FileEncryptionRes::Ok);
        return r;
    }

    void deleteFile(const std::string & fname) override
    {
        auto r = tiflash_instance_wrap.proxy_helper->fn_handle_delete_file(tiflash_instance_wrap.proxy_helper->proxy_ptr, BaseBuffView(fname));
        assert(r.res == FileEncryptionRes::Ok);
    }

    void linkFile(const std::string & src_fname, const std::string & dst_fname) override
    {
        std::ignore = src_fname;
        std::ignore = dst_fname;
    }

    void renameFile(const std::string & src_fname, const std::string & dst_fname) override
    {
        std::ignore = src_fname;
        std::ignore = dst_fname;
    }

private:
    TiFlashServer & tiflash_instance_wrap;
};
}
