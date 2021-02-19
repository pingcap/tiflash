#include <Encryption/DataKeyManager.h>
#include <Storages/Transaction/FileEncryption.h>
#include <Storages/Transaction/ProxyFFI.h>

namespace DB
{
DataKeyManager::DataKeyManager(EngineStoreServerWrap * tiflash_instance_wrap_) : tiflash_instance_wrap{tiflash_instance_wrap_} {}

FileEncryptionInfo DataKeyManager::getFile(const String & fname)
{
    auto r = tiflash_instance_wrap->proxy_helper->getFile(Poco::Path(fname).toString());
    if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled))
    {
        throw DB::TiFlashException("Get encryption info for file: " + fname + " meet error: " + *r.error_msg, Errors::Encryption::Internal);
    }
    return r;
}

FileEncryptionInfo DataKeyManager::newFile(const String & fname)
{
    auto r = tiflash_instance_wrap->proxy_helper->newFile(Poco::Path(fname).toString());
    if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled))
    {
        throw DB::TiFlashException(
            "Create encryption info for file: " + fname + " meet error: " + *r.error_msg, Errors::Encryption::Internal);
    }
    return r;
}

void DataKeyManager::deleteFile(const String & fname, bool throw_on_error)
{
    auto r = tiflash_instance_wrap->proxy_helper->deleteFile(Poco::Path(fname).toString());
    if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled && throw_on_error))
    {
        throw DB::TiFlashException(
            "Delete encryption info for file: " + fname + " meet error: " + *r.error_msg, Errors::Encryption::Internal);
    }
}

void DataKeyManager::linkFile(const String & src_fname, const String & dst_fname)
{
    auto r = tiflash_instance_wrap->proxy_helper->linkFile(Poco::Path(src_fname).toString(), Poco::Path(dst_fname).toString());
    if (unlikely(r.res != FileEncryptionRes::Ok && r.res != FileEncryptionRes::Disabled))
    {
        throw DB::TiFlashException("Link encryption info from file: " + src_fname + " to " + dst_fname + " meet error: " + *r.error_msg,
            Errors::Encryption::Internal);
    }
}

} // namespace DB
