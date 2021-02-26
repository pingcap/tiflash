#include <Encryption/MockKeyManager.h>
#include <Storages/Transaction/FileEncryption.h>

namespace DB
{
const EncryptionMethod MockKeyManager::default_method = EncryptionMethod::Aes256Ctr;
const unsigned char MockKeyManager::default_key[]
    = "\xe4\x3e\x8e\xca\x2a\x83\xe1\x88\xfb\xd8\x02\xdc\xf3\x62\x65\x3e\x00\xee\x31\x39\xe7\xfd\x1d\x92\x20\xb1\x62\xae\xb2\xaf\x0f\x1a";
const unsigned char MockKeyManager::default_iv[] = "\x77\x9b\x82\x72\x26\xb5\x76\x50\xf7\x05\xd2\xd6\xb8\xaa\xa9\x2c";

MockKeyManager::MockKeyManager(bool encryption_enabled_)
    : MockKeyManager(default_method, String(reinterpret_cast<const char *>(default_key), 32),
        String(reinterpret_cast<const char *>(default_iv), 16), encryption_enabled_)
{}

MockKeyManager::MockKeyManager(EncryptionMethod method_, const String & key_, const String & iv, bool encryption_enabled_)
    : method{method_}, key{key_}, iv{iv}, encryption_enabled{encryption_enabled_}
{}

FileEncryptionInfo MockKeyManager::newFile(const String & fname) { return getFile(fname); }

FileEncryptionInfo MockKeyManager::getFile(const String & fname)
{
    std::ignore = fname;
    if (encryption_enabled)
    {
        auto * file_key = RawCppString::New(key);
        auto * file_iv = RawCppString::New(iv);
        FileEncryptionInfo file_info{
            FileEncryptionRes::Ok,
            method,
            file_key,
            file_iv,
            nullptr,
        };
        return file_info;
    }
    else
    {
        return FileEncryptionInfo{
            FileEncryptionRes::Ok,
            EncryptionMethod::Plaintext,
            nullptr,
            nullptr,
            nullptr,
        };
    }
}
} // namespace DB