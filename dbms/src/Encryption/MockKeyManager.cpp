#include <Common/Exception.h>
#include <Encryption/MockKeyManager.h>
#include <Storages/Transaction/FileEncryption.h>
#include <fmt/core.h>

#include <iostream>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

const EncryptionMethod MockKeyManager::default_method = EncryptionMethod::Aes256Ctr;
const unsigned char MockKeyManager::default_key[]
    = "\xe4\x3e\x8e\xca\x2a\x83\xe1\x88\xfb\xd8\x02\xdc\xf3\x62\x65\x3e\x00\xee\x31\x39\xe7\xfd\x1d\x92\x20\xb1\x62\xae\xb2\xaf\x0f\x1a";
const unsigned char MockKeyManager::default_iv[] = "\x77\x9b\x82\x72\x26\xb5\x76\x50\xf7\x05\xd2\xd6\xb8\xaa\xa9\x2c";

MockKeyManager::MockKeyManager(bool encryption_enabled_)
    : MockKeyManager(default_method, String(reinterpret_cast<const char *>(default_key), 32), String(reinterpret_cast<const char *>(default_iv), 16), encryption_enabled_)
{}

MockKeyManager::MockKeyManager(EncryptionMethod method_, const String & key_, const String & iv, bool encryption_enabled_)
    : method{method_}
    , key{key_}
    , iv{iv}
    , encryption_enabled{encryption_enabled_}
{}

FileEncryptionInfo MockKeyManager::newFile(const String & fname)
{
    if (encryption_enabled)
    {
        files.emplace_back(fname);
    }
    return getFile(fname);
}

void MockKeyManager::deleteFile(const String & fname, bool throw_on_error)
{
    std::ignore = throw_on_error;
    if (encryption_enabled)
    {
        if (!fileExist(fname))
        {
            throw DB::Exception(fmt::format("Can't find file which name is {}", fname), DB::ErrorCodes::LOGICAL_ERROR);
        }
        for (auto iter = files.begin(); iter < files.end(); iter++)
        {
            if (*iter == fname)
            {
                files.erase(iter);
                break;
            }
        }
    }
}

void MockKeyManager::linkFile(const String & src_fname, const String & dst_fname)
{
    std::ignore = dst_fname;
    if (encryption_enabled)
    {
        if (!fileExist(src_fname))
        {
            throw DB::Exception(fmt::format("Can't find file which name is {}", src_fname), DB::ErrorCodes::LOGICAL_ERROR);
        }
        files.emplace_back(dst_fname);
    }
}

bool MockKeyManager::fileExist(const String & fname) const
{
    if (!encryption_enabled)
    {
        return true;
    }

    for (const auto & name : files)
    {
        if (name == fname)
        {
            return true;
        }
    }

    return false;
}

FileEncryptionInfo MockKeyManager::getFile(const String & fname)
{
    std::ignore = fname;
    if (encryption_enabled)
    {
        auto * file_key = RawCppString::New(key);
        auto * file_iv = RawCppString::New(iv);
        FileEncryptionInfo file_info{
            fileExist(fname) ? FileEncryptionRes::Ok : FileEncryptionRes::Disabled,
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
