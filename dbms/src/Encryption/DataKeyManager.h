#pragma once

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <Encryption/KeyManager.h>
#include <Poco/Path.h>
#include <common/likely.h>

namespace DB
{
struct EngineStoreServerWrap;
class DataKeyManager : public KeyManager
{
public:
    DataKeyManager(EngineStoreServerWrap * tiflash_instance_wrap_);

    ~DataKeyManager() = default;

    FileEncryptionInfo getFile(const String & fname) override;

    FileEncryptionInfo newFile(const String & fname) override;

    void deleteFile(const String & fname, bool throw_on_error) override;

    void linkFile(const String & src_fname, const String & dst_fname) override;

private:
    EngineStoreServerWrap * tiflash_instance_wrap;
};
} // namespace DB
