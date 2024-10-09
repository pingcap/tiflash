// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <IO/Encryption/DataKeyManager.h>
#include <Poco/Path.h>
#include <Storages/KVStore/FFI/FileEncryption.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>

namespace DB
{
DataKeyManager::DataKeyManager(EngineStoreServerWrap * tiflash_instance_wrap_)
    : tiflash_instance_wrap{tiflash_instance_wrap_}
{}

FileEncryptionInfo DataKeyManager::getInfo(const EncryptionPath & ep)
{
    auto r = tiflash_instance_wrap->proxy_helper->getFile(Poco::Path(ep.full_path).toString());
    LOG_DEBUG(Logger::get("fff"), "KeyManager::getInfo -> {}", ep.full_path);
    if (unlikely(!r.isValid()))
    {
        throw DB::TiFlashException(
            Errors::Encryption::Internal,
            "Get encryption info for file: {} meet error: {}",
            ep.full_path,
            r.getErrorMsg());
    }
    return r;
}

FileEncryptionInfo DataKeyManager::newInfo(const EncryptionPath & ep)
{
    auto r = tiflash_instance_wrap->proxy_helper->newFile(Poco::Path(ep.full_path).toString());
    LOG_DEBUG(Logger::get("fff"), "KeyManager::newInfo -> {}", ep.full_path);
    if (unlikely(!r.isValid()))
    {
        throw DB::TiFlashException(
            Errors::Encryption::Internal,
            "Create encryption info for file: {} meet error: {}",
            ep.full_path,
            r.getErrorMsg());
    }
    return r;
}

void DataKeyManager::deleteInfo(const EncryptionPath & ep, bool throw_on_error)
{
    auto r = tiflash_instance_wrap->proxy_helper->deleteFile(Poco::Path(ep.full_path).toString());
    LOG_DEBUG(Logger::get("fff"), "KeyManager::deleteInfo -> {}", ep.full_path);
    if (unlikely(!r.isValid() && throw_on_error))
    {
        throw DB::TiFlashException(
            Errors::Encryption::Internal,
            "Delete encryption info for file: {} meet error: {}",
            ep.full_path,
            r.getErrorMsg());
    }
}

void DataKeyManager::linkInfo(const EncryptionPath & src_ep, const EncryptionPath & dst_ep)
{
    auto r = tiflash_instance_wrap->proxy_helper->linkFile(
        Poco::Path(src_ep.full_path).toString(),
        Poco::Path(dst_ep.full_path).toString());
    if (unlikely(!r.isValid()))
    {
        throw DB::TiFlashException(
            Errors::Encryption::Internal,
            "Link encryption info from file: {} to {} meet error: {}",
            src_ep.full_path,
            dst_ep.full_path,
            r.getErrorMsg());
    }
}

bool DataKeyManager::isEncryptionEnabled(KeyspaceID keyspace_id)
{
    return keyspace_id != pingcap::pd::NullspaceID
        && tiflash_instance_wrap->proxy_helper->getKeyspaceEncryption(keyspace_id);
}

} // namespace DB
