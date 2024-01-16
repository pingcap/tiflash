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

#include <Encryption/CSEDataKeyManager.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Poco/Path.h>
#include <Storages/KVStore/FFI/FileEncryption.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <openssl/md5.h>


namespace DB
{

CSEDataKeyManager::CSEDataKeyManager(
    EngineStoreServerWrap * tiflash_instance_wrap_,
    std::weak_ptr<UniversalPageStorage> & ps_write_)
    : tiflash_instance_wrap{tiflash_instance_wrap_}
    , ps_write{ps_write_}
    , master_key(std::make_unique<MasterKey>(tiflash_instance_wrap->proxy_helper->getMasterKey()))
{}

FileEncryptionInfo CSEDataKeyManager::getInfo(const EncryptionPath & ep)
{
    const auto keyspace_id = ep.keyspace_id;
    // use likely to reduce performance impact on keyspaces without enable encryption
    if likely (keyspace_id == NullspaceID || !tiflash_instance_wrap->proxy_helper->getKeyspaceEncryption(keyspace_id))
    {
        return FileEncryptionInfo{
            FileEncryptionRes::Disabled,
            EncryptionMethod::Plaintext,
            nullptr,
            nullptr,
            nullptr,
        };
    }

    auto it = keyspace_id_to_key.find(keyspace_id);
    if (it == keyspace_id_to_key.end())
    {
        // Read encrypted key from PageStorage
        const auto page_id = UniversalPageIdFormat::toFullPageId(
            UniversalPageIdFormat::toFullPrefix(keyspace_id, StorageType::Meta, ENCRYPTION_KEY_RESERVED_NAMESPACE_ID),
            ENCRYPTION_KEY_RESERVED_PAGEU64_ID);
        // getFile will be called after newFile, so page must exist
        Page page = ps_write.lock()->read(
            page_id,
            nullptr,
            {},
            /*throw_on_not_exist*/ true);
        auto data = page.getFieldData(0);
        it = keyspace_id_to_key.emplace(keyspace_id, master_key->decryptEncryptionKey(String(data))).first;
    }
    // Use MD5 of file path as IV
    unsigned char md5_value[MD5_DIGEST_LENGTH];
    static_assert(MD5_DIGEST_LENGTH == sizeof(uint64_t) * 2);
    String file_path = fmt::format("{}/{}", ep.full_path, ep.file_name);
    MD5(reinterpret_cast<const unsigned char *>(file_path.c_str()), file_path.size(), md5_value);
    return it->second.generateEncryptionInfo(String(reinterpret_cast<const char *>(md5_value)));
}

FileEncryptionInfo CSEDataKeyManager::newInfo(const EncryptionPath & ep)
{
    const auto keyspace_id = ep.keyspace_id;
    if likely (keyspace_id == NullspaceID || !tiflash_instance_wrap->proxy_helper->getKeyspaceEncryption(keyspace_id))
    {
        return FileEncryptionInfo{
            FileEncryptionRes::Disabled,
            EncryptionMethod::Plaintext,
            nullptr,
            nullptr,
            nullptr,
        };
    }

    auto it = keyspace_id_to_key.find(keyspace_id);
    if (it == keyspace_id_to_key.end())
    {
        // Generate new encryption key
        const auto encryption_key = master_key->generateEncryptionKey();
        // Write encrypted key to PageStorage
        UniversalWriteBatch wb;
        const auto page_id = UniversalPageIdFormat::toFullPageId(
            UniversalPageIdFormat::toFullPrefix(keyspace_id, StorageType::Meta, ENCRYPTION_KEY_RESERVED_NAMESPACE_ID),
            ENCRYPTION_KEY_RESERVED_PAGEU64_ID);
        MemoryWriteBuffer wb_buffer;
        writeBinary(encryption_key.exportString(), wb_buffer);
        auto read_buf = wb_buffer.tryGetReadBuffer();
        wb.putPage(page_id, 0, read_buf, wb_buffer.count());
        ps_write.lock()->write(std::move(wb));
        it = keyspace_id_to_key.emplace(keyspace_id, encryption_key).first;
    }
    // Use MD5 of file path as IV
    unsigned char md5_value[MD5_DIGEST_LENGTH];
    static_assert(MD5_DIGEST_LENGTH == sizeof(uint64_t) * 2);
    String file_path = fmt::format("{}/{}", ep.full_path, ep.file_name);
    MD5(reinterpret_cast<const unsigned char *>(file_path.c_str()), file_path.size(), md5_value);
    return it->second.generateEncryptionInfo(String(reinterpret_cast<const char *>(md5_value)));
}

void CSEDataKeyManager::deleteInfo(const EncryptionPath & ep, bool /*throw_on_error*/)
{
    const auto keyspace_id = ep.keyspace_id;
    if likely (keyspace_id == NullspaceID || !tiflash_instance_wrap->proxy_helper->getKeyspaceEncryption(keyspace_id))
        return;
    // just remove the key from cache
    keyspace_id_to_key.erase(keyspace_id);
}

void CSEDataKeyManager::linkInfo(const EncryptionPath & /*src_ep*/, const EncryptionPath & /*dst_ep*/)
{
    throw DB::Exception("linkFile is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void CSEDataKeyManager::deleteKey(KeyspaceID keyspace_id)
{
    if likely (keyspace_id == NullspaceID || !tiflash_instance_wrap->proxy_helper->getKeyspaceEncryption(keyspace_id))
        return;
    const auto page_id = UniversalPageIdFormat::toFullPageId(
        UniversalPageIdFormat::toFullPrefix(keyspace_id, StorageType::Meta, ENCRYPTION_KEY_RESERVED_NAMESPACE_ID),
        ENCRYPTION_KEY_RESERVED_PAGEU64_ID);
    UniversalWriteBatch wb;
    wb.delPage(page_id);
    ps_write.lock()->write(std::move(wb));
    keyspace_id_to_key.erase(keyspace_id);
}

bool CSEDataKeyManager::isEncryptionEnabled(KeyspaceID keyspace_id)
{
    return keyspace_id != NullspaceID && tiflash_instance_wrap->proxy_helper->getKeyspaceEncryption(keyspace_id);
}

} // namespace DB
