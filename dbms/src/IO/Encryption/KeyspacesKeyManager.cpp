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

#include <IO/Buffer/MemoryReadWriteBuffer.h>
#include <IO/Encryption/KeyspacesKeyManager.h>
#include <Poco/Path.h>
#include <Storages/KVStore/FFI/FileEncryption.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <openssl/md5.h>


namespace DB
{

template <typename ProxyHelper>
KeyspacesKeyManager<ProxyHelper>::KeyspacesKeyManager(ProxyHelper * proxy_helper_)
    : proxy_helper(proxy_helper_)
    , keyspace_id_to_key(1024, 1024)
    , master_key(std::make_unique<MasterKey>(proxy_helper->getMasterKey()))
{}

template <typename ProxyHelper>
FileEncryptionInfo KeyspacesKeyManager<ProxyHelper>::getInfo(const EncryptionPath & ep)
{
    const auto keyspace_id = ep.keyspace_id;
    if (unlikely(keyspace_id == NullspaceID) || (likely(!proxy_helper->getKeyspaceEncryption(keyspace_id))))
    {
        return FileEncryptionInfo{
            FileEncryptionRes::Disabled,
            EncryptionMethod::Plaintext,
            nullptr,
            nullptr,
            nullptr,
        };
    }

    auto ps_write_locked = ps_write.lock();
    RUNTIME_CHECK(ps_write_locked != nullptr);
    auto load_func = [this, keyspace_id, &ps_write_locked]() -> EncryptionKeyPtr {
        const auto page_id = UniversalPageIdFormat::toEncryptionKeyPageID(keyspace_id);
        // getFile will be called after newFile, so page must exist
        Page page = ps_write_locked->read(
            page_id,
            nullptr,
            {},
            /*throw_on_not_exist*/ true);
        ReadBufferFromString rb(page.data);
        String exported;
        readStringBinary(exported, rb);
        return master_key->decryptEncryptionKey(exported);
    };
    auto [key, exist] = keyspace_id_to_key.getOrSet<>(keyspace_id, load_func);
    // Use MD5 of file path as IV
    unsigned char md5_value[MD5_DIGEST_LENGTH];
    static_assert(MD5_DIGEST_LENGTH == sizeof(uint64_t) * 2);
    String file_path = fmt::format("{}/{}", ep.full_path, ep.file_name);
    MD5(reinterpret_cast<const unsigned char *>(file_path.c_str()), file_path.size(), md5_value);
    return key->generateEncryptionInfo(String(reinterpret_cast<const char *>(md5_value), MD5_DIGEST_LENGTH));
}

template <typename ProxyHelper>
FileEncryptionInfo KeyspacesKeyManager<ProxyHelper>::newInfo(const EncryptionPath & ep)
{
    const auto keyspace_id = ep.keyspace_id;
    if (unlikely(keyspace_id == NullspaceID) || (likely(!proxy_helper->getKeyspaceEncryption(keyspace_id))))
    {
        return FileEncryptionInfo{
            FileEncryptionRes::Disabled,
            EncryptionMethod::Plaintext,
            nullptr,
            nullptr,
            nullptr,
        };
    }

    auto ps_write_locked = ps_write.lock();
    RUNTIME_CHECK(ps_write_locked != nullptr);
    auto load_func = [this, keyspace_id, ps_write_locked]() -> EncryptionKeyPtr {
        // Generate new encryption key
        auto encryption_key = master_key->generateEncryptionKey();
        // Write encrypted key to PageStorage
        UniversalWriteBatch wb;
        const auto page_id = UniversalPageIdFormat::toEncryptionKeyPageID(keyspace_id);
        MemoryWriteBuffer wb_buffer;
        writeBinary(encryption_key->exportString(), wb_buffer);
        auto read_buf = wb_buffer.tryGetReadBuffer();
        wb.putPage(page_id, 0, read_buf, wb_buffer.count());
        ps_write_locked->write(std::move(wb));
        return encryption_key;
    };
    auto [key, exist] = keyspace_id_to_key.getOrSet(keyspace_id, load_func);
    // Use MD5 of file path as IV
    unsigned char md5_value[MD5_DIGEST_LENGTH];
    static_assert(MD5_DIGEST_LENGTH == sizeof(uint64_t) * 2);
    String file_path = fmt::format("{}/{}", ep.full_path, ep.file_name);
    MD5(reinterpret_cast<const unsigned char *>(file_path.c_str()), file_path.size(), md5_value);
    return key->generateEncryptionInfo(String(reinterpret_cast<const char *>(md5_value), MD5_DIGEST_LENGTH));
}

template <typename ProxyHelper>
void KeyspacesKeyManager<ProxyHelper>::deleteInfo(const EncryptionPath & ep, bool /*throw_on_error*/)
{
    const auto keyspace_id = ep.keyspace_id;
    if (unlikely(keyspace_id == NullspaceID) || (likely(!proxy_helper->getKeyspaceEncryption(keyspace_id))))
        return;
    // do nothing
}

template <typename ProxyHelper>
void KeyspacesKeyManager<ProxyHelper>::linkInfo(const EncryptionPath & /*src_ep*/, const EncryptionPath & /*dst_ep*/)
{
    throw DB::Exception("linkFile is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

template <typename ProxyHelper>
void KeyspacesKeyManager<ProxyHelper>::deleteKey(KeyspaceID keyspace_id)
{
    if (unlikely(keyspace_id == NullspaceID) || (likely(!proxy_helper->getKeyspaceEncryption(keyspace_id))))
        return;

    auto ps_write_locked = ps_write.lock();
    RUNTIME_CHECK(ps_write_locked != nullptr);
    const auto page_id = UniversalPageIdFormat::toEncryptionKeyPageID(keyspace_id);
    UniversalWriteBatch wb;
    wb.delPage(page_id);
    ps_write_locked->write(std::move(wb));
    keyspace_id_to_key.remove(keyspace_id);
}

template <typename ProxyHelper>
bool KeyspacesKeyManager<ProxyHelper>::isEncryptionEnabled(KeyspaceID keyspace_id)
{
    return keyspace_id != NullspaceID && proxy_helper->getKeyspaceEncryption(keyspace_id);
}

template class KeyspacesKeyManager<TiFlashRaftProxyHelper>;
template class KeyspacesKeyManager<MockProxyEncryptionFFI>;

} // namespace DB
