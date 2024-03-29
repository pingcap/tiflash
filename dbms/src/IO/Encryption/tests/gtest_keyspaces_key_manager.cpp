// Copyright 2024 PingCAP, Inc.
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

#include <Common/RandomData.h>
#include <Flash/Disaggregated/MockS3LockClient.h>
#include <IO/Encryption/KeyspacesKeyManager.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <gtest/gtest.h>


namespace DB::tests
{

const String MasterKey
    = "S2Fybjphd3M6a21zOnVzLXdlc3QtMjoxNzg4NTEyMjQ1OTc6a2V5L2Y2MTcxYzUzLTY3Y2MtNGI5MS04OWY1LTYxMzcxMzgyZDFmMwAAAABIMLb8"
      "6Q/NPZ8TymCvJcKzFRQJrgAyi6IahGt0OSrWoYzR8v2cZY3qqXgtNIbjkGEIRheItR7CP5s9yMA=";

class KeyspacesKeyManagerTest : public DB::base::TiFlashStorageTestBasic
{
public:
    KeyspacesKeyManagerTest() = default;

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto path = getTemporaryPath();
        createIfNotExist(path);
        proxy_helper = new MockProxyEncryptionFFI(true, MasterKey); // NOLINT
        key_manager = std::make_shared<KeyspacesKeyManager<MockProxyEncryptionFFI>>(proxy_helper);
        file_provider = std::make_shared<FileProvider>(key_manager, true, true);
        auto delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        s3_client = S3::ClientFactory::instance().sharedTiFlashClient();

        ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));

        page_storage = UniversalPageStorage::create("write", delegator, config, file_provider);
        page_storage->restore();
        DB::tests::TiFlashTestEnv::enableS3Config();

        file_provider->setPageStoragePtrForKeyManager(page_storage);
    }

    void reload() { page_storage = reopenWithConfig(config); }

    std::shared_ptr<UniversalPageStorage> reopenWithConfig(const PageStorageConfig & config_)
    {
        auto path = getTemporaryPath();
        auto delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto storage = UniversalPageStorage::create("test.t", delegator, config_, file_provider);
        storage->restore();
        auto mock_s3lock_client = std::make_shared<S3::MockS3LockClient>(s3_client);
        storage->initLocksLocalManager(100, mock_s3lock_client);
        return storage;
    }

    void TearDown() override
    {
        DB::tests::TiFlashTestEnv::disableS3Config();
        delete proxy_helper; // NOLINT
    }

protected:
    void deleteBucket() { ::DB::tests::TiFlashTestEnv::deleteBucket(*s3_client); }

protected:
    FileProviderPtr file_provider;
    std::shared_ptr<S3::TiFlashS3Client> s3_client;
    PageStorageConfig config;
    std::shared_ptr<UniversalPageStorage> page_storage;
    MockProxyEncryptionFFI * proxy_helper = nullptr;
    std::shared_ptr<KeyspacesKeyManager<MockProxyEncryptionFFI>> key_manager;
};

TEST_F(KeyspacesKeyManagerTest, SimpleNewGetTest)
try
{
    const KeyspaceID enable_keyspace_id = 0x12345678;
    const KeyspaceID disable_keyspace_id = 0x87654321;
    proxy_helper->addKeyspace(enable_keyspace_id);
    // new enable_keyspace_id
    {
        auto info = key_manager->newInfo(EncryptionPath{"", "", enable_keyspace_id});
        ASSERT_TRUE(info.isValid());
        ASSERT_TRUE(info.isEncrypted());
        ASSERT_TRUE(file_provider->isFileEncrypted(EncryptionPath{"", "", enable_keyspace_id}));
        ASSERT_TRUE(key_manager->keyspace_id_to_key.contains(enable_keyspace_id));
        auto info2 = key_manager->getInfo(EncryptionPath{"", "", enable_keyspace_id});
        ASSERT_TRUE(key_manager->getInfo(EncryptionPath{"", "", enable_keyspace_id}).equals(info));
    }
    // get enable_keyspace_id, from page storage
    {
        key_manager->keyspace_id_to_key.remove(enable_keyspace_id);
        ASSERT_FALSE(key_manager->keyspace_id_to_key.contains(enable_keyspace_id));
        auto info = key_manager->getInfo(EncryptionPath{"", "", enable_keyspace_id});
        ASSERT_TRUE(info.isValid());
        ASSERT_TRUE(info.isEncrypted());
        ASSERT_TRUE(key_manager->keyspace_id_to_key.contains(enable_keyspace_id));
    }
    // get enable_keyspace_id, from cache
    {
        auto info = key_manager->getInfo(EncryptionPath{"", "", enable_keyspace_id});
        ASSERT_TRUE(info.isValid());
        ASSERT_TRUE(info.isEncrypted());
        ASSERT_TRUE(key_manager->keyspace_id_to_key.contains(enable_keyspace_id));
    }
    // get disable_keyspace_id
    {
        auto info = key_manager->getInfo(EncryptionPath{"", "", disable_keyspace_id});
        ASSERT_TRUE(info.isValid());
        ASSERT_FALSE(info.isEncrypted());
        ASSERT_FALSE(key_manager->keyspace_id_to_key.contains(disable_keyspace_id));
    }
    // new disable_keyspace_id
    {
        auto info = key_manager->newInfo(EncryptionPath{"", "", disable_keyspace_id});
        ASSERT_TRUE(info.isValid());
        ASSERT_FALSE(info.isEncrypted());
        ASSERT_FALSE(file_provider->isFileEncrypted(EncryptionPath{"", "", disable_keyspace_id}));
        ASSERT_FALSE(key_manager->keyspace_id_to_key.contains(disable_keyspace_id));
    }
    // get disable_keyspace_id
    {
        auto info = key_manager->getInfo(EncryptionPath{"", "", disable_keyspace_id});
        ASSERT_TRUE(info.isValid());
        ASSERT_FALSE(info.isEncrypted());
        ASSERT_FALSE(key_manager->keyspace_id_to_key.contains(disable_keyspace_id));
    }
}
CATCH


TEST_F(KeyspacesKeyManagerTest, EncryptionTest)
try
{
    const KeyspaceID enable_keyspace_id = 0x12345678;
    const KeyspaceID disable_keyspace_id = 0x87654321;
    proxy_helper->addKeyspace(enable_keyspace_id);

    UInt64 fake_page_id = 123456789;
    String random_data = DB::random::randomString(1024);
    String random_data_copy = random_data;
    auto encrypt_page_without_new_info = [&](const KeyspaceID keyspace_id) {
        file_provider->encryptPage(keyspace_id, random_data.data(), random_data.size(), fake_page_id);
    };
    // Do not call newInfo before encryptPage, should throw exception
    ASSERT_THROW(encrypt_page_without_new_info(enable_keyspace_id), Exception);

    key_manager->newInfo(EncryptionPath{"", "", enable_keyspace_id});
    file_provider->encryptPage(enable_keyspace_id, random_data.data(), random_data.size(), fake_page_id);
    // The encryption of enable_keyspace_id is enabled, so the data should be changed
    ASSERT_NE(random_data, random_data_copy);
    file_provider->decryptPage(enable_keyspace_id, random_data.data(), random_data.size(), fake_page_id);
    ASSERT_EQ(random_data, random_data_copy);

    // The encryption of disable_keyspace_id is disabled, will not throw exception
    ASSERT_NO_THROW(encrypt_page_without_new_info(disable_keyspace_id));

    key_manager->newInfo(EncryptionPath{"", "", disable_keyspace_id});
    file_provider->encryptPage(disable_keyspace_id, random_data.data(), random_data.size(), fake_page_id);
    // The encryption of disable_keyspace_id is disabled, so the data should not be changed
    ASSERT_EQ(random_data, random_data_copy);
}
CATCH

TEST_F(KeyspacesKeyManagerTest, dropEncryptionInfo)
try
{
    const KeyspaceID keyspace_id = 0x12345678;
    proxy_helper->addKeyspace(keyspace_id);

    key_manager->newInfo(EncryptionPath{"", "", keyspace_id});
    ASSERT_TRUE(key_manager->keyspace_id_to_key.contains(keyspace_id));

    file_provider->dropEncryptionInfo(keyspace_id);
    ASSERT_FALSE(key_manager->keyspace_id_to_key.contains(keyspace_id));

    ASSERT_THROW(key_manager->getInfo(EncryptionPath{"", "", keyspace_id}), Exception);

    ASSERT_NO_THROW(key_manager->newInfo(EncryptionPath{"", "", keyspace_id}));
    ASSERT_TRUE(key_manager->keyspace_id_to_key.contains(keyspace_id));
    ASSERT_NO_THROW(key_manager->getInfo(EncryptionPath{"", "", keyspace_id}));
}
CATCH

} // namespace DB::tests
