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

#include <Common/FailPoint.h>
#include <Flash/Disaggregated/MockS3LockClient.h>
#include <Flash/Disaggregated/S3LockClient.h>
#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Buffer/ReadBufferFromRandomAccessFile.h>
#include <IO/Buffer/WriteBufferFromWritableFile.h>
#include <IO/Encryption/MockKeyManager.h>
#include <IO/copyData.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/CheckpointFile/CPFilesWriter.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/CheckpointFile/CPWriteDataSource.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3WritableFile.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>

#include <memory>


namespace DB::PS::universal::tests
{
class UniPageStorageRemoteReadTest
    : public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface<std::pair<bool, bool>>
{
public:
    UniPageStorageRemoteReadTest()
        : log(Logger::get())
    {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto path = getTemporaryPath();
        dir = path;
        data_file_path_pattern = dir + "/data_{index}";
        data_file_id_pattern = "data_{index}";
        manifest_file_path = dir + "/manifest_foo";
        manifest_file_id = "manifest_foo";
        createIfNotExist(path);
        auto [is_encrypted, is_keyspace_encrypted] = GetParam();
        KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(is_encrypted);
        file_provider = std::make_shared<FileProvider>(key_manager, is_encrypted, is_keyspace_encrypted);
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        s3_client = S3::ClientFactory::instance().sharedTiFlashClient();

        ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));

        page_storage = UniversalPageStorage::create("write", delegator, config, file_provider);
        page_storage->restore();
        DB::tests::TiFlashTestEnv::enableS3Config();
    }

    void reload() { page_storage = reopenWithConfig(config); }

    std::shared_ptr<UniversalPageStorage> reopenWithConfig(const PageStorageConfig & config_)
    {
        auto path = getTemporaryPath();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto storage = UniversalPageStorage::create("test.t", delegator, config_, file_provider);
        storage->restore();
        auto mock_s3lock_client = std::make_shared<S3::MockS3LockClient>(s3_client);
        storage->initLocksLocalManager(100, mock_s3lock_client);
        return storage;
    }

    void uploadFile(const String & full_path)
    {
        String f_name = std::filesystem::path(full_path).filename();
        ReadBufferPtr src_buf = std::make_shared<ReadBufferFromFile>(full_path);
        S3::WriteSettings write_setting;
        WritableFilePtr dst_file = std::make_shared<S3::S3WritableFile>(s3_client, f_name, write_setting);
        WriteBufferPtr dst_buf = std::make_shared<WriteBufferFromWritableFile>(dst_file);
        copyData(*src_buf, *dst_buf);
        dst_buf->next();
        auto r = dst_file->fsync();
        ASSERT_EQ(r, 0);
    }

    void TearDown() override { DB::tests::TiFlashTestEnv::disableS3Config(); }

protected:
    void deleteBucket() { ::DB::tests::TiFlashTestEnv::deleteBucket(*s3_client); }

protected:
    StoreID test_store_id = 1234;
    String dir;
    FileProviderPtr file_provider;
    PSDiskDelegatorPtr delegator;
    std::shared_ptr<S3::TiFlashS3Client> s3_client;
    PageStorageConfig config;
    std::shared_ptr<UniversalPageStorage> page_storage;

    LoggerPtr log;

    String data_file_id_pattern;
    String data_file_path_pattern;
    String manifest_file_id;
    String manifest_file_path;
};

TEST_P(UniPageStorageRemoteReadTest, WriteRead)
try
{
    auto writer = PS::V3::CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = PS::V3::CPWriteDataSourceFixture::create({{10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = PS::V3::universal::PageEntriesEdit{};
        edits.appendRecord(
            {.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = "aaabbb", .entry = {.size = 22, .offset = 10}});
        edits.appendRecord({.type = PS::V3::EditRecordType::VAR_REF, .page_id = "aaabbb2", .ori_page_id = "aaabbb"});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    writer.reset();
    for (const auto & data_path : data_paths)
    {
        uploadFile(data_path);
    }
    uploadFile(manifest_file_path);

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = PS::V3::CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    manifest_reader->readPrefix();
    PS::V3::CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(2, r.size());

        UniversalWriteBatch wb;
        wb.disableRemoteLock();
        wb.putPage(r[0].page_id, 0, "local data");
        wb.putRemotePage(
            r[0].page_id,
            0,
            r[0].entry.size,
            r[0].entry.checkpoint_info.data_location,
            std::move(r[0].entry.field_offsets));
        wb.putRefPage(r[1].page_id, r[0].page_id);
        page_storage->write(std::move(wb));
    }

    {
        auto page = page_storage->read("aaabbb");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }

    {
        auto page = page_storage->read("aaabbb2");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }

    // clear remote data and read again to make sure local cache exists
    deleteBucket();

    {
        auto page = page_storage->read("aaabbb");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }

    {
        auto page = page_storage->read("aaabbb2");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }
}
CATCH

TEST_P(UniPageStorageRemoteReadTest, WriteReadWithRestart)
try
{
    auto writer = PS::V3::CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = PS::V3::CPWriteDataSourceFixture::create({{10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = PS::V3::universal::PageEntriesEdit{};
        edits.appendRecord(
            {.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = "aaabbb", .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    writer.reset();
    for (const auto & data_path : data_paths)
    {
        uploadFile(data_path);
    }
    uploadFile(manifest_file_path);

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = PS::V3::CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    manifest_reader->readPrefix();
    PS::V3::CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(1, r.size());

        UniversalWriteBatch wb;
        wb.disableRemoteLock();
        wb.putRemotePage(
            r[0].page_id,
            0,
            r[0].entry.size,
            r[0].entry.checkpoint_info.data_location,
            std::move(r[0].entry.field_offsets));
        page_storage->write(std::move(wb));
    }

    // restart before first read
    reload();

    {
        auto page = page_storage->read("aaabbb");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }

    reload();

    // clear remote data and read again to make sure local cache exists
    deleteBucket();

    {
        auto page = page_storage->read("aaabbb");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }
}
CATCH

TEST_P(UniPageStorageRemoteReadTest, WriteReadWithRef)
try
{
    auto writer = PS::V3::CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = PS::V3::CPWriteDataSourceFixture::create({{10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = PS::V3::universal::PageEntriesEdit{};
        edits.appendRecord(
            {.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = "aaabbb", .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    writer.reset();
    for (const auto & data_path : data_paths)
    {
        uploadFile(data_path);
    }
    uploadFile(manifest_file_path);

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = PS::V3::CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    manifest_reader->readPrefix();
    PS::V3::CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(1, r.size());

        UniversalWriteBatch wb;
        wb.disableRemoteLock();
        wb.putPage(r[0].page_id, 0, "local data");
        wb.putRemotePage(
            r[0].page_id,
            0,
            r[0].entry.size,
            r[0].entry.checkpoint_info.data_location,
            std::move(r[0].entry.field_offsets));
        page_storage->write(std::move(wb));
    }

    {
        UniversalWriteBatch wb;
        wb.disableRemoteLock();
        wb.putRefPage("aaabbb2", "aaabbb");
        wb.delPage("aaabbb");
        page_storage->write(std::move(wb));
    }

    {
        auto page = page_storage->read("aaabbb2");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }

    // clear remote data and read again to make sure local cache exists
    deleteBucket();

    {
        auto page = page_storage->read("aaabbb2");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }

    // create an empty bucket because reload will try to read from S3
    ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));
    reload();

    {
        auto page = page_storage->read("aaabbb2");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }
}
CATCH

TEST_P(UniPageStorageRemoteReadTest, WriteReadMultiple)
try
{
    auto writer = PS::V3::CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = PS::V3::CPWriteDataSourceFixture::create(
            {{5, "Said she just dreamed a dream"}, {10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    UniversalPageId page_id1 = "aaabbb";
    UniversalPageId page_id2 = "aaabbb2";
    {
        auto edits = PS::V3::universal::PageEntriesEdit{};
        edits.appendRecord(
            {.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = page_id1, .entry = {.size = 29, .offset = 5}});
        edits.appendRecord(
            {.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = page_id2, .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    writer.reset();
    for (const auto & data_path : data_paths)
    {
        uploadFile(data_path);
    }
    uploadFile(manifest_file_path);

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = PS::V3::CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    manifest_reader->readPrefix();
    PS::V3::CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(2, r.size());

        UniversalWriteBatch wb;
        wb.disableRemoteLock();
        wb.putRemotePage(
            r[0].page_id,
            0,
            r[0].entry.size,
            r[0].entry.checkpoint_info.data_location,
            std::move(r[0].entry.field_offsets));
        wb.putRemotePage(
            r[1].page_id,
            0,
            r[0].entry.size,
            r[1].entry.checkpoint_info.data_location,
            std::move(r[1].entry.field_offsets));
        page_storage->write(std::move(wb));
    }

    {
        UniversalPageIds page_ids{page_id1, page_id2};
        auto page_map = page_storage->read(page_ids);
        ASSERT_EQ(page_map.size(), 2);
        auto & page1 = page_map.at(page_id1);
        ASSERT_EQ("Said she just dreamed a dream", String(page1.data.begin(), page1.data.size()));
        auto & page2 = page_map.at(page_id2);
        ASSERT_EQ("nahida opened her eyes", String(page2.data.begin(), page2.data.size()));
    }

    reload();

    {
        UniversalPageIds page_ids{page_id1, page_id2};
        auto page_map = page_storage->read(page_ids);
        ASSERT_EQ(page_map.size(), 2);
        auto & page1 = page_map.at(page_id1);
        ASSERT_EQ("Said she just dreamed a dream", String(page1.data.begin(), page1.data.size()));
        auto & page2 = page_map.at(page_id2);
        ASSERT_EQ("nahida opened her eyes", String(page2.data.begin(), page2.data.size()));
    }
}
CATCH

TEST_P(UniPageStorageRemoteReadTest, WriteReadWithFields)
try
{
    PageTypeAndConfig page_type_and_config{
        {PageType::Normal, PageTypeConfig{.heavy_gc_valid_rate = 0.5}},
        {PageType::RaftData, PageTypeConfig{.heavy_gc_valid_rate = 0.1}},
    };
    auto blob_store = PS::V3::BlobStore<PS::V3::universal::BlobStoreTrait>(
        getCurrentTestName(),
        file_provider,
        delegator,
        PS::V3::BlobConfig{},
        page_type_and_config);

    auto edits = PS::V3::universal::PageEntriesEdit{};
    {
        UniversalWriteBatch wb;
        wb.disableRemoteLock();
        wb.putPage("page_foo", 0, "The flower carriage rocked", {4, 10, 12});
        auto blob_store_edits = blob_store.write(std::move(wb));

        edits.appendRecord(
            {.type = PS::V3::EditRecordType::VAR_ENTRY,
             .page_id = "page_foo",
             .entry = blob_store_edits.getRecords()[0].entry});
        edits.appendRecord(
            {.type = PS::V3::EditRecordType::VAR_REF, .page_id = "page_foo2", .ori_page_id = "page_foo"});
    }

    auto writer = PS::V3::CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = PS::V3::CPWriteDataSourceBlobStore::create(blob_store, file_provider),
    });
    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    writer->writeEditsAndApplyCheckpointInfo(edits);
    auto data_paths = writer->writeSuffix();
    writer.reset();
    for (const auto & data_path : data_paths)
    {
        uploadFile(data_path);
    }
    uploadFile(manifest_file_path);

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = PS::V3::CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    manifest_reader->readPrefix();
    PS::V3::CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(2, r.size());

        UniversalWriteBatch wb;
        wb.disableRemoteLock();
        wb.putRemotePage(
            r[0].page_id,
            0,
            r[0].entry.size,
            r[0].entry.checkpoint_info.data_location,
            std::move(r[0].entry.field_offsets));
        wb.putRefPage(r[1].page_id, r[0].page_id);
        page_storage->write(std::move(wb));
    }

    {
        std::vector<UniversalPageStorage::PageReadFields> page_fields;
        std::vector<size_t> read_indices = {0, 2};
        UniversalPageStorage::PageReadFields read_fields = std::make_pair("page_foo", read_indices);
        page_fields.emplace_back(read_fields);
        auto page_map = page_storage->read(page_fields);
        ASSERT_EQ(page_map.size(), 1);
        auto & page = page_map.at("page_foo");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ(page.field_offsets.size(), 2);
        ASSERT_EQ(page.data.size(), 4 + 12);
        auto fields0_buf = page.getFieldData(0);
        ASSERT_EQ("The ", String(fields0_buf.begin(), fields0_buf.size()));
        auto fields3_buf = page.getFieldData(2);
        ASSERT_EQ("riage rocked", String(fields3_buf.begin(), fields3_buf.size()));
    }

    {
        std::vector<UniversalPageStorage::PageReadFields> page_fields;
        std::vector<size_t> read_indices = {0, 2};
        UniversalPageStorage::PageReadFields read_fields = std::make_pair("page_foo2", read_indices);
        page_fields.emplace_back(read_fields);
        auto page_map = page_storage->read(page_fields);
        ASSERT_EQ(page_map.size(), 1);
        auto & page = page_map.at("page_foo2");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ(page.field_offsets.size(), 2);
        ASSERT_EQ(page.data.size(), 4 + 12);
        auto fields0_buf = page.getFieldData(0);
        ASSERT_EQ("The ", String(fields0_buf.begin(), fields0_buf.size()));
        auto fields3_buf = page.getFieldData(2);
        ASSERT_EQ("riage rocked", String(fields3_buf.begin(), fields3_buf.size()));
    }
}
CATCH

TEST_P(UniPageStorageRemoteReadTest, WriteReadExternal)
try
{
    UniversalPageId page_id1{"aaabbb"};
    UniversalPageId page_id2{"aaabbb2"};
    {
        UniversalWriteBatch wb;
        wb.disableRemoteLock();
        PS::V3::CheckpointLocation data_location{
            .data_file_id = std::make_shared<String>("nahida opened her eyes"),
            .offset_in_file = 0,
            .size_in_file = 0,
        };
        wb.putRemoteExternal(page_id1, data_location);
        wb.putRefPage(page_id2, page_id1);
        page_storage->write(std::move(wb));
    }

    {
        auto location = page_storage->getCheckpointLocation(page_id1);
        ASSERT_TRUE(location.has_value());
        ASSERT_EQ(*(location->data_file_id), "nahida opened her eyes");
    }

    {
        auto location = page_storage->getCheckpointLocation(page_id2);
        ASSERT_TRUE(location.has_value());
        ASSERT_EQ(*(location->data_file_id), "nahida opened her eyes");
    }

    // restart and do another read
    reload();

    {
        auto location = page_storage->getCheckpointLocation(page_id1);
        ASSERT_TRUE(location.has_value());
        ASSERT_EQ(*(location->data_file_id), "nahida opened her eyes");
    }

    {
        auto location = page_storage->getCheckpointLocation(page_id2);
        ASSERT_TRUE(location.has_value());
        ASSERT_EQ(*(location->data_file_id), "nahida opened her eyes");
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(
    UniPageStorageRemote,
    UniPageStorageRemoteReadTest,
    testing::Values(std::make_pair(false, false), std::make_pair(true, false), std::make_pair(true, true)));

} // namespace DB::PS::universal::tests
