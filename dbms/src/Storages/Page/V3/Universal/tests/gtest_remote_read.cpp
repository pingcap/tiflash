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
#include <Common/SyncPoint/Ctl.h>
#include <Debug/TiFlashTestEnv.h>
#include <Flash/Disaggregated/MockS3LockClient.h>
#include <Flash/Disaggregated/S3LockClient.h>
#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <IO/BaseFile/PosixWritableFile.h>
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
#include <TestUtils/TiFlashTestBasic.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>

#include <memory>

namespace DB::FailPoints
{
extern const char pause_before_page_dir_update_local_cache[];
} // namespace DB::FailPoints

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

    static ReadBufferFromRandomAccessFilePtr getReadBuffFromDataSource(const V3::CPWriteDataSourcePtr & source)
    {
        auto * raw_ds_ptr = dynamic_cast<V3::CPWriteDataSourceBlobStore *>(source.get());
        if (!raw_ds_ptr)
            return nullptr;
        return raw_ds_ptr->current_s3file_buf;
    }

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

TEST_P(UniPageStorageRemoteReadTest, WriteReadGCWithRestart)
try
{
    const String test_page_id = "aaabbb";
    /// Prepare data on remote store
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
            {.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = test_page_id, .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    writer.reset();
    for (const auto & data_path : data_paths)
    {
        uploadFile(data_path);
    }
    uploadFile(manifest_file_path);

    /// Put remote page into local
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

    // generate snapshot for reading
    auto snap0 = page_storage->getSnapshot("read");

    // Delete the page before reading from snapshot
    {
        UniversalWriteBatch wb;
        wb.disableRemoteLock();
        wb.delPage(test_page_id);
        page_storage->write(std::move(wb));
    }

    // read with snapshot
    {
        auto page = page_storage->read(test_page_id, nullptr, snap0);
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }

    // Mock restart
    reload();

    {
        // ensure the page is deleted as expected
        auto page = page_storage->read(test_page_id, nullptr, nullptr, false);
        ASSERT_FALSE(page.isValid());
    }
}
CATCH

TEST_P(UniPageStorageRemoteReadTest, WriteReadRefWithRestart)
try
{
    const String test_page_id = "aaabbb";
    /// Prepare data on remote store
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
            {.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = test_page_id, .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    writer.reset();
    for (const auto & data_path : data_paths)
    {
        uploadFile(data_path);
    }
    uploadFile(manifest_file_path);

    /// Put remote page into local
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

    /// Ref page and delete the original page
    {
        UniversalWriteBatch wb;
        wb.putRefPage("xxx_a", test_page_id);
        page_storage->write(std::move(wb));
    }
    // delete in another wb
    {
        UniversalWriteBatch wb;
        wb.delPage(test_page_id);
        page_storage->write(std::move(wb));
    }
    // Ref again
    {
        UniversalWriteBatch wb;
        wb.putRefPage("xxx_a_a", "xxx_a");
        wb.putRefPage("xxx_a_b", "xxx_a");
        page_storage->write(std::move(wb));
    }
    // delete in another wb
    {
        UniversalWriteBatch wb;
        wb.delPage("xxx_a");
        page_storage->write(std::move(wb));
    }

    // read the tail ref page
    {
        auto page = page_storage->read("xxx_a_a");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }

    // Mock restart
    reload();

    {
        // ensure the page is deleted as expected
        auto page = page_storage->read(test_page_id, nullptr, nullptr, false);
        ASSERT_FALSE(page.isValid());
        page = page_storage->read("xxx_a", nullptr, nullptr, false);
        ASSERT_FALSE(page.isValid());
        // read by the ref_id
        auto page_ref = page_storage->read("xxx_a_a");
        ASSERT_TRUE(page_ref.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page_ref.data.begin(), page_ref.data.size()));
        page_ref = page_storage->read("xxx_a_b");
        ASSERT_TRUE(page_ref.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page_ref.data.begin(), page_ref.data.size()));
    }
}
CATCH

TEST_P(UniPageStorageRemoteReadTest, MultiThreadReadUpdateRmotePage)
try
{
    const String test_page_id = "aaabbb";
    /// Prepare data on remote store
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
            {.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = test_page_id, .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    writer.reset();
    for (const auto & data_path : data_paths)
    {
        uploadFile(data_path);
    }
    uploadFile(manifest_file_path);

    /// Put remote page into local
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

    // read with snapshot
    FAIL_POINT_PAUSE(FailPoints::pause_before_page_dir_update_local_cache);
    auto th_read0 = std::async([&]() {
        auto snap0 = page_storage->getSnapshot("read0");
        auto page = page_storage->read(test_page_id, nullptr, snap0);
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
        LOG_DEBUG(log, "th_read0 finished");
    });
    auto th_read1 = std::async([&]() {
        auto snap1 = page_storage->getSnapshot("read1");
        auto page = page_storage->read(test_page_id, nullptr, snap1);
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
        LOG_DEBUG(log, "th_read1 finished");
    });
    LOG_DEBUG(log, "concurrent read block before update");

    FailPointHelper::disableFailPoint(FailPoints::pause_before_page_dir_update_local_cache);
    th_read0.get();
    th_read1.get();
    LOG_DEBUG(log, "there must be one read thread update fail");

    {
        auto page = page_storage->read(test_page_id);
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }

    // Mock restart
    reload();

    {
        auto page = page_storage->read(test_page_id);
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

    // Read the manifest from remote store(S3)
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

        // Mock that some remote page and ref to remote page is
        // ingest into page_storage.
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

    // Read the remote page "page_foo"
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

    // Read the ref page "page_foo2", which ref to the remote page "page_foo"
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

struct FilenameWithData
{
    std::string_view filename;
    std::vector<std::string_view> data;
};

/// The "CPWriteDataSourceBlobStore" should reuse the underlying
/// read buffer as possible
TEST_P(UniPageStorageRemoteReadTest, OptimizedRemoteRead)
try
{
    std::vector<FilenameWithData> test_input{
        FilenameWithData{
            .filename = "raw_data0",
            .data = {
            "The flower carriage rocked",
            "Nahida opened her eyes",
            "Said she just dreamed a dream",
            },
        },
        FilenameWithData{
            .filename = "raw_data1",
            .data = {
                "Dreamed of the day that she was born",
                "Dreamed of the day that the sages found her",
            },
        }
    };

    // Prepare a file on remote store with "data"
    for (const auto & filename_with_data : test_input)
    {
        const auto filename = String(filename_with_data.filename);
        const String full_path = fmt::format("{}/{}", getTemporaryPath(), filename);
        PosixWritableFile wf(full_path, true, -1, 0600, nullptr);
        for (const auto & d : filename_with_data.data)
        {
            wf.write(const_cast<char *>(d.data()), d.size());
        }
        wf.fsync();
        wf.close();
        uploadFile(full_path);
    }

    using namespace DB::PS::V3;
    auto data_file0 = std::make_shared<const String>(test_input[0].filename);
    auto data_file1 = std::make_shared<const String>(test_input[1].filename);
    const PageEntriesV3 all_entries{
        // test_input[0]
        PageEntryV3{
            .checkpoint_info = OptionalCheckpointInfo(
                CheckpointLocation{
                    .data_file_id = data_file0,
                    .offset_in_file = 0,
                    .size_in_file = test_input[0].data[0].size(),
                },
                true,
                true),
        },
        PageEntryV3{
            .checkpoint_info = OptionalCheckpointInfo(
                CheckpointLocation{
                    .data_file_id = data_file0,
                    .offset_in_file = test_input[0].data[0].size(),
                    .size_in_file = test_input[0].data[1].size(),
                },
                true,
                true),
        },
        PageEntryV3{
            .checkpoint_info = OptionalCheckpointInfo(
                CheckpointLocation{
                    .data_file_id = data_file0,
                    .offset_in_file = test_input[0].data[0].size() + test_input[0].data[1].size(),
                    .size_in_file = test_input[0].data[2].size(),
                },
                true,
                true),
        },
        // test_input[1]
        PageEntryV3{
            .checkpoint_info = OptionalCheckpointInfo(
                CheckpointLocation{
                    .data_file_id = data_file1,
                    .offset_in_file = 0,
                    .size_in_file = test_input[1].data[0].size(),
                },
                true,
                true),
        },
        PageEntryV3{
            .checkpoint_info = OptionalCheckpointInfo(
                CheckpointLocation{
                    .data_file_id = data_file1,
                    .offset_in_file = test_input[1].data[0].size(),
                    .size_in_file = test_input[1].data[1].size(),
                },
                true,
                true),
        },
    };
    auto gen_read_entries = [&all_entries](const std::vector<size_t> indexes) {
        UniversalPageIdAndEntries to_read;
        to_read.reserve(indexes.size());
        for (const auto index : indexes)
            to_read.emplace_back(std::make_pair(fmt::format("page_{}", index), all_entries[index]));
        return to_read;
    };

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

    /// Begin the read testing
    {
        // Read all the data in order, should reuse the underlying read buffer
        UniversalPageIdAndEntries to_read = gen_read_entries({0, 1, 2, 3, 4});

        auto data_source = PS::V3::CPWriteDataSourceBlobStore::create(
            blob_store,
            std::shared_ptr<FileProvider>(nullptr),
            5 * 1024 * 1024);
        ASSERT_EQ(nullptr, getReadBuffFromDataSource(data_source));

        Page page = data_source->read(to_read[0]);
        ASSERT_STRVIEW_EQ(page.data, test_input[0].data[0]);
        auto prev_read_buff_of_source = getReadBuffFromDataSource(data_source);
        ASSERT_NE(nullptr, prev_read_buff_of_source);

        page = data_source->read(to_read[1]);
        ASSERT_STRVIEW_EQ(page.data, test_input[0].data[1]);
        // should reuse the read buffer
        auto read_buff_of_source = getReadBuffFromDataSource(data_source);
        ASSERT_EQ(read_buff_of_source, prev_read_buff_of_source);
        prev_read_buff_of_source = read_buff_of_source;

        page = data_source->read(to_read[2]);
        ASSERT_STRVIEW_EQ(page.data, test_input[0].data[2]);
        // should reuse the read buffer
        read_buff_of_source = getReadBuffFromDataSource(data_source);
        ASSERT_EQ(read_buff_of_source, prev_read_buff_of_source);
        prev_read_buff_of_source = read_buff_of_source;

        page = data_source->read(to_read[3]);
        ASSERT_STRVIEW_EQ(page.data, test_input[1].data[0]);
        // A new filename, should NOT reuse the read buffer
        read_buff_of_source = getReadBuffFromDataSource(data_source);
        ASSERT_NE(read_buff_of_source, prev_read_buff_of_source);
        prev_read_buff_of_source = read_buff_of_source;

        page = data_source->read(to_read[4]);
        ASSERT_STRVIEW_EQ(page.data, test_input[1].data[1]);
        // should reuse the read buffer
        read_buff_of_source = getReadBuffFromDataSource(data_source);
        ASSERT_EQ(read_buff_of_source, prev_read_buff_of_source);
        prev_read_buff_of_source = read_buff_of_source;
    }
    {
        // Read page_0, page_2, page_4, page_3, page_1
        UniversalPageIdAndEntries to_read = gen_read_entries({0, 2, 4, 3, 1});

        auto data_source = PS::V3::CPWriteDataSourceBlobStore::create(
            blob_store,
            std::shared_ptr<FileProvider>(nullptr),
            5 * 1024 * 1024);
        ASSERT_EQ(nullptr, getReadBuffFromDataSource(data_source));

        Page page = data_source->read(to_read[0]);
        ASSERT_STRVIEW_EQ(page.data, test_input[0].data[0]);
        auto prev_read_buff_of_source = getReadBuffFromDataSource(data_source);
        ASSERT_NE(nullptr, prev_read_buff_of_source);

        page = data_source->read(to_read[1]);
        ASSERT_STRVIEW_EQ(page.data, test_input[0].data[2]);
        // should reuse the read buffer
        auto read_buff_of_source = getReadBuffFromDataSource(data_source);
        ASSERT_EQ(read_buff_of_source, prev_read_buff_of_source);
        prev_read_buff_of_source = read_buff_of_source;

        page = data_source->read(to_read[2]);
        ASSERT_STRVIEW_EQ(page.data, test_input[1].data[1]);
        // A new filename, should NOT reuse the read buffer
        read_buff_of_source = getReadBuffFromDataSource(data_source);
        ASSERT_NE(read_buff_of_source, prev_read_buff_of_source);
        prev_read_buff_of_source = read_buff_of_source;

        page = data_source->read(to_read[3]);
        ASSERT_STRVIEW_EQ(page.data, test_input[1].data[0]);
        // Rewind back in the same file, should NOT reuse the read buffer
        read_buff_of_source = getReadBuffFromDataSource(data_source);
        ASSERT_NE(read_buff_of_source, prev_read_buff_of_source);
        prev_read_buff_of_source = read_buff_of_source;

        page = data_source->read(to_read[4]);
        ASSERT_STRVIEW_EQ(page.data, test_input[0].data[1]);
        // A new filename, should NOT reuse the read buffer
        read_buff_of_source = getReadBuffFromDataSource(data_source);
        ASSERT_NE(read_buff_of_source, prev_read_buff_of_source);
        prev_read_buff_of_source = read_buff_of_source;
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
    // <is_encrypted, is_keyspace_encrypted>
    testing::Values(std::make_pair(false, false), std::make_pair(true, false), std::make_pair(true, true)));

} // namespace DB::PS::universal::tests
