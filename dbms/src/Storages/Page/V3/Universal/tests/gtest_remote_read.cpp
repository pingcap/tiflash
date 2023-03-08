// Copyright 2022 PingCAP, Ltd.
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

#include <IO/WriteBufferFromWritableFile.h>
#include <IO/copyData.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/CheckpointFile/CPFilesWriter.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/CheckpointFile/CPWriteDataSource.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3WritableFile.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>

namespace DB
{
namespace PS::universal::tests
{
class UniPageStorageRemoteReadTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto path = getTemporaryPath();
        remote_dir = path;
        createIfNotExist(path);
        file_provider = DB::tests::TiFlashTestEnv::getGlobalContext().getFileProvider();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        s3_client = S3::ClientFactory::instance().sharedClient();
        bucket = S3::ClientFactory::instance().bucket();
        page_storage = UniversalPageStorage::create("write", delegator, config, file_provider, s3_client, bucket);
        page_storage->restore();

        log = Logger::get("UniPageStorageRemoteReadTest");
        ASSERT_TRUE(createBucketIfNotExist());
    }

    void reload()
    {
        page_storage = reopenWithConfig(config);
    }

    std::shared_ptr<UniversalPageStorage> reopenWithConfig(const PageStorageConfig & config_)
    {
        auto path = getTemporaryPath();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto storage = UniversalPageStorage::create("test.t", delegator, config_, file_provider, s3_client, bucket);
        storage->restore();
        return storage;
    }

    void uploadFile(const String & dir, const String & f_name)
    {
        const String & full_path = dir + "/" + f_name;
        ReadBufferPtr src_buf = std::make_shared<ReadBufferFromFile>(full_path);
        S3::WriteSettings write_setting;
        WritableFilePtr dst_file = std::make_shared<S3::S3WritableFile>(s3_client, bucket, f_name, write_setting);
        WriteBufferPtr dst_buf = std::make_shared<WriteBufferFromWritableFile>(dst_file);
        copyData(*src_buf, *dst_buf);
        dst_buf->next();
        auto r = dst_file->fsync();
        ASSERT_EQ(r, 0);
    }

protected:
    bool createBucketIfNotExist()
    {
        Aws::S3::Model::CreateBucketRequest request;
        request.SetBucket(bucket);
        auto outcome = s3_client->CreateBucket(request);
        if (outcome.IsSuccess())
        {
            LOG_DEBUG(log, "Created bucket {}", bucket);
        }
        else if (outcome.GetError().GetExceptionName() == "BucketAlreadyOwnedByYou")
        {
            LOG_DEBUG(log, "Bucket {} already exist", bucket);
        }
        else
        {
            const auto & err = outcome.GetError();
            LOG_ERROR(log, "CreateBucket: {}:{}", err.GetExceptionName(), err.GetMessage());
        }
        return outcome.IsSuccess() || outcome.GetError().GetExceptionName() == "BucketAlreadyOwnedByYou";
    }

    void deleteBucket()
    {
        Aws::S3::Model::DeleteBucketRequest request;
        request.SetBucket(bucket);
        s3_client->DeleteBucket(request);
    }

protected:
    String remote_dir;
    FileProviderPtr file_provider;
    PSDiskDelegatorPtr delegator;
    std::shared_ptr<Aws::S3::S3Client> s3_client;
    String bucket;
    PageStorageConfig config;
    std::shared_ptr<UniversalPageStorage> page_storage;

    LoggerPtr log;
};

TEST_F(UniPageStorageRemoteReadTest, WriteRead)
try
{
    auto writer = PS::V3::CPFilesWriter::create({
        .data_file_path = remote_dir + "/data_1",
        .data_file_id = "data_1",
        .manifest_file_path = remote_dir + "/manifest_foo",
        .manifest_file_id = "manifest_foo",
        .data_source = PS::V3::CPWriteDataSourceFixture::create({{10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = PS::V3::universal::PageEntriesEdit{};
        edits.appendRecord({.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = "aaabbb", .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    writer->writeSuffix();
    writer.reset();
    uploadFile(remote_dir, "data_1");
    uploadFile(remote_dir, "manifest_foo");

    auto manifest_reader = PS::V3::CPManifestFileReader::create({
        .file_path = remote_dir + "/manifest_foo",
    });
    manifest_reader->readPrefix();
    PS::V3::CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(1, r.size());

        UniversalWriteBatch wb;
        wb.putPage(r[0].page_id, 0, "local data");
        wb.putRemotePage(r[0].page_id, 0, r[0].entry.checkpoint_info->data_location, std::move(r[0].entry.field_offsets));
        page_storage->write(std::move(wb));
    }

    {
        auto page = page_storage->read("aaabbb");
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
}
CATCH

TEST_F(UniPageStorageRemoteReadTest, WriteReadWithRestart)
try
{
    auto writer = PS::V3::CPFilesWriter::create({
        .data_file_path = remote_dir + "/data_1",
        .data_file_id = "data_1",
        .manifest_file_path = remote_dir + "/manifest_foo",
        .manifest_file_id = "manifest_foo",
        .data_source = PS::V3::CPWriteDataSourceFixture::create({{10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = PS::V3::universal::PageEntriesEdit{};
        edits.appendRecord({.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = "aaabbb", .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    writer->writeSuffix();
    writer.reset();
    uploadFile(remote_dir, "data_1");
    uploadFile(remote_dir, "manifest_foo");

    auto manifest_reader = PS::V3::CPManifestFileReader::create({
        .file_path = remote_dir + "/manifest_foo",
    });
    manifest_reader->readPrefix();
    PS::V3::CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(1, r.size());

        UniversalWriteBatch wb;
        wb.putRemotePage(r[0].page_id, 0, r[0].entry.checkpoint_info->data_location, std::move(r[0].entry.field_offsets));
        page_storage->write(std::move(wb));
    }

    // restart before first read
    reload();

    {
        auto page = page_storage->read("aaabbb");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }

    // clear remote data and read again to make sure local cache exists
    deleteBucket();

    reload();

    {
        auto page = page_storage->read("aaabbb");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }
}
CATCH

TEST_F(UniPageStorageRemoteReadTest, WriteReadMultiple)
try
{
    auto writer = PS::V3::CPFilesWriter::create({
        .data_file_path = remote_dir + "/data_1",
        .data_file_id = "data_1",
        .manifest_file_path = remote_dir + "/manifest_foo",
        .manifest_file_id = "manifest_foo",
        .data_source = PS::V3::CPWriteDataSourceFixture::create({{5, "Said she just dreamed a dream"},
                                                                 {10, "nahida opened her eyes"}}),
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
        edits.appendRecord({.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = page_id1, .entry = {.size = 29, .offset = 5}});
        edits.appendRecord({.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = page_id2, .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    writer->writeSuffix();
    writer.reset();
    uploadFile(remote_dir, "data_1");
    uploadFile(remote_dir, "manifest_foo");

    auto manifest_reader = PS::V3::CPManifestFileReader::create({
        .file_path = remote_dir + "/manifest_foo",
    });
    manifest_reader->readPrefix();
    PS::V3::CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(2, r.size());

        UniversalWriteBatch wb;
        wb.putRemotePage(r[0].page_id, 0, r[0].entry.checkpoint_info->data_location, std::move(r[0].entry.field_offsets));
        wb.putRemotePage(r[1].page_id, 0, r[1].entry.checkpoint_info->data_location, std::move(r[1].entry.field_offsets));
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

TEST_F(UniPageStorageRemoteReadTest, WriteReadWithFields)
try
{
    auto blob_store = PS::V3::BlobStore<PS::V3::universal::BlobStoreTrait>(getCurrentTestName(), file_provider, delegator, PS::V3::BlobConfig{});

    auto edits = PS::V3::universal::PageEntriesEdit{};
    {
        UniversalWriteBatch wb;
        wb.putPage("page_foo", 0, "The flower carriage rocked", {4, 10, 12});
        auto blob_store_edits = blob_store.write(std::move(wb), nullptr);

        edits.appendRecord({.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = "page_foo", .entry = blob_store_edits.getRecords()[0].entry});
    }

    auto writer = PS::V3::CPFilesWriter::create({
        .data_file_path = remote_dir + "/data_1",
        .data_file_id = "data_1",
        .manifest_file_path = remote_dir + "/manifest_foo",
        .manifest_file_id = "manifest_foo",
        .data_source = PS::V3::CPWriteDataSourceBlobStore::create(blob_store),
    });
    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    writer->writeEditsAndApplyCheckpointInfo(edits);
    writer->writeSuffix();
    writer.reset();
    uploadFile(remote_dir, "data_1");
    uploadFile(remote_dir, "manifest_foo");

    auto manifest_reader = PS::V3::CPManifestFileReader::create({
        .file_path = remote_dir + "/manifest_foo",
    });
    manifest_reader->readPrefix();
    PS::V3::CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(1, r.size());

        UniversalWriteBatch wb;
        wb.putRemotePage(r[0].page_id, 0, r[0].entry.checkpoint_info->data_location, std::move(r[0].entry.field_offsets));
        page_storage->write(std::move(wb));
    }

    std::vector<UniversalPageStorage::PageReadFields> page_fields;
    std::vector<size_t> read_indices = {0, 2};
    UniversalPageStorage::PageReadFields read_fields = std::make_pair("page_foo", read_indices);
    page_fields.emplace_back(read_fields);
    {
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
}
CATCH

} // namespace PS::universal::tests
} // namespace DB
