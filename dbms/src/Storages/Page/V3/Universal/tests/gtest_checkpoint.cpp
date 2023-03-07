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

#include <Common/SyncPoint/SyncPoint.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>

#include <future>

namespace DB::FailPoints
{
extern const char force_ps_wal_compact[];
} // namespace DB::FailPoints

namespace DB::PS::universal::tests
{

using namespace DB::PS::V3;

class PSCheckpointTest : public DB::base::TiFlashStorageTestBasic
{
public:
    PSCheckpointTest()
    {
        writer_info = std::make_shared<V3::CheckpointProto::WriterInfo>();
        writer_info->set_store_id(1027);
    }

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto path = getTemporaryPath();
        dropDataOnDisk(path);
        createIfNotExist(path);
        auto file_provider = DB::tests::TiFlashTestEnv::getGlobalContext().getFileProvider();
        auto delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        page_storage = UniversalPageStorage::create("test.t", delegator, PageStorageConfig{.blob_heavy_gc_valid_rate = 1.0}, file_provider);
        page_storage->restore();

        dir = getTemporaryPath() + "/checkpoint_output/";
        dropDataOnDisk(dir);
        createIfNotExist(dir);
    }

    std::string readData(const V3::CheckpointLocation & location)
    {
        RUNTIME_CHECK(location.offset_in_file > 0);
        RUNTIME_CHECK(location.data_file_id != nullptr && !location.data_file_id->empty());

        std::string ret;
        ret.resize(location.size_in_file);

        // Note: We will introduce a DataReader when compression is added later.
        // When there is compression, we first need to seek and read compressed blocks, decompress them, and then seek to the data we want.
        // A DataReader will encapsulate this logic.
        // Currently there is no compression, so reading data is rather easy.

        auto buf = ReadBufferFromFile(dir + *location.data_file_id);
        buf.seek(location.offset_in_file);
        auto n = buf.readBig(ret.data(), location.size_in_file);
        RUNTIME_CHECK(n == location.size_in_file);

        return ret;
    }

    void dumpCheckpoint()
    {
        page_storage->dumpIncrementalCheckpoint({
            .data_file_id_pattern = "{seq}_{index}.data",
            .data_file_path_pattern = dir + "{seq}_{index}.data",
            .manifest_file_id_pattern = "{seq}.manifest",
            .manifest_file_path_pattern = dir + "{seq}.manifest",
            .writer_info = *writer_info,
        });
    }

protected:
    std::shared_ptr<UniversalPageStorage> page_storage;
    std::shared_ptr<V3::CheckpointProto::WriterInfo> writer_info;
    std::string dir; // Checkpoint output directory
    UInt64 tag = 0;
};

TEST_F(PSCheckpointTest, DumpAndRead)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("5", tag, "The flower carriage rocked");
        batch.putPage("3", tag, "Said she just dreamed a dream");
        page_storage->write(std::move(batch));
    }
    {
        UniversalWriteBatch batch;
        batch.delPage("1");
        batch.putRefPage("2", "5");
        batch.putPage("10", tag, "Nahida opened her eyes");
        batch.delPage("3");
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint();
    ASSERT_TRUE(Poco::File(dir + "6.manifest").exists());
    ASSERT_TRUE(Poco::File(dir + "6_0.data").exists());

    auto manifest_file = PosixRandomAccessFile::create(dir + "6.manifest");
    auto reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto im = CheckpointProto::StringsInternMap{};
    auto prefix = reader->readPrefix();
    auto edits = reader->readEdits(im);
    auto records = edits->getRecords();

    ASSERT_EQ(5, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("10", iter->page_id);
    ASSERT_EQ("6_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
    ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_REF, iter->type);
    ASSERT_EQ("2", iter->page_id);

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("3", iter->page_id);
    ASSERT_TRUE(iter->entry.checkpoint_info.has_value());
    ASSERT_EQ("6_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_DELETE, iter->type);
    ASSERT_EQ("3", iter->page_id);

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("5", iter->page_id);
    ASSERT_EQ("6_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
    ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info->data_location));
}
CATCH

TEST_F(PSCheckpointTest, MultiVersion)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("5", tag, "The flower carriage rocked");
        page_storage->write(std::move(batch));
    }
    {
        UniversalWriteBatch batch;
        batch.putPage("5", tag, "Said she just dreamed a dream");
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint();
    ASSERT_TRUE(Poco::File(dir + "2.manifest").exists());
    ASSERT_TRUE(Poco::File(dir + "2_0.data").exists());

    auto manifest_file = PosixRandomAccessFile::create(dir + "2.manifest");
    auto reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto im = CheckpointProto::StringsInternMap{};
    auto prefix = reader->readPrefix();
    auto edits = reader->readEdits(im);
    auto records = edits->getRecords();

    ASSERT_EQ(1, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("5", iter->page_id);
    ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info->data_location));
}
CATCH

TEST_F(PSCheckpointTest, ZeroSizedEntry)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "Said she just dreamed a dream");
        batch.putPage("7", tag, "");
        batch.putPage("14", tag, "The flower carriage rocked");
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint();
    ASSERT_TRUE(Poco::File(dir + "3.manifest").exists());
    ASSERT_TRUE(Poco::File(dir + "3_0.data").exists());

    auto manifest_file = PosixRandomAccessFile::create(dir + "3.manifest");
    auto reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto im = CheckpointProto::StringsInternMap{};
    auto prefix = reader->readPrefix();
    auto edits = reader->readEdits(im);
    auto records = edits->getRecords();

    ASSERT_EQ(3, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("14", iter->page_id);
    ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("3", iter->page_id);
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("7", iter->page_id);
    ASSERT_EQ("", readData(iter->entry.checkpoint_info->data_location));
}
CATCH

TEST_F(PSCheckpointTest, PutAndDelete)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "The flower carriage rocked");
        page_storage->write(std::move(batch));
    }
    {
        UniversalWriteBatch batch;
        batch.delPage("3");
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint();
    ASSERT_TRUE(Poco::File(dir + "2.manifest").exists());
    ASSERT_TRUE(Poco::File(dir + "2_0.data").exists());

    auto manifest_file = PosixRandomAccessFile::create(dir + "2.manifest");
    auto reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto im = CheckpointProto::StringsInternMap{};
    auto prefix = reader->readPrefix();
    auto edits = reader->readEdits(im);
    auto records = edits->getRecords();

    ASSERT_EQ(2, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("3", iter->page_id);
    ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_DELETE, iter->type);
    ASSERT_EQ("3", iter->page_id);
}
CATCH

TEST_F(PSCheckpointTest, MultiplePut)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "The flower carriage rocked");
        page_storage->write(std::move(batch));
    }
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "Nahida opened her eyes");
        page_storage->write(std::move(batch));
    }
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "Said she just dreamed a dream");
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint();
    ASSERT_TRUE(Poco::File(dir + "3.manifest").exists());
    ASSERT_TRUE(Poco::File(dir + "3_0.data").exists());

    auto manifest_file = PosixRandomAccessFile::create(dir + "3.manifest");
    auto reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto im = CheckpointProto::StringsInternMap{};
    auto prefix = reader->readPrefix();
    auto edits = reader->readEdits(im);
    auto records = edits->getRecords();

    ASSERT_EQ(1, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("3", iter->page_id);
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info->data_location));
}
CATCH

TEST_F(PSCheckpointTest, DumpWriteDump)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "The flower carriage rocked");
        batch.putPage("4", tag, "Nahida opened her eyes");
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint();
    {
        ASSERT_TRUE(Poco::File(dir + "2.manifest").exists());
        ASSERT_TRUE(Poco::File(dir + "2_0.data").exists());

        auto manifest_file = PosixRandomAccessFile::create(dir + "2.manifest");
        auto reader = CPManifestFileReader::create({
            .plain_file = manifest_file,
        });
        auto im = CheckpointProto::StringsInternMap{};
        auto prefix = reader->readPrefix();
        auto edits = reader->readEdits(im);
        auto records = edits->getRecords();

        ASSERT_EQ(2, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("3", iter->page_id);
        ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info->data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("4", iter->page_id);
        ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info->data_location));
    }

    // Write and dump again.

    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "Said she just dreamed a dream"); // Override
        batch.putPage("5", tag, "Dreamed of the day that she was born"); // New
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint();
    {
        ASSERT_TRUE(Poco::File(dir + "4.manifest").exists());
        ASSERT_TRUE(Poco::File(dir + "4_0.data").exists());

        auto manifest_file = PosixRandomAccessFile::create(dir + "4.manifest");
        auto reader = CPManifestFileReader::create({
            .plain_file = manifest_file,
        });
        auto im = CheckpointProto::StringsInternMap{};
        auto prefix = reader->readPrefix();
        auto edits = reader->readEdits(im);
        auto records = edits->getRecords();

        ASSERT_EQ(3, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("3", iter->page_id);
        ASSERT_EQ("4_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info->data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("4", iter->page_id);
        ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info->data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("5", iter->page_id);
        ASSERT_EQ("4_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("Dreamed of the day that she was born", readData(iter->entry.checkpoint_info->data_location));
    }
}
CATCH

TEST_F(PSCheckpointTest, DeleteAndGCDuringDump)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "The flower carriage rocked");
        batch.delPage("3");
        page_storage->write(std::move(batch));
    }
    {
        auto sp_before_apply = SyncPointCtl::enableInScope("before_PageStorage::dumpIncrementalCheckpoint_copyInfo");
        auto th_cp = std::async([&]() {
            dumpCheckpoint();
        });
        sp_before_apply.waitAndPause();
        page_storage->gc(/* not_skip */ true);
        sp_before_apply.next();
        th_cp.get();
    }
    {
        ASSERT_TRUE(Poco::File(dir + "2.manifest").exists());
        ASSERT_TRUE(Poco::File(dir + "2_0.data").exists());

        auto manifest_file = PosixRandomAccessFile::create(dir + "2.manifest");
        auto reader = CPManifestFileReader::create({
            .plain_file = manifest_file,
        });
        auto im = CheckpointProto::StringsInternMap{};
        auto prefix = reader->readPrefix();
        auto edits = reader->readEdits(im);
        auto records = edits->getRecords();

        ASSERT_EQ(2, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("3", iter->page_id);
        ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info->data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_DELETE, iter->type);
        ASSERT_EQ("3", iter->page_id);
    }
    {
        UniversalWriteBatch batch;
        batch.putPage("5", tag, "Said she just dreamed a dream");
        page_storage->write(std::move(batch));
    }
    {
        dumpCheckpoint();

        auto manifest_file = PosixRandomAccessFile::create(dir + "3.manifest");
        auto reader = CPManifestFileReader::create({
            .plain_file = manifest_file,
        });
        auto im = CheckpointProto::StringsInternMap{};
        auto prefix = reader->readPrefix();
        auto edits = reader->readEdits(im);
        auto records = edits->getRecords();

        ASSERT_EQ(1, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("5", iter->page_id);
        ASSERT_EQ("3_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info->data_location));
    }
}
CATCH

TEST_F(PSCheckpointTest, DeleteRefAndGCDuringDump)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "The flower carriage rocked");
        batch.delPage("3");
        batch.putPage("foo", tag, "Value");
        page_storage->write(std::move(batch));
    }
    {
        auto sp_before_apply = SyncPointCtl::enableInScope("before_PageStorage::dumpIncrementalCheckpoint_copyInfo");
        auto th_cp = std::async([&]() {
            dumpCheckpoint();
        });
        sp_before_apply.waitAndPause();

        page_storage->gc(/* not_skip */ true);
        {
            UniversalWriteBatch batch;
            batch.putRefPage("3", "foo");
            page_storage->write(std::move(batch));
        }
        sp_before_apply.next();
        th_cp.get();
    }
    {
        ASSERT_TRUE(Poco::File(dir + "3.manifest").exists());
        ASSERT_TRUE(Poco::File(dir + "3_0.data").exists());

        auto manifest_file = PosixRandomAccessFile::create(dir + "3.manifest");
        auto reader = CPManifestFileReader::create({
            .plain_file = manifest_file,
        });
        auto im = CheckpointProto::StringsInternMap{};
        auto prefix = reader->readPrefix();
        auto edits = reader->readEdits(im);
        auto records = edits->getRecords();

        ASSERT_EQ(3, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("3", iter->page_id);
        ASSERT_EQ("3_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info->data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_DELETE, iter->type);
        ASSERT_EQ("3", iter->page_id);

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("foo", iter->page_id);
        ASSERT_EQ("3_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("Value", readData(iter->entry.checkpoint_info->data_location));
    }
    {
        dumpCheckpoint();

        auto manifest_file = PosixRandomAccessFile::create(dir + "4.manifest");
        auto reader = CPManifestFileReader::create({
            .plain_file = manifest_file,
        });
        auto im = CheckpointProto::StringsInternMap{};
        auto prefix = reader->readPrefix();
        auto edits = reader->readEdits(im);
        auto records = edits->getRecords();

        ASSERT_EQ(2, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_REF, iter->type);
        ASSERT_EQ("3", iter->page_id);

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("foo", iter->page_id);
        ASSERT_EQ("3_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("Value", readData(iter->entry.checkpoint_info->data_location));
    }
}
CATCH

TEST_F(PSCheckpointTest, DeletePutAndGCDuringDump)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "The flower carriage rocked");
        batch.delPage("3");
        page_storage->write(std::move(batch));
    }
    {
        auto sp_before_apply = SyncPointCtl::enableInScope("before_PageStorage::dumpIncrementalCheckpoint_copyInfo");
        auto th_cp = std::async([&]() {
            dumpCheckpoint();
        });
        sp_before_apply.waitAndPause();

        page_storage->gc(/* not_skip */ true);
        {
            UniversalWriteBatch batch;
            batch.putPage("3", tag, "updated value");
            page_storage->write(std::move(batch));
        }
        sp_before_apply.next();
        th_cp.get();
    }
    {
        ASSERT_TRUE(Poco::File(dir + "2.manifest").exists());
        ASSERT_TRUE(Poco::File(dir + "2_0.data").exists());

        auto manifest_file = PosixRandomAccessFile::create(dir + "2.manifest");
        auto reader = CPManifestFileReader::create({
            .plain_file = manifest_file,
        });
        auto im = CheckpointProto::StringsInternMap{};
        auto prefix = reader->readPrefix();
        auto edits = reader->readEdits(im);
        auto records = edits->getRecords();

        ASSERT_EQ(2, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("3", iter->page_id);
        ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info->data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_DELETE, iter->type);
        ASSERT_EQ("3", iter->page_id);
    }
    {
        dumpCheckpoint();

        auto manifest_file = PosixRandomAccessFile::create(dir + "3.manifest");
        auto reader = CPManifestFileReader::create({
            .plain_file = manifest_file,
        });
        auto im = CheckpointProto::StringsInternMap{};
        auto prefix = reader->readPrefix();
        auto edits = reader->readEdits(im);
        auto records = edits->getRecords();

        ASSERT_EQ(1, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("3", iter->page_id);
        ASSERT_EQ("3_0.data", *iter->entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("updated value", readData(iter->entry.checkpoint_info->data_location));
    }
}
CATCH

class UniversalPageStorageServiceCheckpointTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto path = getTemporaryPath();
        dropDataOnDisk(path);
        createIfNotExist(path);
        auto delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto & global_context = DB::tests::TiFlashTestEnv::getGlobalContext();
        uni_ps_service = UniversalPageStorageService::create(global_context, "test.t", delegator, std::move(path), PageStorageConfig{.blob_heavy_gc_valid_rate = 1.0});
        s3_client = S3::ClientFactory::instance().sharedClient();
        bucket = S3::ClientFactory::instance().bucket();
        log = Logger::get("UniversalPageStorageServiceCheckpointTest");
        ASSERT_TRUE(createBucketIfNotExist());
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

    std::string readData(const V3::CheckpointLocation & location)
    {
        RUNTIME_CHECK(location.offset_in_file > 0);
        RUNTIME_CHECK(location.data_file_id != nullptr && !location.data_file_id->empty());

        std::string ret;
        ret.resize(location.size_in_file);


        // Note: We will introduce a DataReader when compression is added later.
        // When there is compression, we first need to seek and read compressed blocks, decompress them, and then seek to the data we want.
        // A DataReader will encapsulate this logic.
        // Currently there is no compression, so reading data is rather easy.

        auto data_file = S3::S3RandomAccessFile::create(*location.data_file_id);
        ReadBufferFromRandomAccessFile buf(data_file);
        buf.seek(location.offset_in_file);
        auto n = buf.readBig(ret.data(), location.size_in_file);
        RUNTIME_CHECK(n == location.size_in_file);

        return ret;
    }

protected:
    UniversalPageStorageServicePtr uni_ps_service;
    std::shared_ptr<Aws::S3::S3Client> s3_client;
    String bucket;
    UInt64 tag = 0;
    UInt64 store_id = 2;

    LoggerPtr log;
};

TEST_F(UniversalPageStorageServiceCheckpointTest, DumpAndRead)
try
{
    auto page_storage = uni_ps_service->getUniversalPageStorage();
    {
        UniversalWriteBatch batch;
        batch.putPage("5", tag, "The flower carriage rocked");
        batch.putPage("3", tag, "Said she just dreamed a dream");
        page_storage->write(std::move(batch));
    }
    {
        UniversalWriteBatch batch;
        batch.delPage("1");
        batch.putRefPage("2", "5");
        batch.putPage("10", tag, "Nahida opened her eyes");
        batch.delPage("3");
        page_storage->write(std::move(batch));
    }
    {
        auto kvstore = DB::tests::TiFlashTestEnv::getGlobalContext().getTMTContext().getKVStore();
        auto store = metapb::Store{};
        store.set_id(store_id);
        kvstore->setStore(store);
        uni_ps_service->doCheckpoint(/*force*/ true);
    }

    auto s3_manifest_name = S3::S3Filename::newCheckpointManifest(store_id, 6);
    auto manifest_file = S3::S3RandomAccessFile::create(s3_manifest_name.toFullKey());
    auto reader = CPManifestFileReader::create({.plain_file = manifest_file});
    auto im = CheckpointProto::StringsInternMap{};
    reader->readPrefix();
    auto edits = reader->readEdits(im);
    auto records = edits->getRecords();

    ASSERT_EQ(5, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("10", iter->page_id);
    ASSERT_EQ("s2/data/dat_6_0", *iter->entry.checkpoint_info->data_location.data_file_id);
    ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_REF, iter->type);
    ASSERT_EQ("2", iter->page_id);

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("3", iter->page_id);
    ASSERT_TRUE(iter->entry.checkpoint_info.has_value());
    ASSERT_EQ("s2/data/dat_6_0", *iter->entry.checkpoint_info->data_location.data_file_id);
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_DELETE, iter->type);
    ASSERT_EQ("3", iter->page_id);

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("5", iter->page_id);
    ASSERT_EQ("s2/data/dat_6_0", *iter->entry.checkpoint_info->data_location.data_file_id);
    ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info->data_location));
}
CATCH

} // namespace DB::PS::universal::tests
