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

#include <Common/SyncPoint/SyncPoint.h>
#include <Encryption/FileProvider.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <Flash/Disaggregated/MockS3LockClient.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStoreS3.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/CheckpointFile/CheckpointFiles.h>
#include <Storages/Page/V3/PageEntryCheckpointInfo.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/MockS3Client.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>

#include <future>
#include <limits>
using namespace DB::S3::tests;

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
        auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        auto delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        page_storage = UniversalPageStorage::create(
            "test.t",
            delegator,
            PageStorageConfig{.blob_heavy_gc_valid_rate = 1.0},
            file_provider);
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

    void dumpCheckpoint(
        bool upload_success = true,
        std::unordered_set<String> file_ids_to_compact = {},
        UInt64 max_data_file_size = 256 * 1024 * 1024,
        UInt64 max_edit_records_per_part = std::numeric_limits<UInt64>::max())
    {
        page_storage->dumpIncrementalCheckpoint(UniversalPageStorage::DumpCheckpointOptions{
            .data_file_id_pattern = "{seq}_{index}.data",
            .data_file_path_pattern = dir + "{seq}_{index}.data",
            .manifest_file_id_pattern = "{seq}.manifest",
            .manifest_file_path_pattern = dir + "{seq}.manifest",
            .writer_info = *writer_info,
            .must_locked_files = {},
            .persist_checkpoint = [upload_success](const PS::V3::LocalCheckpointFiles &) { return upload_success; },
            .compact_getter = [=] { return file_ids_to_compact; },
            .max_data_file_size = max_data_file_size,
            .max_edit_records_per_part = max_edit_records_per_part,
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
        batch.disableRemoteLock();
        batch.delPage("1");
        batch.putRefPage("2", "5");
        batch.putPage("10", tag, "Nahida opened her eyes");
        batch.delPage("3");
        PS::V3::CheckpointLocation data_location{
            .data_file_id = std::make_shared<String>("dt file path"),
            .offset_in_file = 0,
            .size_in_file = 0,
        };
        batch.putRemoteExternal("9", data_location);
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint();
    ASSERT_TRUE(Poco::File(dir + "7.manifest").exists());
    ASSERT_TRUE(Poco::File(dir + "7_0.data").exists());

    auto manifest_file = PosixRandomAccessFile::create(dir + "7.manifest");
    auto reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto im = CheckpointProto::StringsInternMap{};
    auto prefix = reader->readPrefix();
    auto edits = reader->readEdits(im);
    auto records = edits->getRecords();

    ASSERT_EQ(4, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("10", iter->page_id);
    ASSERT_EQ("7_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
    ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info.data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_REF, iter->type);
    ASSERT_EQ("2", iter->page_id);

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("5", iter->page_id);
    ASSERT_EQ("7_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
    ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info.data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_EXTERNAL, iter->type);
    ASSERT_EQ("9", iter->page_id);
    ASSERT_EQ("dt file path", *iter->entry.checkpoint_info.data_location.data_file_id);
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
    ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info.data_location));
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
    ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info.data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("3", iter->page_id);
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info.data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("7", iter->page_id);
    ASSERT_EQ("", readData(iter->entry.checkpoint_info.data_location));
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
    ASSERT_TRUE(!edits.has_value());
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
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info.data_location));
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
        ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info.data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("4", iter->page_id);
        ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info.data_location));
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
        ASSERT_EQ("4_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info.data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("4", iter->page_id);
        ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info.data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("5", iter->page_id);
        ASSERT_EQ("4_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Dreamed of the day that she was born", readData(iter->entry.checkpoint_info.data_location));
    }

    // Write and dump again with compact file ids

    {
        UniversalWriteBatch batch;
        batch.putPage("7", tag, "alas, but where had Lord Rukkhadevata gone"); // New
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint(/*upload_success*/ true, /*file_ids_to_compact*/ {"4_0.data"});
    {
        ASSERT_TRUE(Poco::File(dir + "5.manifest").exists());
        ASSERT_TRUE(Poco::File(dir + "5_0.data").exists());

        auto manifest_file = PosixRandomAccessFile::create(dir + "5.manifest");
        auto reader = CPManifestFileReader::create({
            .plain_file = manifest_file,
        });
        auto im = CheckpointProto::StringsInternMap{};
        auto prefix = reader->readPrefix();
        auto edits = reader->readEdits(im);
        auto records = edits->getRecords();

        ASSERT_EQ(4, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        EXPECT_EQ("3", iter->page_id);
        EXPECT_EQ("5_0.data", *iter->entry.checkpoint_info.data_location.data_file_id); // rewrite
        EXPECT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info.data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        EXPECT_EQ("4", iter->page_id);
        EXPECT_EQ("2_0.data", *iter->entry.checkpoint_info.data_location.data_file_id); // not rewrite
        EXPECT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info.data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        EXPECT_EQ("5", iter->page_id);
        EXPECT_EQ("5_0.data", *iter->entry.checkpoint_info.data_location.data_file_id); // rewrite
        EXPECT_EQ("Dreamed of the day that she was born", readData(iter->entry.checkpoint_info.data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        EXPECT_EQ("7", iter->page_id);
        EXPECT_EQ("5_0.data", *iter->entry.checkpoint_info.data_location.data_file_id); // new write
        EXPECT_EQ("alas, but where had Lord Rukkhadevata gone", readData(iter->entry.checkpoint_info.data_location));
    }
}
CATCH

TEST_F(PSCheckpointTest, DumpWriteDumpWithUploadFailure)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "The flower carriage rocked");
        batch.putPage("4", tag, "Nahida opened her eyes");
        page_storage->write(std::move(batch));
    }
    // mock that local files are generated, but uploading to remote data source is failed
    try
    {
        dumpCheckpoint(/*upload_success*/ false);
        FAIL() << "Uploading checkpoint failed would throw exception, should not come here.";
    }
    catch (...)
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
        ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info.data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("4", iter->page_id);
        ASSERT_EQ("2_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info.data_location));
    }

    // Write and dump again.
    // This time the checkpoint should also contains the
    // data in previous write

    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "Said she just dreamed a dream"); // Override
        batch.putPage("5", tag, "Dreamed of the day that she was born"); // New
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint(/*upload_success*/ true);
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
        ASSERT_EQ("4_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info.data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("4", iter->page_id);
        // 2_0.data is not uploaded and the data_location only get updated after success upload
        ASSERT_EQ("4_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info.data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("5", iter->page_id);
        ASSERT_EQ("4_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Dreamed of the day that she was born", readData(iter->entry.checkpoint_info.data_location));
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
        ASSERT_TRUE(!edits.has_value());
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
        ASSERT_EQ("3_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info.data_location));
    }
}
CATCH

TEST_F(PSCheckpointTest, DeleteAllIDsAndGCDuringDump)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("4", tag, "The flower carriage rocked");
        batch.putRefPage("3", "4");
        batch.putRefPage("5", "3");
        batch.putRefPage("6", "5");
        batch.delPage("4");
        batch.delPage("3");
        batch.delPage("5");
        batch.delPage("6");
        page_storage->write(std::move(batch));
    }

    dumpCheckpoint();

    {
        ASSERT_TRUE(Poco::File(dir + "8.manifest").exists());
        ASSERT_TRUE(Poco::File(dir + "8_0.data").exists());

        auto manifest_file = PosixRandomAccessFile::create(dir + "8.manifest");
        auto reader = CPManifestFileReader::create({
            .plain_file = manifest_file,
        });
        auto im = CheckpointProto::StringsInternMap{};
        auto prefix = reader->readPrefix();
        auto edits = reader->readEdits(im);
        ASSERT_TRUE(!edits.has_value());
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
        auto th_cp = std::async([&]() { dumpCheckpoint(); });
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

        ASSERT_EQ(1, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("foo", iter->page_id);
        ASSERT_EQ("3_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Value", readData(iter->entry.checkpoint_info.data_location));
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
        ASSERT_EQ("3_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Value", readData(iter->entry.checkpoint_info.data_location));
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
        auto th_cp = std::async([&]() { dumpCheckpoint(); });
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
        ASSERT_TRUE(!edits.has_value());
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
        ASSERT_EQ("3_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("updated value", readData(iter->entry.checkpoint_info.data_location));
    }
}
CATCH


TEST_F(PSCheckpointTest, DumpMultiFiles)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("5", tag, "The flower carriage rocked");
        batch.putPage("3", tag, "Said she just dreamed a dream");
        batch.putPage("11", tag, "Said she just dreamed a dream");
        page_storage->write(std::move(batch));
    }
    {
        UniversalWriteBatch batch;
        batch.disableRemoteLock();
        batch.delPage("1");
        batch.putRefPage("2", "5");
        batch.putPage("10", tag, "Nahida opened her eyes");
        batch.delPage("5");
        batch.delPage("11");
        PS::V3::CheckpointLocation data_location{
            .data_file_id = std::make_shared<String>("dt file path"),
            .offset_in_file = 0,
            .size_in_file = 0,
        };
        batch.putRemoteExternal("9", data_location);
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint(true, {}, 1); // One record per file.

    // valid record in data file: put 10, put 3, put 5
    ASSERT_TRUE(Poco::File(dir + "9.manifest").exists());
    ASSERT_TRUE(Poco::File(dir + "9_0.data").exists());
    ASSERT_TRUE(Poco::File(dir + "9_1.data").exists());
    ASSERT_TRUE(Poco::File(dir + "9_2.data").exists());
    ASSERT_FALSE(Poco::File(dir + "9_3.data").exists());

    auto manifest_file = PosixRandomAccessFile::create(dir + "9.manifest");
    auto reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto im = CheckpointProto::StringsInternMap{};
    auto prefix = reader->readPrefix();
    auto edits = reader->readEdits(im);
    auto records = edits->getRecords();

    ASSERT_EQ(6, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("10", iter->page_id);
    ASSERT_EQ("9_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
    ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info.data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_REF, iter->type);
    ASSERT_EQ("2", iter->page_id);

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("3", iter->page_id);
    ASSERT_TRUE(iter->entry.checkpoint_info.has_value());
    ASSERT_EQ("9_1.data", *iter->entry.checkpoint_info.data_location.data_file_id);
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info.data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("5", iter->page_id);
    ASSERT_EQ("9_2.data", *iter->entry.checkpoint_info.data_location.data_file_id);
    ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info.data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_DELETE, iter->type);
    ASSERT_EQ("5", iter->page_id);

    iter++;
    ASSERT_EQ(EditRecordType::VAR_EXTERNAL, iter->type);
    ASSERT_EQ("9", iter->page_id);
    ASSERT_EQ("dt file path", *iter->entry.checkpoint_info.data_location.data_file_id);
}
CATCH

TEST_F(PSCheckpointTest, ManifestRecordParts)
try
{
    {
        UniversalWriteBatch batch;
        batch.putPage("5", tag, "The flower carriage rocked");
        batch.putPage("3", tag, "Said she just dreamed a dream");
        batch.putPage("11", tag, "Said she just dreamed a dream");
        page_storage->write(std::move(batch));
    }
    {
        UniversalWriteBatch batch;
        batch.disableRemoteLock();
        batch.delPage("1");
        batch.putRefPage("2", "5");
        batch.putPage("10", tag, "Nahida opened her eyes");
        batch.delPage("11");
        batch.delPage("5");
        PS::V3::CheckpointLocation data_location{
            .data_file_id = std::make_shared<String>("dt file path"),
            .offset_in_file = 0,
            .size_in_file = 0,
        };
        batch.putRemoteExternal("9", data_location);
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint(
        /*upload_success*/ true,
        /*file_ids_to_compact*/ {},
        /*max_data_file_size*/ 1,
        /*max_edit_records_per_part*/ 1);

    ASSERT_TRUE(Poco::File(dir + "9.manifest").exists());
    auto manifest_file = PosixRandomAccessFile::create(dir + "9.manifest");
    auto reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto im = CheckpointProto::StringsInternMap{};
    auto prefix = reader->readPrefix();
    UInt64 record_count = 0;
    while (true)
    {
        auto edits = reader->readEdits(im);
        if (!edits)
        {
            break;
        }
        const auto & records = edits->getRecords();
        record_count += records.size();
        ASSERT_EQ(records.size(), 1);

        auto iter = records.begin();
        switch (record_count)
        {
        case 1:
            ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
            ASSERT_EQ("10", iter->page_id);
            ASSERT_EQ("9_0.data", *iter->entry.checkpoint_info.data_location.data_file_id);
            ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info.data_location));
            break;
        case 2:
            ASSERT_EQ(EditRecordType::VAR_REF, iter->type);
            ASSERT_EQ("2", iter->page_id);
            break;
        case 3:
            ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
            ASSERT_EQ("3", iter->page_id);
            ASSERT_TRUE(iter->entry.checkpoint_info.has_value());
            ASSERT_EQ("9_1.data", *iter->entry.checkpoint_info.data_location.data_file_id);
            ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.checkpoint_info.data_location));
            break;
        case 4:
            ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
            ASSERT_EQ("5", iter->page_id);
            ASSERT_EQ("9_2.data", *iter->entry.checkpoint_info.data_location.data_file_id);
            ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info.data_location));
            break;
        case 5:
            ASSERT_EQ(EditRecordType::VAR_DELETE, iter->type);
            ASSERT_EQ("5", iter->page_id);
            break;
        case 6:
            ASSERT_EQ(EditRecordType::VAR_EXTERNAL, iter->type);
            ASSERT_EQ("9", iter->page_id);
            ASSERT_EQ("dt file path", *iter->entry.checkpoint_info.data_location.data_file_id);
            break;
        default:
            FAIL();
            break;
        }
    }
    ASSERT_EQ(6, record_count);
}
CATCH

class UniversalPageStorageServiceCheckpointTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        DB::tests::TiFlashTestEnv::enableS3Config();
        auto & global_context = DB::tests::TiFlashTestEnv::getGlobalContext();
        auto & settings = global_context.getSettingsRef();
        old_remote_checkpoint_only_upload_manifest = settings.remote_checkpoint_only_upload_manifest;
        settings.remote_checkpoint_only_upload_manifest = false;
        uni_ps_service = newService();
        log = Logger::get("UniversalPageStorageServiceCheckpointTest");
        s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));
    }

    void TearDown() override
    {
        auto & global_context = DB::tests::TiFlashTestEnv::getGlobalContext();
        auto & settings = global_context.getSettingsRef();
        settings.remote_checkpoint_only_upload_manifest = old_remote_checkpoint_only_upload_manifest;
        DB::tests::TiFlashTestEnv::disableS3Config();
    }

    static UniversalPageStorageServicePtr newService()
    {
        auto path = getTemporaryPath();
        auto delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto & global_context = DB::tests::TiFlashTestEnv::getGlobalContext();
        return UniversalPageStorageService::createForTest(
            global_context,
            "test.t",
            delegator,
            PageStorageConfig{.blob_heavy_gc_valid_rate = 1.0});
    }

protected:
    static std::string readData(const V3::CheckpointLocation & location)
    {
        RUNTIME_CHECK(location.offset_in_file > 0);
        RUNTIME_CHECK(location.data_file_id != nullptr && !location.data_file_id->empty());

        std::string ret;
        ret.resize(location.size_in_file);

        // Note: We will introduce a DataReader when compression is added later.
        // When there is compression, we first need to seek and read compressed blocks, decompress them, and then seek to the data we want.
        // A DataReader will encapsulate this logic.
        // Currently there is no compression, so reading data is rather easy.

        // parse from lockkey to data_file_key
        auto data_file_key = S3::S3FilenameView::fromKey(*location.data_file_id).asDataFile().toFullKey();

        auto data_file = S3::S3RandomAccessFile::create(data_file_key);
        ReadBufferFromRandomAccessFile buf(data_file);
        buf.seek(location.offset_in_file);
        auto n = buf.readBig(ret.data(), location.size_in_file);
        RUNTIME_CHECK(n == location.size_in_file);

        return ret;
    }

protected:
    UniversalPageStorageServicePtr uni_ps_service;
    std::shared_ptr<S3::TiFlashS3Client> s3_client;
    UInt64 tag = 0;
    UInt64 store_id = 2;

    bool old_remote_checkpoint_only_upload_manifest;

    LoggerPtr log;
};

TEST_F(UniversalPageStorageServiceCheckpointTest, DumpAndRead)
try
{
    auto page_storage = uni_ps_service->getUniversalPageStorage();
    auto store_info = metapb::Store{};
    store_info.set_id(store_id);
    auto s3lock_client = std::make_shared<S3::MockS3LockClient>(s3_client);
    auto remote_store = std::make_shared<DM::Remote::DataStoreS3>(::DB::tests::TiFlashTestEnv::getMockFileProvider());
    // Mock normal writes
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
    uni_ps_service->uploadCheckpointImpl(store_info, s3lock_client, remote_store, false);

    { // check the first manifest
        UInt64 upload_seq = 1;
        auto s3_manifest_name = S3::S3Filename::newCheckpointManifest(store_id, upload_seq);
        auto manifest_file = S3::S3RandomAccessFile::create(s3_manifest_name.toFullKey());
        auto reader = CPManifestFileReader::create({.plain_file = manifest_file});
        auto im = CheckpointProto::StringsInternMap{};
        reader->readPrefix();
        auto edits = reader->readEdits(im);
        auto records = edits->getRecords();

        ASSERT_EQ(3, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("10", iter->page_id);
        ASSERT_EQ(
            "lock/s2/dat_1_0.lock_s2_1",
            *iter->entry.checkpoint_info.data_location.data_file_id); // this is the lock key to CPDataFile
        ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info.data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_REF, iter->type);
        ASSERT_EQ("2", iter->page_id);

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("5", iter->page_id);
        ASSERT_EQ(
            "lock/s2/dat_1_0.lock_s2_1",
            *iter->entry.checkpoint_info.data_location.data_file_id); // this is the lock key to CPDataFile
        ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info.data_location));
    } // check the first manifest

    // Mock normal writes && FAP ingest remote page
    {
        UniversalWriteBatch batch;
        batch.putPage("20", tag, "Dreamed of the day that she was born");
        page_storage->write(std::move(batch));
    }
    StoreID another_store_id = 99;
    const auto ingest_from_data_file = S3::S3Filename::newCheckpointData(another_store_id, 100, 1);
    const auto ingest_from_dtfile
        = S3::S3Filename::fromDMFileOID(S3::DMFileOID{.store_id = another_store_id, .table_id = 50, .file_id = 999});
    {
        // create object on s3 for locking
        S3::uploadEmptyFile(*s3_client, ingest_from_data_file.toFullKey());
        S3::uploadEmptyFile(
            *s3_client,
            fmt::format(
                "{}/{}",
                ingest_from_dtfile.toFullKey(),
                DM::DMFileMetaV2::metaFileName(/* meta_version= */ 0)));

        UniversalWriteBatch batch;

        batch.putRemotePage(
            "21",
            tag,
            1024,
            PS::V3::CheckpointLocation{
                .data_file_id = std::make_shared<String>(ingest_from_data_file.toFullKey()),
                .offset_in_file = 1024,
                .size_in_file = 1024,
            },
            {});
        batch.putRemoteExternal(
            "22",
            PS::V3::CheckpointLocation{
                .data_file_id = std::make_shared<String>(ingest_from_dtfile.toFullKey()),
                .offset_in_file = 0,
                .size_in_file = 0,
            });
        page_storage->write(std::move(batch));
    }
    uni_ps_service->uploadCheckpointImpl(store_info, s3lock_client, remote_store, false);

    { // check the second manifest
        UInt64 upload_seq = 2;
        auto s3_manifest_name = S3::S3Filename::newCheckpointManifest(store_id, upload_seq);
        auto manifest_file = S3::S3RandomAccessFile::create(s3_manifest_name.toFullKey());
        auto reader = CPManifestFileReader::create({.plain_file = manifest_file});
        auto im = CheckpointProto::StringsInternMap{};
        reader->readPrefix();
        auto edits = reader->readEdits(im);
        auto records = edits->getRecords();

        ASSERT_EQ(3 + 3, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("10", iter->page_id);
        ASSERT_EQ(
            "lock/s2/dat_1_0.lock_s2_1",
            *iter->entry.checkpoint_info.data_location.data_file_id); // this is the lock key to CPDataFile
        ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.checkpoint_info.data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_REF, iter->type);
        ASSERT_EQ("2", iter->page_id);

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("20", iter->page_id);
        ASSERT_EQ(
            "lock/s2/dat_2_0.lock_s2_2",
            *iter->entry.checkpoint_info.data_location.data_file_id); // this is the lock key to second CPDataFile

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("21", iter->page_id);
        ASSERT_EQ(
            "lock/s99/dat_100_1.lock_s2_2",
            *iter->entry.checkpoint_info.data_location.data_file_id); // this is the lock key to CPDataFile

        iter++;
        ASSERT_EQ(EditRecordType::VAR_EXTERNAL, iter->type);
        ASSERT_EQ("22", iter->page_id);
        ASSERT_EQ(
            "lock/s99/t_50/dmf_999.lock_s2_2",
            *iter->entry.checkpoint_info.data_location.data_file_id); // this is the lock key to DMFile
        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("5", iter->page_id);
        ASSERT_EQ(
            "lock/s2/dat_1_0.lock_s2_1",
            *iter->entry.checkpoint_info.data_location.data_file_id); // this is the lock key to CPDataFile
        ASSERT_EQ("The flower carriage rocked", readData(iter->entry.checkpoint_info.data_location));
    } // check the second manifest

    {
        // Persist some new WriteBatch but checkpoint is not uploaded
        {
            UniversalWriteBatch batch;
            batch.delPage("2"); // delete
            batch.putPage("10", tag, "new version data");
            batch.putPage("30", tag, "testing"); // new page_id
            batch.putRemoteExternal(
                "31",
                PS::V3::CheckpointLocation{
                    .data_file_id = std::make_shared<String>(ingest_from_dtfile.toFullKey()),
                    .offset_in_file = 0,
                    .size_in_file = 0,
                }); // new ingest id
            batch.putRemotePage(
                "32",
                tag,
                128,
                PS::V3::CheckpointLocation{
                    .data_file_id = std::make_shared<String>(ingest_from_data_file.toFullKey()),
                    .offset_in_file = 2048,
                    .size_in_file = 128,
                },
                {}); // new ingest id
            page_storage->write(std::move(batch));
        }
    }

    // mock restart
    auto new_service = newService();
    EXPECT_EQ(new_service->uni_page_storage->last_checkpoint_sequence, 0);
    new_service->uni_page_storage->initLocksLocalManager(store_id, s3lock_client);
    EXPECT_EQ(new_service->uni_page_storage->last_checkpoint_sequence, 9);
    auto upload_info = new_service->uni_page_storage->allocateNewUploadLocksInfo();
    EXPECT_EQ(upload_info.upload_sequence, 3);

    // Check that data_location are restored from S3 latest manifest
    {
        auto & restored_page_directory = new_service->uni_page_storage->page_directory;
        auto snap = restored_page_directory->createSnapshot("");
        // page_id "2" is deleted
        EXPECT_EQ(restored_page_directory->numPages(), 8)
            << fmt::format("{}", restored_page_directory->getAllPageIds());

        auto restored_entry = restored_page_directory->getByID("10", snap);
        ASSERT_FALSE(restored_entry.second.checkpoint_info.has_value()); // new version is not persisted to S3

        restored_entry = restored_page_directory->getByID("20", snap);
        ASSERT_TRUE(restored_entry.second.checkpoint_info.has_value());
        EXPECT_EQ(
            *restored_entry.second.checkpoint_info.data_location.data_file_id,
            "lock/s2/dat_2_0.lock_s2_2"); // second checkpoint
        EXPECT_EQ(restored_entry.second.checkpoint_info.is_local_data_reclaimed, false);

        restored_entry = restored_page_directory->getByID("21", snap);
        ASSERT_TRUE(restored_entry.second.checkpoint_info.has_value());
        EXPECT_EQ(*restored_entry.second.checkpoint_info.data_location.data_file_id, "lock/s99/dat_100_1.lock_s2_2");
        EXPECT_EQ(restored_entry.second.checkpoint_info.is_local_data_reclaimed, true);

        restored_entry = restored_page_directory->getByID("22", snap);
        ASSERT_TRUE(restored_entry.second.checkpoint_info.has_value());
        EXPECT_EQ(*restored_entry.second.checkpoint_info.data_location.data_file_id, "lock/s99/t_50/dmf_999.lock_s2_2");
        EXPECT_EQ(restored_entry.second.checkpoint_info.is_local_data_reclaimed, true);

        restored_entry = restored_page_directory->getByID("5", snap);
        ASSERT_TRUE(restored_entry.second.checkpoint_info.has_value());
        EXPECT_EQ(*restored_entry.second.checkpoint_info.data_location.data_file_id, "lock/s2/dat_1_0.lock_s2_1");
        EXPECT_EQ(restored_entry.second.checkpoint_info.is_local_data_reclaimed, false);

        // These ID are persisted in UniPS but not uploaded to S3 manifest
        restored_entry = restored_page_directory->getByID("30", snap);
        ASSERT_FALSE(restored_entry.second.checkpoint_info.has_value()); // not persisted to S3
        EXPECT_EQ(restored_entry.second.checkpoint_info.is_local_data_reclaimed, false);

        restored_entry = restored_page_directory->getByID("31", snap);
        ASSERT_TRUE(restored_entry.second.checkpoint_info.has_value());
        EXPECT_EQ(
            *restored_entry.second.checkpoint_info.data_location.data_file_id,
            "lock/s99/t_50/dmf_999.lock_s2_3"); // restored from local WAL
        EXPECT_EQ(restored_entry.second.checkpoint_info.is_local_data_reclaimed, true);

        restored_entry = restored_page_directory->getByID("32", snap);
        ASSERT_TRUE(restored_entry.second.checkpoint_info.has_value());
        EXPECT_EQ(
            *restored_entry.second.checkpoint_info.data_location.data_file_id,
            "lock/s99/dat_100_1.lock_s2_3"); // restored from local WAL
        EXPECT_EQ(restored_entry.second.checkpoint_info.is_local_data_reclaimed, true);
    }
}
CATCH


TEST_F(UniversalPageStorageServiceCheckpointTest, DumpFail)
try
{
    auto page_storage = uni_ps_service->getUniversalPageStorage();
    auto store_info = metapb::Store{};
    store_info.set_id(store_id);
    auto s3lock_client = std::make_shared<S3::MockS3LockClient>(s3_client);
    auto remote_store = std::make_shared<DM::Remote::DataStoreS3>(::DB::tests::TiFlashTestEnv::getMockFileProvider());
    // Mock normal writes
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
    MockS3Client::setPutObjectStatus(MockS3Client::S3Status::FAILED);
    SCOPE_EXIT({ MockS3Client::setPutObjectStatus(MockS3Client::S3Status::NORMAL); });
    try
    {
        uni_ps_service->uploadCheckpointImpl(store_info, s3lock_client, remote_store, false);
        FAIL() << "Exception should be thrown above, should not come here.";
    }
    catch (...)
    {
        auto & global_context = DB::tests::TiFlashTestEnv::getGlobalContext();
        const auto & tmp_path = global_context.getTemporaryPath();
        std::vector<String> short_names;
        Poco::File(tmp_path).list(short_names);
        for (const auto & name : short_names)
        {
            ASSERT_FALSE(startsWith(name, UniversalPageStorageService::checkpoint_dirname_prefix)) << name;
        }
    }
}
CATCH

TEST_F(UniversalPageStorageServiceCheckpointTest, removeAllLocalCheckpointFiles)
try
{
    auto list_files = [](const String & dir) {
        std::vector<String> filenames;
        Poco::File(dir).list(filenames);
        return std::set<String>(filenames.begin(), filenames.end());
    };

    auto remove_file = [](const String & fname) {
        Poco::File f(fname);
        if (f.exists())
        {
            f.remove(true);
        }
    };

    auto & global_context = DB::tests::TiFlashTestEnv::getGlobalContext();
    const auto & tmp_path = global_context.getTemporaryPath();

    // Clean old data if necessary.
    auto cp_dir1 = uni_ps_service->getCheckpointLocalDir(1);
    remove_file(cp_dir1.toString());
    auto cp_dir2 = uni_ps_service->getCheckpointLocalDir(2);
    remove_file(cp_dir2.toString());
    remove_file(tmp_path + "/" + "not_checkpoint");

    auto fnames0 = list_files(tmp_path);

    auto create_dir = [](const String & name) {
        Poco::File f(name);
        f.createDirectories();
        ASSERT_TRUE(f.exists()) << name;
    };

    create_dir(cp_dir1.getFileName());
    create_dir(cp_dir2.getFileName());
    create_dir(tmp_path + "/" + "not_checkpoint");

    uni_ps_service->removeAllLocalCheckpointFiles();

    ASSERT_FALSE(Poco::File(cp_dir1).exists()) << cp_dir1.getFileName();
    ASSERT_FALSE(Poco::File(cp_dir2).exists()) << cp_dir2.getFileName();
    ASSERT_TRUE(Poco::File(tmp_path + "/" + "not_checkpoint").exists()) << tmp_path;
}
CATCH

} // namespace DB::PS::universal::tests
