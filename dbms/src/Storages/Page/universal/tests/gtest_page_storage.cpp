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

#include <IO/ReadBufferFromFile.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/V3/Remote/CheckpointFilesWriter.h>
#include <Storages/Page/V3/Remote/CheckpointManifestFileReader.h>
#include <Storages/Page/universal/UniversalPageStorage.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>

namespace DB
{
namespace PS::universal::tests
{
class UniPageStorageTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto path = getTemporaryPath();
        createIfNotExist(path);
        file_provider = DB::tests::TiFlashTestEnv::getGlobalContext().getFileProvider();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        page_storage = UniversalPageStorage::create("test.t", delegator, config, file_provider);
        page_storage->restore();

        for (size_t i = 0; i < buf_sz; ++i)
        {
            c_buff[i] = i % 0xff;
        }

        log = Logger::get("PageStorageTest");
    }

    std::shared_ptr<UniversalPageStorage> reopenWithConfig(const PageStorageConfig & config_)
    {
        auto path = getTemporaryPath();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto storage = UniversalPageStorage::create("test.t", delegator, config_, file_provider);
        storage->restore();
        return storage;
    }

protected:
    FileProviderPtr file_provider;
    PSDiskDelegatorPtr delegator;
    PageStorageConfig config;
    std::shared_ptr<UniversalPageStorage> page_storage;

    LoggerPtr log;

    static constexpr size_t buf_sz = 1024;
    char c_buff[buf_sz] = {};
};

TEST_F(UniPageStorageTest, RaftLog)
{
    UInt64 tag = 0;
    {
        UniversalWriteBatch wb;
        c_buff[0] = 10;
        c_buff[1] = 1;
        wb.putPage(RaftLogReader::toFullPageId(10, 1), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 4;
        wb.putPage(RaftLogReader::toFullPageId(10, 4), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 5;
        wb.putPage(RaftLogReader::toFullPageId(10, 5), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 6;
        wb.putPage(RaftLogReader::toFullPageId(10, 6), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 7;
        wb.putPage(RaftLogReader::toFullPageId(10, 7), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 10;
        wb.putPage(RaftLogReader::toFullPageId(10, 10), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);

        c_buff[0] = 100;
        c_buff[1] = 1;
        wb.putPage(RaftLogReader::toFullPageId(100, 7), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);

        wb.putPage(RaftLogReader::toFullPageId(255, 10), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);

        page_storage->write(std::move(wb));
    }

    RaftLogReader raft_log_reader(*page_storage);
    auto checker = [this](DB::PageId page_id, const DB::Page & page) {
        LOG_INFO(log, "{} {}", page_id, page.isValid());
    };
    raft_log_reader.traverse(RaftLogReader::toFullPageId(10, 0), RaftLogReader::toFullPageId(101, 0), checker);
}

// ===== Begin Remote Checkpoint Tests =====
// These tests should be moved to other places, when these methods are reorganized.
// TODO: These tests shares a lot in common with the non-universal version (see Page/V3/tests/gtest_page_storage.cpp).
//       We should find someway to deduplicate.

class UniPageStorageRemoteCheckpointTest : public UniPageStorageTest
{
public:
    UniPageStorageRemoteCheckpointTest()
    {
        writer_info = std::make_shared<V3::Remote::WriterInfo>();
        writer_info->set_store_id(1027);

        output_directory = DB::tests::TiFlashTestEnv::getTemporaryPath("UniPSRemoteCheckpointTest/");
        DB::tests::TiFlashTestEnv::tryRemovePath(output_directory);
    }

    std::string readData(const V3::RemoteDataLocation & location)
    {
        RUNTIME_CHECK(location.offset_in_file > 0);
        RUNTIME_CHECK(location.data_file_id != nullptr && !location.data_file_id->empty());

        std::string ret;
        ret.resize(location.size_in_file);

        // Note: We will introduce a DataReader when compression is added later.
        // When there is compression, we first need to seek and read compressed blocks, decompress them, and then seek to the data we want.
        // A DataReader will encapsulate this logic.
        // Currently there is no compression, so reading data is rather easy.

        auto buf = ReadBufferFromFile(output_directory + *location.data_file_id);
        buf.seek(location.offset_in_file);
        auto n = buf.readBig(ret.data(), location.size_in_file);
        RUNTIME_CHECK(n == location.size_in_file);

        return ret;
    }

    void dumpCheckpoint()
    {
        page_storage->page_directory->dumpRemoteCheckpoint(V3::PageDirectory<V3::universal::PageDirectoryTrait>::DumpRemoteCheckpointOptions<V3::universal::BlobStoreTrait>{
            .temp_directory = output_directory,
            .remote_directory = output_directory,
            .data_file_name_pattern = "{sequence}_{sub_file_index}.data",
            .manifest_file_name_pattern = "{sequence}.manifest",
            .writer_info = writer_info,
            .blob_store = *page_storage->blob_store,
        });
    }

protected:
    std::shared_ptr<V3::Remote::WriterInfo> writer_info;
    std::string output_directory;
};

TEST_F(UniPageStorageRemoteCheckpointTest, DumpEmpty)
try
{
    dumpCheckpoint();
    {
        ASSERT_FALSE(Poco::File(output_directory + "0.manifest").exists());
        ASSERT_FALSE(Poco::File(output_directory + "0_0.data").exists());
    }
}
CATCH

TEST_F(UniPageStorageRemoteCheckpointTest, DumpAndRead)
try
{
    using namespace PS::V3;
    using namespace PS::V3::Remote;
    const UInt64 tag = 0;
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
    {
        ASSERT_TRUE(Poco::File(output_directory + "6.manifest").exists());
        ASSERT_TRUE(Poco::File(output_directory + "6_0.data").exists());
    }

    // FIXME: When there is a trait this is ridiculously long.....
    auto reader = CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::create(CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::Options{.file_path = output_directory + "6.manifest"});
    auto edit = reader->read();
    auto records = edit.getRecords();

    ASSERT_EQ(5, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("10", iter->page_id);
    ASSERT_EQ("6_0.data", *iter->entry.remote_info->data_location.data_file_id);
    ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.remote_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_REF, iter->type);
    ASSERT_EQ("2", iter->page_id);

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("3", iter->page_id);
    ASSERT_TRUE(iter->entry.remote_info.has_value());
    ASSERT_EQ("6_0.data", *iter->entry.remote_info->data_location.data_file_id);
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.remote_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_DELETE, iter->type);
    ASSERT_EQ("3", iter->page_id);

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("5", iter->page_id);
    ASSERT_EQ("6_0.data", *iter->entry.remote_info->data_location.data_file_id);
    ASSERT_EQ("The flower carriage rocked", readData(iter->entry.remote_info->data_location));
}
CATCH

TEST_F(UniPageStorageRemoteCheckpointTest, ZeroSizedEntry)
try
{
    using namespace PS::V3;
    using namespace PS::V3::Remote;
    const UInt64 tag = 0;
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "Said she just dreamed a dream");
        batch.putPage("7", tag, "");
        batch.putPage("14", tag, "The flower carriage rocked");
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint();
    {
        ASSERT_TRUE(Poco::File(output_directory + "3.manifest").exists());
        ASSERT_TRUE(Poco::File(output_directory + "3_0.data").exists());
    }

    auto reader = CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::create(CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::Options{.file_path = output_directory + "3.manifest"});
    auto edit = reader->read();
    auto records = edit.getRecords();

    ASSERT_EQ(3, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("14", iter->page_id);
    ASSERT_EQ("The flower carriage rocked", readData(iter->entry.remote_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("3", iter->page_id);
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.remote_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("7", iter->page_id);
    ASSERT_EQ("", readData(iter->entry.remote_info->data_location));
}
CATCH

TEST_F(UniPageStorageRemoteCheckpointTest, PutAndDelete)
try
{
    using namespace PS::V3;
    using namespace PS::V3::Remote;
    const UInt64 tag = 0;
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
    {
        ASSERT_TRUE(Poco::File(output_directory + "2.manifest").exists());
        ASSERT_TRUE(Poco::File(output_directory + "2_0.data").exists());
    }

    auto reader = CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::create(CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::Options{.file_path = output_directory + "2.manifest"});
    auto edit = reader->read();
    auto records = edit.getRecords();

    ASSERT_EQ(2, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("3", iter->page_id);
    ASSERT_EQ("The flower carriage rocked", readData(iter->entry.remote_info->data_location));

    iter++;
    ASSERT_EQ(EditRecordType::VAR_DELETE, iter->type);
    ASSERT_EQ("3", iter->page_id);
}
CATCH

TEST_F(UniPageStorageRemoteCheckpointTest, MultiplePut)
try
{
    using namespace PS::V3;
    using namespace PS::V3::Remote;
    const UInt64 tag = 0;
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
    {
        ASSERT_TRUE(Poco::File(output_directory + "3.manifest").exists());
        ASSERT_TRUE(Poco::File(output_directory + "3_0.data").exists());
    }

    auto reader = CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::create(CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::Options{.file_path = output_directory + "3.manifest"});
    auto edit = reader->read();
    auto records = edit.getRecords();

    ASSERT_EQ(1, records.size());

    auto iter = records.begin();
    ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
    ASSERT_EQ("3", iter->page_id);
    ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.remote_info->data_location));
}
CATCH

TEST_F(UniPageStorageRemoteCheckpointTest, GCDuringDump)
try
{
    // TODO
}
CATCH

TEST_F(UniPageStorageRemoteCheckpointTest, DeleteAndGCDuringDump)
try
{
    // TODO
}
CATCH

TEST_F(UniPageStorageRemoteCheckpointTest, DumpWriteDump)
try
{
    using namespace PS::V3;
    using namespace PS::V3::Remote;
    const UInt64 tag = 0;
    {
        UniversalWriteBatch batch;
        batch.putPage("3", tag, "The flower carriage rocked");
        batch.putPage("4", tag, "Nahida opened her eyes");
        page_storage->write(std::move(batch));
    }
    dumpCheckpoint();
    {
        ASSERT_TRUE(Poco::File(output_directory + "2.manifest").exists());
        ASSERT_TRUE(Poco::File(output_directory + "2_0.data").exists());

        auto reader = CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::create(CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::Options{.file_path = output_directory + "2.manifest"});
        auto edit = reader->read();
        auto records = edit.getRecords();

        ASSERT_EQ(2, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("3", iter->page_id);
        ASSERT_EQ("2_0.data", *iter->entry.remote_info->data_location.data_file_id);
        ASSERT_EQ("The flower carriage rocked", readData(iter->entry.remote_info->data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("4", iter->page_id);
        ASSERT_EQ("2_0.data", *iter->entry.remote_info->data_location.data_file_id);
        ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.remote_info->data_location));
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
        ASSERT_TRUE(Poco::File(output_directory + "4.manifest").exists());
        ASSERT_TRUE(Poco::File(output_directory + "4_0.data").exists());

        auto reader = CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::create(CheckpointManifestFileReader<V3::universal::PageDirectoryTrait>::Options{.file_path = output_directory + "4.manifest"});
        auto edit = reader->read();
        auto records = edit.getRecords();

        ASSERT_EQ(3, records.size());

        auto iter = records.begin();
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("3", iter->page_id);
        ASSERT_EQ("4_0.data", *iter->entry.remote_info->data_location.data_file_id);
        ASSERT_EQ("Said she just dreamed a dream", readData(iter->entry.remote_info->data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("4", iter->page_id);
        ASSERT_EQ("2_0.data", *iter->entry.remote_info->data_location.data_file_id);
        ASSERT_EQ("Nahida opened her eyes", readData(iter->entry.remote_info->data_location));

        iter++;
        ASSERT_EQ(EditRecordType::VAR_ENTRY, iter->type);
        ASSERT_EQ("5", iter->page_id);
        ASSERT_EQ("4_0.data", *iter->entry.remote_info->data_location.data_file_id);
        ASSERT_EQ("Dreamed of the day that she was born", readData(iter->entry.remote_info->data_location));
    }
}
CATCH

// ===== End Remote Checkpoint Tests =====


// FIXME: move to a separate test file
TEST_F(UniPageStorageTest, UniversalPageId)
{
    auto u_id = buildTableUniversalPageId(getStoragePrefix(TableStorageTag::Log), 1, 1);
    ASSERT_EQ(DB::PS::V3::universal::ExternalIdTrait::getU64ID(u_id), 1);
}
} // namespace PS::universal::tests
} // namespace DB
