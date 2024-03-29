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
#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <IO/Buffer/ReadBufferFromRandomAccessFile.h>
#include <IO/Encryption/MockKeyManager.h>
#include <Storages/Page/V3/CheckpointFile/CPFilesWriter.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/CheckpointFile/CPWriteDataSource.h>
#include <Storages/Page/V3/PageEntryCheckpointInfo.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ext/scope_guard.h>

namespace DB::PS::V3::tests
{

class CheckpointFileTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        dir = getTemporaryPath();
        dropDataOnDisk(dir);
        createIfNotExist(dir);

        data_file_path_pattern = dir + "/data_{index}";
        data_file_id_pattern = "data_{index}";
        manifest_file_path = dir + "/manifest_foo";
        manifest_file_id = "manifest_foo";

        log = Logger::get("CheckpointFileTest");
    }

    std::string readData(const V3::CheckpointLocation & location)
    {
        RUNTIME_CHECK(location.offset_in_file > 0);
        RUNTIME_CHECK(location.data_file_id != nullptr && !location.data_file_id->empty());

        std::string ret;
        ret.resize(location.size_in_file);

        auto data_file = PosixRandomAccessFile::create(dir + "/" + *location.data_file_id);
        ReadBufferFromRandomAccessFile buf(data_file);
        buf.seek(location.offset_in_file);
        auto n = buf.readBig(ret.data(), location.size_in_file);
        RUNTIME_CHECK(n == location.size_in_file);

        return ret;
    }

protected:
    String dir;
    String data_file_id_pattern;
    String data_file_path_pattern;
    String manifest_file_id;
    String manifest_file_path;
    LoggerPtr log;
};

TEST_F(CheckpointFileTest, WritePrefixOnly)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = CPWriteDataSourceFixture::create({}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    auto data_file_paths = writer->data_file_paths;
    writer.reset();

    ASSERT_FALSE(data_file_paths.empty());
    for (const auto & path : data_file_paths)
    {
        ASSERT_TRUE(Poco::File(path).exists());
    }
    ASSERT_TRUE(Poco::File(manifest_file_path).exists());

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto prefix = manifest_reader->readPrefix();
    ASSERT_EQ(5, prefix.local_sequence());
    ASSERT_EQ(3, prefix.last_local_sequence());
}
CATCH

TEST_F(CheckpointFileTest, WriteEditsWithoutPrefix)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = CPWriteDataSourceFixture::create({}),
    });

    auto edits = universal::PageEntriesEdit{};
    edits.appendRecord({.type = EditRecordType::DEL});

    ASSERT_THROW({ writer->writeEditsAndApplyCheckpointInfo(edits); }, DB::Exception);
}
CATCH

TEST_F(CheckpointFileTest, WriteEdits)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = CPWriteDataSourceFixture::create({}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = universal::PageEntriesEdit{};
        edits.appendRecord({.type = EditRecordType::VAR_DELETE, .page_id = "water"});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    LOG_DEBUG(log, "Checkpoint data paths: {}", data_paths);
    writer.reset();

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    auto prefix = manifest_reader->readPrefix();
    CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_TRUE(edits_r.has_value());
        ASSERT_EQ(1, edits_r->size());
        ASSERT_EQ("water", edits_r->getRecords()[0].page_id);
        ASSERT_EQ(EditRecordType::VAR_DELETE, edits_r->getRecords()[0].type);
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
    {
        auto locks = manifest_reader->readLocks();
        ASSERT_FALSE(locks.has_value());
    }
    {
        auto locks = manifest_reader->readLocks();
        ASSERT_FALSE(locks.has_value());
    }
}
CATCH

TEST_F(CheckpointFileTest, WriteMultipleEdits)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source
        = CPWriteDataSourceFixture::create({{5, "Said she just dreamed a dream"}, {10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = universal::PageEntriesEdit{};
        edits.appendRecord({.type = EditRecordType::VAR_DELETE, .page_id = "water"});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    {
        auto edits = universal::PageEntriesEdit{};
        edits.appendRecord({.type = EditRecordType::VAR_ENTRY, .page_id = "abc", .entry = {.size = 29, .offset = 5}});
        edits.appendRecord({.type = EditRecordType::VAR_REF, .page_id = "foo", .ori_page_id = "abc"});
        edits.appendRecord(
            {.type = EditRecordType::VAR_ENTRY, .page_id = "aaabbb", .entry = {.size = 22, .offset = 10}});
        edits.appendRecord({.type = EditRecordType::VAR_DELETE, .page_id = "rain"});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    LOG_DEBUG(log, "Checkpoint data paths: {}", data_paths);
    writer.reset();

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    manifest_reader->readPrefix();
    CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_TRUE(edits_r.has_value());
        ASSERT_EQ(1, edits_r->size());
        ASSERT_EQ(EditRecordType::VAR_DELETE, edits_r->getRecords()[0].type);
        ASSERT_EQ("water", edits_r->getRecords()[0].page_id);
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(4, r.size());

        ASSERT_EQ(EditRecordType::VAR_ENTRY, r[0].type);
        ASSERT_EQ("abc", r[0].page_id);
        ASSERT_EQ(0, r[0].entry.offset); // The deserialized offset is not the same as the original one!
        ASSERT_EQ(29, r[0].entry.size);
        ASSERT_TRUE(r[0].entry.checkpoint_info.is_valid);
        ASSERT_TRUE(r[0].entry.checkpoint_info.is_local_data_reclaimed);
        ASSERT_EQ("data_0", *r[0].entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("Said she just dreamed a dream", readData(r[0].entry.checkpoint_info.data_location));

        ASSERT_EQ(EditRecordType::VAR_REF, r[1].type);
        ASSERT_EQ("foo", r[1].page_id);
        ASSERT_EQ("abc", r[1].ori_page_id);

        ASSERT_EQ(EditRecordType::VAR_ENTRY, r[2].type);
        ASSERT_EQ("aaabbb", r[2].page_id);
        ASSERT_EQ(0, r[2].entry.offset);
        ASSERT_EQ(22, r[2].entry.size);
        ASSERT_TRUE(r[2].entry.checkpoint_info.is_valid);
        ASSERT_TRUE(r[2].entry.checkpoint_info.is_local_data_reclaimed);
        ASSERT_EQ("data_0", *r[2].entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("nahida opened her eyes", readData(r[2].entry.checkpoint_info.data_location));

        ASSERT_EQ(EditRecordType::VAR_DELETE, r[3].type);
        ASSERT_EQ("rain", r[3].page_id);

        // Check data_file_id is shared.
        ASSERT_EQ(
            r[0].entry.checkpoint_info.data_location.data_file_id->data(),
            r[2].entry.checkpoint_info.data_location.data_file_id->data());
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
    {
        auto locks = manifest_reader->readLocks();
        ASSERT_TRUE(locks.has_value());
        ASSERT_EQ(1, locks->size());
        ASSERT_EQ(1, locks->count("data_0"));
    }
    {
        auto locks = manifest_reader->readLocks();
        ASSERT_FALSE(locks.has_value());
    }
}
CATCH

TEST_F(CheckpointFileTest, WriteEditsWithCheckpointInfo)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = CPWriteDataSourceFixture::create({{10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = universal::PageEntriesEdit{};
        edits.appendRecord({
            .type = EditRecordType::VAR_ENTRY,
            .page_id = "abc",
            .entry = {
                .size = 10,
                .offset = 5,
                .checkpoint_info = OptionalCheckpointInfo(
                    CheckpointLocation{
                        .data_file_id = std::make_shared<String>("my_file_id"),
                    },
                    true,
                    false
                ),
            },
        });
        edits.appendRecord({.type = EditRecordType::VAR_REF, .page_id = "foo", .ori_page_id = "abc"});
        edits.appendRecord(
            {.type = EditRecordType::VAR_ENTRY, .page_id = "aaabbb", .entry = {.size = 22, .offset = 10}});
        edits.appendRecord({.type = EditRecordType::VAR_DELETE, .page_id = "sun"});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    LOG_DEBUG(log, "Checkpoint data paths: {}", data_paths);
    writer.reset();

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    manifest_reader->readPrefix();
    CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(4, r.size());

        ASSERT_EQ(EditRecordType::VAR_ENTRY, r[0].type);
        ASSERT_EQ("abc", r[0].page_id);
        ASSERT_EQ(0, r[0].entry.offset); // The deserialized offset is not the same as the original one!
        ASSERT_EQ(10, r[0].entry.size);
        ASSERT_TRUE(r[0].entry.checkpoint_info.is_valid);
        ASSERT_TRUE(
            r[0].entry.checkpoint_info.is_local_data_reclaimed); // After deserialization, this field is always true!
        ASSERT_EQ("my_file_id", *r[0].entry.checkpoint_info.data_location.data_file_id);

        ASSERT_EQ(EditRecordType::VAR_REF, r[1].type);
        ASSERT_EQ("foo", r[1].page_id);
        ASSERT_EQ("abc", r[1].ori_page_id);

        ASSERT_EQ(EditRecordType::VAR_ENTRY, r[2].type);
        ASSERT_EQ("aaabbb", r[2].page_id);
        ASSERT_EQ(0, r[2].entry.offset);
        ASSERT_EQ(22, r[2].entry.size);
        ASSERT_TRUE(r[2].entry.checkpoint_info.is_valid);
        ASSERT_TRUE(r[2].entry.checkpoint_info.is_local_data_reclaimed);
        ASSERT_EQ("data_0", *r[2].entry.checkpoint_info.data_location.data_file_id);
        ASSERT_EQ("nahida opened her eyes", readData(r[2].entry.checkpoint_info.data_location));

        ASSERT_EQ(EditRecordType::VAR_DELETE, r[3].type);
        ASSERT_EQ("sun", r[3].page_id);
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
    {
        auto locks = manifest_reader->readLocks();
        ASSERT_TRUE(locks.has_value());
        ASSERT_EQ(2, locks->size());
        ASSERT_EQ(1, locks->count("data_0"));
        ASSERT_EQ(1, locks->count("my_file_id"));
    }
    {
        auto locks = manifest_reader->readLocks();
        ASSERT_FALSE(locks.has_value());
    }
}
CATCH

TEST_F(CheckpointFileTest, FromBlobStore)
try
{
    std::vector<FileProviderPtr> file_providers;
    file_providers.reserve(3);
    file_providers.push_back(DB::tests::TiFlashTestEnv::getDefaultFileProvider());
    {
        // enable encryption
        KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(true);
        auto file_provider = std::make_shared<FileProvider>(key_manager, true);
        file_providers.push_back(file_provider);
    }
    {
        // enable keyspace encryption
        KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(true);
        auto file_provider = std::make_shared<FileProvider>(key_manager, true, true);
        file_providers.push_back(file_provider);
    }
    for (const auto & file_provider : file_providers)
    {
        const auto delegator = std::make_shared<DB::tests::MockDiskDelegatorMulti>(std::vector{dir});
        PageTypeAndConfig page_type_and_config{
            {PageType::Normal, PageTypeConfig{.heavy_gc_valid_rate = 0.5}},
            {PageType::RaftData, PageTypeConfig{.heavy_gc_valid_rate = 0.01}},
        };
        auto blob_store = BlobStore<universal::BlobStoreTrait>(
            getCurrentTestName(),
            file_provider,
            delegator,
            BlobConfig{},
            page_type_and_config);

        auto edits = universal::PageEntriesEdit{};
        {
            UniversalWriteBatch wb;
            wb.putPage("page_foo", 0, "The flower carriage rocked", {4, 10, 12});
            wb.delPage("id_bar");
            wb.putPage("page_abc", 0, "Dreamed of the day that she was born");
            auto blob_store_edits = blob_store.write(std::move(wb));

            ASSERT_EQ(blob_store_edits.size(), 3);

            edits.appendRecord(
                {.type = EditRecordType::VAR_ENTRY,
                 .page_id = "page_foo",
                 .entry = blob_store_edits.getRecords()[0].entry});
            edits.appendRecord({.type = EditRecordType::VAR_DELETE, .page_id = "id_bar"});
            edits.appendRecord(
                {.type = EditRecordType::VAR_ENTRY,
                 .page_id = "page_abc",
                 .entry = blob_store_edits.getRecords()[2].entry});
        }

        auto writer = CPFilesWriter::create({
            .data_file_path_pattern = data_file_path_pattern,
            .data_file_id_pattern = data_file_id_pattern,
            .manifest_file_path = manifest_file_path,
            .manifest_file_id = manifest_file_id,
            .data_source = CPWriteDataSourceBlobStore::create(blob_store, file_provider),
        });
        writer->writePrefix({
            .writer = {},
            .sequence = 5,
            .last_sequence = 3,
        });
        writer->writeEditsAndApplyCheckpointInfo(edits);
        auto data_paths = writer->writeSuffix();
        LOG_DEBUG(log, "Checkpoint data paths: {}", data_paths);
        writer.reset();

        auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
        auto manifest_reader = CPManifestFileReader::create({
            .plain_file = manifest_file,
        });
        manifest_reader->readPrefix();
        CheckpointProto::StringsInternMap im;
        {
            auto edits_r = manifest_reader->readEdits(im);
            auto r = edits_r->getRecords();
            ASSERT_EQ(3, r.size());

            ASSERT_EQ(EditRecordType::VAR_ENTRY, r[0].type);
            ASSERT_EQ("page_foo", r[0].page_id);
            ASSERT_EQ(0, r[0].entry.offset);
            ASSERT_EQ(26, r[0].entry.size);
            ASSERT_TRUE(r[0].entry.checkpoint_info.is_valid);
            ASSERT_TRUE(r[0].entry.checkpoint_info.is_local_data_reclaimed);
            ASSERT_EQ("The flower carriage rocked", readData(r[0].entry.checkpoint_info.data_location));

            int begin, end;
            std::tie(begin, end) = r[0].entry.getFieldOffsets(0);
            ASSERT_EQ(std::make_pair(0, 4), std::make_pair(begin, end));
            std::tie(begin, end) = r[0].entry.getFieldOffsets(1);
            ASSERT_EQ(std::make_pair(4, 14), std::make_pair(begin, end));
            std::tie(begin, end) = r[0].entry.getFieldOffsets(2);
            ASSERT_EQ(std::make_pair(14, 26), std::make_pair(begin, end));
            ASSERT_EQ(4, r[0].entry.getFieldSize(0));
            ASSERT_EQ(10, r[0].entry.getFieldSize(1));
            ASSERT_EQ(12, r[0].entry.getFieldSize(2));

            ASSERT_EQ(EditRecordType::VAR_DELETE, r[1].type);
            ASSERT_EQ("id_bar", r[1].page_id);

            ASSERT_EQ(EditRecordType::VAR_ENTRY, r[2].type);
            ASSERT_EQ("page_abc", r[2].page_id);
            ASSERT_EQ(0, r[2].entry.offset);
            ASSERT_EQ(36, r[2].entry.size);
            ASSERT_TRUE(r[2].entry.checkpoint_info.is_valid);
            ASSERT_TRUE(r[2].entry.checkpoint_info.is_local_data_reclaimed);
            ASSERT_EQ("Dreamed of the day that she was born", readData(r[2].entry.checkpoint_info.data_location));
        }
        EXPECT_THROW(
            {
                // Call readLocks without draining readEdits should result in exceptions
                manifest_reader->readLocks();
            },
            DB::Exception);
        {
            auto edits_r = manifest_reader->readEdits(im);
            ASSERT_FALSE(edits_r.has_value());
        }
        {
            auto locks = manifest_reader->readLocks();
            ASSERT_TRUE(locks.has_value());
            ASSERT_EQ(1, locks->size());
            ASSERT_EQ(1, locks->count("data_0"));
        }
        {
            auto locks = manifest_reader->readLocks();
            ASSERT_FALSE(locks.has_value());
        }
    }
}
CATCH

TEST_F(CheckpointFileTest, WriteEmptyEdits)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = CPWriteDataSourceFixture::create({}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = universal::PageEntriesEdit{};
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    {
        auto edits = universal::PageEntriesEdit{};
        edits.appendRecord({.type = EditRecordType::VAR_DELETE, .page_id = "snow"});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    LOG_DEBUG(log, "Checkpoint data paths: {}", data_paths);
    writer.reset();

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    manifest_reader->readPrefix();
    CheckpointProto::StringsInternMap im;

    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(1, r.size());
        ASSERT_EQ(EditRecordType::VAR_DELETE, r[0].type);
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
    {
        auto locks = manifest_reader->readLocks();
        ASSERT_FALSE(locks.has_value());
    }
}
CATCH

TEST_F(CheckpointFileTest, WriteEditsNotCalled)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = CPWriteDataSourceFixture::create({}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    auto data_paths = writer->writeSuffix();
    LOG_DEBUG(log, "Checkpoint data paths: {}", data_paths);
    writer.reset();

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    manifest_reader->readPrefix();
    CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
    {
        auto locks = manifest_reader->readLocks();
        ASSERT_FALSE(locks.has_value());
    }
}
CATCH

TEST_F(CheckpointFileTest, PreDefinedLocks)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = data_file_id_pattern,
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = CPWriteDataSourceFixture::create({{10, "nahida opened her eyes"}}),
        .must_locked_files = {"f1", "fx"},
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = universal::PageEntriesEdit{};
        edits.appendRecord({
            .type = EditRecordType::VAR_ENTRY,
            .page_id = "abc",
            .entry = {
                .offset = 5,
                .checkpoint_info = OptionalCheckpointInfo(
                    CheckpointLocation{
                        .data_file_id = std::make_shared<String>("my_file_id"),
                    },
                    true,
                    false
                ),
            },
        });
        edits.appendRecord(
            {.type = EditRecordType::VAR_ENTRY, .page_id = "aaabbb", .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    LOG_DEBUG(log, "Checkpoint data paths: {}", data_paths);
    writer.reset();

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    manifest_reader->readPrefix();
    CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(2, r.size());
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
    {
        auto locks = manifest_reader->readLocks();
        ASSERT_TRUE(locks.has_value());
        ASSERT_EQ(4, locks->size());
        ASSERT_EQ(1, locks->count("f1"));
        ASSERT_EQ(1, locks->count("fx"));
        ASSERT_EQ(1, locks->count("my_file_id"));
        ASSERT_EQ(1, locks->count("data_0"));
    }
    {
        auto locks = manifest_reader->readLocks();
        ASSERT_FALSE(locks.has_value());
    }
}
CATCH

TEST_F(CheckpointFileTest, LotsOfEdits)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path_pattern = data_file_path_pattern,
        .data_file_id_pattern = "this-is-simply-a-very-long-data-file-id-7b768082-db43-4e65-a0fb-d34645200298-{index}",
        .manifest_file_path = manifest_file_path,
        .manifest_file_id = manifest_file_id,
        .data_source = CPWriteDataSourceFixture::create({{10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = universal::PageEntriesEdit{};
        for (size_t i = 0; i < 10000; ++i)
            edits.appendRecord({
                .type = EditRecordType::VAR_ENTRY,
                .page_id = fmt::format("record_{}", i),
                .entry = {.size = 22, .offset = 10},
            });
        writer->writeEditsAndApplyCheckpointInfo(edits);
    }
    auto data_paths = writer->writeSuffix();
    LOG_DEBUG(log, "Checkpoint data paths: {}", data_paths);
    writer.reset();

    auto manifest_file = PosixRandomAccessFile::create(manifest_file_path);
    auto manifest_reader = CPManifestFileReader::create({
        .plain_file = manifest_file,
    });
    manifest_reader->readPrefix();
    CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(10000, r.size());
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
}
CATCH

} // namespace DB::PS::V3::tests
