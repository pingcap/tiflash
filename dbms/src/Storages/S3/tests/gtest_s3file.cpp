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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <IO/BaseFile/PosixWritableFile.h>
#include <IO/Encryption/MockKeyManager.h>
#include <Interpreters/Context.h>
#include <Poco/DigestStream.h>
#include <Poco/MD5Engine.h>
#include <Poco/StreamCopier.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStoreS3.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/Page/V3/CheckpointFile/CPDataFileStat.h>
#include <Storages/Page/V3/CheckpointFile/CheckpointFiles.h>
#include <Storages/S3/MockS3Client.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <Storages/S3/S3WritableFile.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketCorsRequest.h>
#include <fmt/chrono.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <chrono>
#include <ext/scope_guard.h>
#include <fstream>


using namespace std::chrono_literals;
using namespace DB::DM;
using namespace DB::DM::tests;
using namespace DB::S3;

namespace DB::FailPoints
{
extern const char force_set_mocked_s3_object_mtime[];
} // namespace DB::FailPoints

namespace DB::tests
{
using DMFileBlockOutputStreamPtr = std::shared_ptr<DMFileBlockOutputStream>;
using DMFileBlockInputStreamPtr = std::shared_ptr<DMFileBlockInputStream>;

class S3FileTest
    : public DB::base::TiFlashStorageTestBasic
    , public testing::WithParamInterface<bool>
{
public:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();

        reload();

        log = Logger::get();

        buf_unit.resize(256);
        std::iota(buf_unit.begin(), buf_unit.end(), 0);

        bool is_encrypted = GetParam();
        KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(is_encrypted);
        auto file_provider = std::make_shared<FileProvider>(key_manager, is_encrypted);
        db_context->setFileProvider(file_provider);

        s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        data_store = std::make_shared<DM::Remote::DataStoreS3>(dbContext().getFileProvider());
        ASSERT_TRUE(::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));
        DB::tests::TiFlashTestEnv::enableS3Config();
    }

    void reload() { TiFlashStorageTestBasic::reload(); }

    Context & dbContext() { return *db_context; }

    void TearDown() override { DB::tests::TiFlashTestEnv::disableS3Config(); }

protected:
    void writeLocalFile(const String & path, size_t size)
    {
        PosixWritableFile file(path, /*truncate_when_exists*/ true, -1, 0600);
        size_t write_size = 0;
        while (write_size < size)
        {
            auto to_write = std::min(buf_unit.size(), size - write_size);
            auto n = file.write(buf_unit.data(), to_write);
            ASSERT_EQ(n, to_write);
            write_size += n;
        }
        ASSERT_EQ(file.fsync(), 0);
    }

    void writeFile(const String & key, size_t size, const WriteSettings & write_setting)
    {
        S3WritableFile file(s3_client, key, write_setting);
        size_t write_size = 0;
        while (write_size < size)
        {
            auto to_write = std::min(buf_unit.size(), size - write_size);
            auto n = file.write(buf_unit.data(), to_write);
            ASSERT_EQ(n, to_write);
            write_size += n;
        }
        auto r = file.fsync();
        ASSERT_EQ(r, 0);
        last_upload_info = file.getUploadInfo();
    }

    void verifyFile(const String & key, size_t size)
    {
        S3RandomAccessFile file(s3_client, key);
        std::vector<char> tmp_buf;
        size_t read_size = 0;
        while (read_size < size)
        {
            tmp_buf.resize(256);
            std::iota(tmp_buf.begin(), tmp_buf.end(), 1);
            auto n = file.read(tmp_buf.data(), tmp_buf.size());
            ASSERT_GE(n, 0);
            if (static_cast<size_t>(n) == buf_unit.size())
            {
                ASSERT_EQ(tmp_buf, buf_unit);
            }
            else
            {
                ASSERT_EQ(
                    std::vector<char>(tmp_buf.begin(), tmp_buf.begin() + n),
                    std::vector<char>(buf_unit.begin(), buf_unit.begin() + n));
            }
            read_size += n;
        }
        ASSERT_EQ(read_size, size);
    }

    static String md5(const std::string & filename)
    {
        Poco::MD5Engine md5_engine;
        Poco::DigestOutputStream output_stream(md5_engine);
        std::ifstream fstr(filename);
        Poco::StreamCopier::copyStream(fstr, output_stream);
        output_stream.close();
        auto md5_val = md5_engine.digest();
        String res;
        static constexpr const char * hex_table = "0123456789abcdef";
        for (int i = 0; i < 16; ++i)
        {
            auto c = md5_val[i];
            res += hex_table[c >> 4];
            res += hex_table[c & 15];
        }
        return res;
    }

    static void downloadDMFile(
        const DMFileOID & remote_oid,
        const String & local_dir,
        const std::vector<String> & target_files)
    {
        Remote::DataStoreS3::copyToLocal(remote_oid, target_files, local_dir);
    }

    std::unordered_map<String, size_t> listFiles(const DMFileOID & oid)
    {
        auto dmfile_dir = getPathByStatus(
            S3::S3Filename::fromTableID(oid.store_id, oid.keyspace_id, oid.table_id).toFullKey(),
            oid.file_id,
            DMFileStatus::READABLE);
        return S3::listPrefixWithSize(*s3_client, dmfile_dir + "/");
    }

    DMFilePtr restoreDMFile(const DMFileOID & oid)
    {
        return data_store->prepareDMFile(oid, /* page_id= */ 0)
            ->restore(DMFileMeta::ReadMode::all(), /* meta_version= */ 0);
    }

    LoggerPtr log;
    std::vector<char> buf_unit;
    std::shared_ptr<TiFlashS3Client> s3_client;
    S3WritableFile::UploadInfo last_upload_info;
    Remote::IDataStorePtr data_store;
    KeyspaceID keyspace_id = 0x12345678;
};

TEST_P(S3FileTest, SinglePart)
try
{
    for (int i = 0; i < 10; i++)
    {
        const size_t size = 256 * i + ::rand() % 256; // NOLINT(cert-msc50-cpp)
        const String key = "/a/b/c/singlepart";
        writeFile(key, size, WriteSettings{});
        ASSERT_EQ(last_upload_info.part_number, 0);
        ASSERT_TRUE(last_upload_info.multipart_upload_id.empty());
        ASSERT_TRUE(last_upload_info.part_tags.empty());
        ASSERT_EQ(last_upload_info.total_write_bytes, size);
        verifyFile(key, size);
    }
}
CATCH

TEST_P(S3FileTest, MultiPart)
try
{
    const auto size = 1024 * 1024 * 18; // 18MB
    WriteSettings write_setting;
    write_setting.max_single_part_upload_size = 1024 * 1024 * 6; // 6MB
    write_setting.upload_part_size = 1024 * 1024 * 5; // 5MB
    const String key = "/a/b/c/multipart";
    writeFile(key, size, write_setting);
    ASSERT_EQ(last_upload_info.part_number, 4);
    ASSERT_FALSE(last_upload_info.multipart_upload_id.empty());
    ASSERT_EQ(last_upload_info.part_tags.size(), last_upload_info.part_number);
    ASSERT_EQ(last_upload_info.total_write_bytes, size);
    verifyFile(key, size);
}
CATCH

TEST_P(S3FileTest, Seek)
try
{
    const auto size = 1024 * 1024 * 10; // 10MB
    WriteSettings write_setting;
    const String key = "/a/b/c/seek";
    writeFile(key, size, write_setting);
    S3RandomAccessFile file(s3_client, key);
    {
        std::vector<char> tmp_buf(256);
        auto n = file.read(tmp_buf.data(), tmp_buf.size());
        ASSERT_EQ(n, tmp_buf.size());
        ASSERT_EQ(tmp_buf, buf_unit);
    }
    {
        auto offset = file.seek(513, SEEK_SET);
        ASSERT_EQ(offset, 513);
        std::vector<char> tmp_buf(256);
        auto n = file.read(tmp_buf.data(), tmp_buf.size());
        ASSERT_EQ(n, tmp_buf.size());

        std::vector<char> expected(256);
        std::iota(expected.begin(), expected.end(), 1);
        ASSERT_EQ(tmp_buf, expected);
    }
}
CATCH

TEST_P(S3FileTest, WriteRead)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);

    const size_t num_rows_write = 128;

    DMFileBlockOutputStream::BlockProperty block_property1;
    block_property1.effective_num_rows = 1;
    block_property1.gc_hint_version = 1;
    block_property1.deleted_rows = 1;
    DMFileBlockOutputStream::BlockProperty block_property2;
    block_property2.effective_num_rows = 2;
    block_property2.gc_hint_version = 2;
    block_property2.deleted_rows = 2;
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys;
    block_propertys.push_back(block_property1);
    block_propertys.push_back(block_property2);
    auto parent_path = TiFlashStorageTestBasic::getTemporaryPath();
    DMFilePtr dmfile;
    DMFileOID oid;
    oid.store_id = 1;
    oid.table_id = 1;
    oid.file_id = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now())
                      .time_since_epoch()
                      .count();
    oid.keyspace_id = keyspace_id;

    {
        // Prepare for write
        // Block 1: [0, 64)
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write / 2);
        // Block 2: [64, 128)
        Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write / 2, num_rows_write);

        auto configuration = std::make_optional<DMChecksumConfig>();
        dmfile = DMFile::create(
            oid.file_id,
            parent_path,
            std::move(configuration),
            128 * 1024,
            16 * 1024 * 1024,
            keyspace_id,
            DMFileFormat::V3);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dmfile, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->write(block2, block_property2);
        stream->writeSuffix();

        ASSERT_EQ(dmfile->getPackProperties().property_size(), 2);
    }

    std::vector<String> uploaded_files;
    {
        auto local_files = dmfile->listFilesForUpload();
        data_store->putDMFile(dmfile, oid, /*remove_local*/ false);
        auto remote_files_with_size = listFiles(oid);
        ASSERT_EQ(local_files.size(), remote_files_with_size.size());
        for (const auto & fname : local_files)
        {
            auto itr = remote_files_with_size.find(fname);
            ASSERT_NE(itr, remote_files_with_size.end());
            uploaded_files.push_back(fname);
        }
        LOG_TRACE(log, "remote_files_with_size => {}", remote_files_with_size);
    }

    {
        auto dmfile_dir = dmfile->path();
        auto copy_dir = fmt::format("{}_copy", dmfile_dir);
        Poco::File file(copy_dir);
        if (file.exists())
        {
            file.remove(true);
        }
        file.createDirectory();
        downloadDMFile(oid, copy_dir, uploaded_files);
        Poco::File poco_copy_dir(copy_dir);
        std::vector<String> filenames;
        poco_copy_dir.list(filenames);

        ASSERT_FALSE(filenames.empty());

        for (const auto & filename : filenames)
        {
            auto local_fname = fmt::format("{}/{}", dmfile_dir, filename);
            auto copy_fname = fmt::format("{}/{}", copy_dir, filename);
            auto local_md5 = md5(local_fname);
            auto copy_md5 = md5(copy_fname);
            bool is_encrypted = GetParam();
            if (is_encrypted)
            {
                // if encryption is enabled, local file is encrypted, but remote file is not encrypted,
                // so md5 is different
                ASSERT_NE(copy_md5, local_md5) << fmt::format("local_fname={}, copy_fname={}", local_fname, copy_fname);
                LOG_TRACE(log, "local_fname={}, copy_fname={}, md5={}", local_fname, copy_fname, local_md5);
            }
            else
            {
                ASSERT_EQ(copy_md5, local_md5) << fmt::format("local_fname={}, copy_fname={}", local_fname, copy_fname);
                LOG_TRACE(log, "local_fname={}, copy_fname={}, md5={}", local_fname, copy_fname, local_md5);
            }
        }
    }

    auto dmfile_from_s3 = restoreDMFile(oid);
    ASSERT_NE(dmfile_from_s3, nullptr);
    try
    {
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.build(
            dmfile_from_s3,
            *cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
    }
    catch (...)
    {
        tryLogCurrentException("restore...");
        std::abort();
    }
}
CATCH

TEST_P(S3FileTest, RemoveLocal)
try
{
    auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ true);

    const size_t num_rows_write = 128;

    auto read_dmfile = [&](DMFilePtr dmf) {
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream
            = builder.build(dmf, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
            }));
    };

    DMFileBlockOutputStream::BlockProperty block_property1;
    block_property1.effective_num_rows = 1;
    block_property1.gc_hint_version = 1;
    block_property1.deleted_rows = 1;
    DMFileBlockOutputStream::BlockProperty block_property2;
    block_property2.effective_num_rows = 2;
    block_property2.gc_hint_version = 2;
    block_property2.deleted_rows = 2;
    std::vector<DMFileBlockOutputStream::BlockProperty> block_propertys;
    block_propertys.push_back(block_property1);
    block_propertys.push_back(block_property2);
    auto parent_path = TiFlashStorageTestBasic::getTemporaryPath();
    DMFilePtr dmfile;
    DMFileOID oid;
    oid.store_id = 1;
    oid.table_id = 1;
    oid.file_id = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now())
                      .time_since_epoch()
                      .count();
    oid.keyspace_id = keyspace_id;

    {
        // Prepare for write
        // Block 1: [0, 64)
        Block block1 = DMTestEnv::prepareSimpleWriteBlockWithNullable(0, num_rows_write / 2);
        // Block 2: [64, 128)
        Block block2 = DMTestEnv::prepareSimpleWriteBlockWithNullable(num_rows_write / 2, num_rows_write);

        auto configuration = std::make_optional<DMChecksumConfig>();
        dmfile = DMFile::create(
            oid.file_id,
            parent_path,
            std::move(configuration),
            128 * 1024,
            16 * 1024 * 1024,
            keyspace_id,
            DMFileFormat::V3);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dmfile, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->write(block2, block_property2);
        stream->writeSuffix();

        ASSERT_EQ(dmfile->getPackProperties().property_size(), 2);
    }

    std::vector<String> uploaded_files;
    auto local_dir = dmfile->path();
    ASSERT_TRUE(std::filesystem::exists(local_dir));
    {
        auto local_files = dmfile->listFilesForUpload();
        data_store->putDMFile(dmfile, oid, /*remove_local*/ true);
        auto remote_files_with_size = listFiles(oid);
        ASSERT_EQ(local_files.size(), remote_files_with_size.size());
        for (const auto & fname : local_files)
        {
            auto itr = remote_files_with_size.find(fname);
            ASSERT_NE(itr, remote_files_with_size.end());
            uploaded_files.push_back(fname);
        }
        LOG_TRACE(log, "remote_files_with_size => {}", remote_files_with_size);
    }
    ASSERT_FALSE(std::filesystem::exists(local_dir));
    ASSERT_EQ(dmfile->path(), S3::S3Filename::fromDMFileOID(oid).toFullKeyWithPrefix());
    read_dmfile(dmfile);

    auto dmfile_from_s3 = restoreDMFile(oid);
    ASSERT_NE(dmfile_from_s3, nullptr);
    read_dmfile(dmfile_from_s3);
}
CATCH

struct TestFileInfo
{
    Int64 total_size{};
    Int64 valid_size{};
    std::chrono::system_clock::time_point mtime;
};

TEST_P(S3FileTest, CheckpointUpload)
try
{
    auto timepoint = std::chrono::system_clock::now();
    using namespace std::chrono_literals;
    StoreID store_id = 987;
    UInt64 sequence = 200;
    const std::vector<TestFileInfo> test_infos{
        // old, big, valid rate is high
        TestFileInfo{.total_size = 345 * 1024, .valid_size = 300 * 1024, .mtime = timepoint - 7200s},
        // old, big, valid rate is low
        TestFileInfo{.total_size = 156 * 1024, .valid_size = 16 * 1024, .mtime = timepoint - 7200s},
        // old, small
        TestFileInfo{.total_size = 1234, .valid_size = 1234, .mtime = timepoint - 7200s},
        // fresh, small
        TestFileInfo{.total_size = 1234, .valid_size = 1024, .mtime = timepoint - 30s},
    };
    // prepare
    {
        Strings data_files{
            getTemporaryPath() + "/data_file_11",
            getTemporaryPath() + "/data_file_12",
            getTemporaryPath() + "/data_file_13",
            getTemporaryPath() + "/data_file_14",
        };
        String manifest_file = getTemporaryPath() + "/manifest";
        for (size_t i = 0; i < test_infos.size(); ++i)
        {
            writeLocalFile(data_files[i], test_infos[i].total_size);
        }
        Poco::File(manifest_file).createFile();
        PS::V3::LocalCheckpointFiles checkpoint{.data_files = data_files, .manifest_file = manifest_file};
        // test upload
        data_store->putCheckpointFiles(checkpoint, store_id, sequence);
    }

    Strings df_keys;
    Strings lock_keys;
    std::unordered_set<String> lock_keyset;
    auto s3client = S3::ClientFactory::instance().sharedTiFlashClient();
    /// test 1: ensure CheckpointDataFile, theirs lock file and CheckpointManifest are uploaded
    {
        for (size_t idx = 0; idx < test_infos.size(); ++idx)
        {
            auto cp_data = S3::S3Filename::newCheckpointData(store_id, sequence, idx);
            ASSERT_TRUE(S3::objectExists(*s3client, cp_data.toFullKey()));
            df_keys.emplace_back(cp_data.toFullKey());
            ASSERT_TRUE(S3::objectExists(*s3client, cp_data.toView().getLockKey(store_id, sequence)));
            lock_keys.emplace_back(cp_data.toView().getLockKey(store_id, sequence));
            lock_keyset.insert(lock_keys.back());
        }
        // manifest
        ASSERT_TRUE(S3::objectExists(*s3client, S3::S3Filename::newCheckpointManifest(store_id, sequence).toFullKey()));
    }

    /// test 2: check get data file infos from remote store
    // add an not exist key for testing
    lock_keys.emplace_back(
        S3::S3Filename::newCheckpointData(store_id, sequence, 999).toView().getLockKey(store_id, sequence));
    lock_keyset.insert(lock_keys.back());

    FailPointHelper::enableFailPoint(
        FailPoints::force_set_mocked_s3_object_mtime,
        std::map<String, Aws::Utils::DateTime>{
            {df_keys[0], test_infos[0].mtime},
            {df_keys[1], test_infos[1].mtime},
            {df_keys[2], test_infos[2].mtime},
            {df_keys[3], test_infos[3].mtime},
        });
    SCOPE_EXIT({ FailPointHelper::disableFailPoint(FailPoints::force_set_mocked_s3_object_mtime); });

    const auto remote_files_info = data_store->getDataFilesInfo(lock_keyset);
    ASSERT_EQ(remote_files_info.size(), 5);
    for (size_t idx = 0; idx < test_infos.size(); ++idx)
    {
        ASSERT_EQ(remote_files_info.at(lock_keys[idx]).size, test_infos[idx].total_size);
        ASSERT_EQ(remote_files_info.at(lock_keys[idx]).mtime, test_infos[idx].mtime) << fmt::format(
            "remote_mtime:{:%Y-%m-%d %H:%M:%S} test_mtime:{:%Y-%m-%d %H:%M:%S}",
            remote_files_info.at(lock_keys[idx]).mtime,
            test_infos[idx].mtime);
    }
    ASSERT_EQ(remote_files_info.at(lock_keys[4]).size, -1); // not exist or exception happens

    /// test 3: create an empty file stat cache
    PS::V3::CPDataFilesStatCache cache;
    auto gc_threshold = DB::DM::Remote::RemoteGCThreshold{
        .min_age_seconds = 3600,
        .valid_rate = 0.2,
        .min_file_threshold = 128 * 1024};
    {
        auto stats = cache.getCopy(); // valid size is not collected
        EXPECT_EQ(stats.size(), 0);
        auto files_to_compact = PS::V3::getRemoteFileIdsNeedCompact(stats, gc_threshold, data_store, log);
        ASSERT_TRUE(files_to_compact.empty());
        // nothing added
        EXPECT_EQ(stats.size(), 0);
    }
    /// test 4: update file stat cache and try get file ids need compact
    {
        cache.updateValidSize(PS::V3::RemoteFileValidSizes{
            {lock_keys[0], test_infos[0].valid_size},
            {lock_keys[1], test_infos[1].valid_size},
            {lock_keys[2], test_infos[2].valid_size},
            {lock_keys[3], test_infos[3].valid_size},
        });
        auto stats = cache.getCopy(); // valid size is collected
        // valid size are updated
        for (size_t idx = 0; idx < test_infos.size(); ++idx)
        {
            EXPECT_EQ(stats.at(lock_keys[idx]).valid_size, test_infos[idx].valid_size);
            EXPECT_LT(stats.at(lock_keys[idx]).total_size, 0); // indicate for invalid
        }
        auto files_to_compact = PS::V3::getRemoteFileIdsNeedCompact(stats, gc_threshold, data_store, log);
        ASSERT_EQ(files_to_compact.size(), 2) << fmt::format("{}", files_to_compact);
        ASSERT_TRUE(files_to_compact.contains(lock_keys[1])) << fmt::format("{}", files_to_compact);
        ASSERT_TRUE(files_to_compact.contains(lock_keys[2])) << fmt::format("{}", files_to_compact);
        ASSERT_TRUE(!files_to_compact.contains(lock_keys[0])) << fmt::format("{}", files_to_compact);
        ASSERT_TRUE(!files_to_compact.contains(lock_keys[3])) << fmt::format("{}", files_to_compact);
        // total size are updated after `getRemoteFileIdsNeedCompact`
        for (size_t idx = 0; idx < test_infos.size(); ++idx)
        {
            EXPECT_EQ(stats.at(lock_keys[idx]).total_size, test_infos[idx].total_size);
            EXPECT_EQ(stats.at(lock_keys[idx]).valid_size, test_infos[idx].valid_size);
            EXPECT_EQ(stats.at(lock_keys[idx]).mtime, test_infos[idx].mtime);
        }

        // update the cache
        cache.updateCache(stats);
    }

    /// test 5: check the cache after updated
    {
        auto stats = cache.getCopy();
        for (size_t idx = 0; idx < test_infos.size(); ++idx)
        {
            EXPECT_EQ(stats.at(lock_keys[idx]).total_size, test_infos[idx].total_size);
            EXPECT_EQ(stats.at(lock_keys[idx]).valid_size, test_infos[idx].valid_size);
            EXPECT_EQ(stats.at(lock_keys[idx]).mtime, test_infos[idx].mtime);
        }
    }
}
CATCH

TEST_P(S3FileTest, RetryWrapper)
try
{
    // Always succ
    {
        Int32 retry = 0;
        retryWrapper(
            [&retry](Int32 max_retry_times, Int32 current_retry) {
                UNUSED(max_retry_times);
                retry = current_retry;
                return true;
            },
            3);
        ASSERT_EQ(retry, 0);
    }

    // Always fail
    {
        Int32 retry = 0;
        try
        {
            retryWrapper(
                [&retry](Int32 max_retry_times, Int32 current_retry) {
                    retry = current_retry;
                    RUNTIME_CHECK(max_retry_times - 1 != current_retry);
                    return false;
                },
                3);
        }
        catch (...)
        {}
        ASSERT_EQ(retry, 2);
    }

    // Partial fail
    {
        Int32 retry = 0;
        retryWrapper(
            [&retry](Int32 max_retry_times, Int32 current_retry) {
                UNUSED(max_retry_times);
                retry = current_retry;
                return current_retry > 0;
            },
            3);
        ASSERT_EQ(retry, 1);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(S3File, S3FileTest, testing::Values(false, true));

} // namespace DB::tests
