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

#include <Common/Exception.h>
#include <Poco/DigestStream.h>
#include <Poco/MD5Engine.h>
#include <Poco/StreamCopier.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/S3/MockS3Client.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>
#include <Storages/S3/S3WritableFile.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketCorsRequest.h>
#include <gtest/gtest.h>

#include <chrono>
#include <fstream>

using namespace std::chrono_literals;
using namespace DB::DM;
using namespace DB::DM::tests;
using namespace DB::S3;

namespace DB::tests
{
using DMFileBlockOutputStreamPtr = std::shared_ptr<DMFileBlockOutputStream>;
using DMFileBlockInputStreamPtr = std::shared_ptr<DMFileBlockInputStream>;

class S3FileTest : public DB::base::TiFlashStorageTestBasic
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

        s3_client = S3::ClientFactory::instance().sharedClient();
        bucket = S3::ClientFactory::instance().bucket();
        ASSERT_TRUE(createBucketIfNotExist());
    }

    void reload()
    {
        TiFlashStorageTestBasic::reload();
    }

    Context & dbContext() { return *db_context; }

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
    void writeFile(const String & key, size_t size, const WriteSettings & write_setting)
    {
        S3WritableFile file(s3_client, bucket, key, write_setting);
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
        S3RandomAccessFile file(s3_client, bucket, key);
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
                ASSERT_EQ(std::vector<char>(tmp_buf.begin(), tmp_buf.begin() + n),
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

    static std::vector<String> getLocalFiles(const String & dir)
    {
        std::vector<String> filenames;
        Poco::DirectoryIterator end;
        for (auto itr = Poco::DirectoryIterator{dir}; itr != end; ++itr)
        {
            if (itr->isFile())
            {
                // `NGC` file is unused in Cloud-Native mode.
                if (itr.name() != "NGC")
                {
                    filenames.push_back(itr.name());
                }
            }
        }
        return filenames;
    }

    std::vector<String> uploadDMFile(DMFilePtr local_dmfile, const DMFileOID & oid)
    {
        Stopwatch sw;
        RUNTIME_CHECK(local_dmfile->fileId() == oid.file_id);

        const auto local_dir = local_dmfile->path();
        auto local_files = getLocalFiles(local_dir);
        RUNTIME_CHECK(!local_files.empty());

        const auto remote_dir = S3::S3Filename::fromDMFileOID(oid).toFullKey();
        LOG_DEBUG(log, "Start upload DMFile, local_dir={} remote_dir={} local_files={}", local_dir, remote_dir, local_files);

        std::vector<std::future<void>> upload_results;
        for (const auto & fname : local_files)
        {
            if (fname == DMFile::metav2FileName())
            {
                // meta file will be upload at last.
                continue;
            }
            auto local_fname = fmt::format("{}/{}", local_dir, fname);
            auto remote_fname = fmt::format("{}/{}", remote_dir, fname);
            S3::uploadFile(*s3_client, bucket, local_fname, remote_fname);
        }

        // Only when the meta upload is successful, the dmfile upload can be considered successful.
        auto local_meta_fname = fmt::format("{}/{}", local_dir, DMFile::metav2FileName());
        auto remote_meta_fname = fmt::format("{}/{}", remote_dir, DMFile::metav2FileName());
        S3::uploadFile(*s3_client, bucket, local_meta_fname, remote_meta_fname);

        LOG_DEBUG(log, "Upload DMFile finished, remote={}, cost={}ms", remote_dir, sw.elapsedMilliseconds());
        return local_files;
    }

    void downloadDMFile(const DMFileOID & remote_oid, const String & local_dir, const std::vector<String> & target_files)
    {
        Stopwatch sw;
        const auto remote_dir = S3::S3Filename::fromDMFileOID(remote_oid).toFullKey();
        std::vector<std::future<void>> download_results;
        for (const auto & name : target_files)
        {
            auto remote_fname = fmt::format("{}/{}", remote_dir, name);
            auto local_fname = fmt::format("{}/{}", local_dir, name);
            S3::downloadFile(*s3_client, bucket, local_fname, remote_fname);
        }
        LOG_DEBUG(log, "Download DMFile meta finished, remote_dir={}, local_dir={} cost={}ms", remote_dir, local_dir, sw.elapsedMilliseconds());
    }

    std::unordered_map<String, size_t> listFiles(const DMFileOID & oid)
    {
        auto dmfile_dir = DMFile::getPathByStatus(
            S3::S3Filename::fromTableID(oid.store_id, oid.table_id).toFullKey(),
            oid.file_id,
            DMFile::Status::READABLE);
        return S3::listPrefixWithSize(*s3_client, bucket, dmfile_dir + "/");
    }

    DMFilePtr restoreDMFile(const DMFileOID & oid)
    {
        return DMFile::restore(db_context->getFileProvider(), oid.file_id, oid.file_id, S3::S3Filename::fromTableID(oid.store_id, oid.table_id).toFullKeyWithPrefix(), DMFile::ReadMetaMode::all());
    }

    LoggerPtr log;
    std::vector<char> buf_unit;
    std::shared_ptr<Aws::S3::S3Client> s3_client;
    String bucket;
    S3WritableFile::UploadInfo last_upload_info;
};

TEST_F(S3FileTest, SinglePart)
try
{
    for (int i = 0; i < 10; i++)
    {
        const size_t size = 256 * i + ::rand() % 256;
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

TEST_F(S3FileTest, MultiPart)
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

TEST_F(S3FileTest, Seek)
try
{
    const auto size = 1024 * 1024 * 10; // 10MB
    WriteSettings write_setting;
    const String key = "/a/b/c/seek";
    writeFile(key, size, write_setting);
    S3RandomAccessFile file(s3_client, bucket, key);
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

TEST_F(S3FileTest, WriteRead)
try
{
    auto cols = DMTestEnv::getDefaultColumns();

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
    oid.file_id = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
    {
        // Prepare for write
        // Block 1: [0, 64)
        Block block1 = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        // Block 2: [64, 128)
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);

        auto configuration = std::make_optional<DMChecksumConfig>();
        dmfile = DMFile::create(oid.file_id, parent_path, std::move(configuration), DMFileFormat::V3);
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dmfile, *cols);
        stream->writePrefix();
        stream->write(block1, block_property1);
        stream->write(block2, block_property2);
        stream->writeSuffix();

        ASSERT_EQ(dmfile->getPackProperties().property_size(), 2);
    }

    std::vector<String> uploaded_files;
    {
        uploaded_files = uploadDMFile(dmfile, oid);
        auto files_with_size = listFiles(oid);
        ASSERT_EQ(uploaded_files.size(), files_with_size.size());
        LOG_TRACE(log, "{}\n", files_with_size);
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
            ASSERT_EQ(copy_md5, local_md5) << fmt::format("local_fname={}, copy_fname={}", local_fname, copy_fname);
            LOG_TRACE(log, "local_fname={}, copy_fname={}, md5={}", local_fname, copy_fname, local_md5);
        }
    }

    auto dmfile_from_s3 = restoreDMFile(oid);
    ASSERT_NE(dmfile_from_s3, nullptr);
    try
    {
        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.build(dmfile_from_s3, *cols, RowKeyRanges{RowKeyRange::newAll(false, 1)}, std::make_shared<ScanContext>());
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
} // namespace DB::tests