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

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <IO/IOThreadPools.h>
#include <Interpreters/Context.h>
#include <Server/StorageConfigParser.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/KVStore/Types.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3WritableFile.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <fmt/compile.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>

using namespace DB;
using namespace DB::S3;
using namespace std::chrono_literals;
using DMFileOID = ::DB::S3::DMFileOID;
using S3Filename = ::DB::S3::S3Filename;
using FileType = ::DB::FileSegment::FileType;

namespace DB::ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
}

namespace DB::tests::S3
{
class FileCacheTest : public ::testing::Test
{
public:
    void SetUp() override
    {
        tmp_dir = DB::tests::TiFlashTestEnv::getTemporaryPath("FileCacheTest");
        DB::tests::TiFlashTestEnv::enableS3Config();
        log = Logger::get("FileCacheTest");
        std::filesystem::remove_all(std::filesystem::path(tmp_dir));
        s3_client = ::DB::S3::ClientFactory::instance().sharedTiFlashClient();
        ASSERT_TRUE(DB::tests::TiFlashTestEnv::createBucketIfNotExist(*s3_client));
        std::random_device dev;
        rng = std::mt19937{dev()};
        next_id = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        capacity_metrics = TiFlashTestEnv::getContext()->getPathCapacity();
    }

    void TearDown() override { DB::tests::TiFlashTestEnv::disableS3Config(); }

protected:
    std::shared_ptr<TiFlashS3Client> s3_client;
    std::mt19937 rng;
    UInt64 next_id = 0;

    inline static const std::vector<String> basenames = {
        "%2D1.dat",
        "%2D1.mrk",
        "%2D1.idx",
        "%2D1024.dat",
        "%2D1024.mrk",
        "%2D1024.idx",
        "%2D1025.dat",
        "%2D1025.mrk",
        "%2D1025.idx",
        "1.dat",
        "1.mrk",
        "1.idx",
        "1.null.dat",
        "1.null.mrk",
        "meta",
    };


    void writeFile(const String & key, char value, size_t size, const WriteSettings & write_setting)
    {
        Stopwatch sw;
        S3WritableFile file(s3_client, key, write_setting);
        size_t write_size = 0;
        constexpr size_t buf_size = 1024 * 1024 * 10;
        String buf_unit(buf_size, value);
        while (write_size < size)
        {
            auto to_write = std::min(buf_unit.size(), size - write_size);
            auto n = file.write(buf_unit.data(), to_write);
            ASSERT_EQ(n, to_write);
            write_size += n;
        }
        auto r = file.fsync();
        ASSERT_EQ(r, 0);
        LOG_DEBUG(log, "write fname={} size={} done, cost={}s", key, size, sw.elapsedSeconds());
    }

    void writeS3FileWithSize(const S3Filename & s3_dir, std::string_view file_name, size_t size)
    {
        std::vector<UInt8> data;
        data.resize(size);
        writeFile(fmt::format("{}/{}", s3_dir.toFullKey(), file_name), '0', size, WriteSettings{});
    }

    struct ObjectInfo
    {
        String key;
        char value;
        size_t size;
    };

    std::vector<ObjectInfo> genDMFile(const DMFileOID & oid, const std::vector<String> & names)
    {
        auto dmfile_name = S3Filename::fromDMFileOID(oid).toFullKey();
        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<std::mt19937::result_type> rnd_size(1024 * 1024 * 15, 1024 * 1024 * 25);
        std::uniform_int_distribution<std::mt19937::result_type> rnd_value(0, 255);
        std::vector<ObjectInfo> objects;
        std::vector<std::future<void>> upload_results;
        for (const auto & name : names)
        {
            String key = fmt::format("{}/{}", dmfile_name, name);
            char value = rnd_value(rng);
            size_t size = rnd_size(rng);

            auto task = std::make_shared<std::packaged_task<void()>>(
                [&, key = key, value = value, size = size]() { writeFile(key, value, size, WriteSettings{}); });
            upload_results.push_back(task->get_future());
            S3FileCachePool::get().scheduleOrThrowOnError([task]() { (*task)(); });
            objects.emplace_back(ObjectInfo{.key = key, .value = value, .size = size});
        }
        for (auto & f : upload_results)
        {
            f.get();
        }
        return objects;
    }

    UInt64 nextId() { return next_id++; }
    std::vector<ObjectInfo> genObjects(
        UInt32 store_count,
        UInt32 table_count,
        UInt32 file_count,
        const std::vector<String> & names)
    {
        std::vector<ObjectInfo> objects;
        for (UInt32 i = 1; i <= store_count; ++i)
        {
            for (UInt32 j = 1; j <= table_count; ++j)
            {
                for (UInt32 k = 1; k <= file_count; ++k)
                {
                    auto objs = genDMFile(
                        DMFileOID{.store_id = nextId(), .table_id = static_cast<Int64>(nextId()), .file_id = nextId()},
                        names);
                    objects.insert(objects.end(), objs.begin(), objs.end());
                }
            }
        }
        return objects;
    }

    static size_t objectsTotalSize(const std::vector<ObjectInfo> & objects)
    {
        size_t total_size = 0;
        for (const auto & obj : objects)
        {
            total_size += obj.size;
        }
        return total_size;
    }

    void waitForBgDownload(const FileCache & file_cache)
    {
        Stopwatch sw;
        while (file_cache.bg_downloading_count.load(std::memory_order_relaxed) > 0)
        {
            std::this_thread::sleep_for(1000ms);
        }
        LOG_DEBUG(
            log,
            "Download summary: succ={} fail={} cost={}s",
            file_cache.bg_download_succ_count.load(std::memory_order_relaxed),
            file_cache.bg_download_fail_count.load(std::memory_order_relaxed),
            sw.elapsedSeconds());
    }

    static void calculateCacheCapacity(StorageRemoteCacheConfig & config, UInt64 dt_size)
    {
        config.capacity = dt_size / (1.0 - config.delta_rate);
        bool forward = false;
        bool backward = false;
        while (config.getDTFileCapacity() != dt_size)
        {
            if (config.getDTFileCapacity() > dt_size)
            {
                backward = true;
                config.capacity--;
            }
            else
            {
                forward = true;
                config.capacity++;
            }
            ASSERT_FALSE(forward && backward) << fmt::format("delta_rate {} dt_size {}", config.delta_rate, dt_size);
        }
    }

    String tmp_dir;
    UInt64 cache_capacity = 100 * 1024 * 1024;
    const UInt64 cache_level = 5;
    UInt64 cache_min_age_seconds = 30 * 60;
    LoggerPtr log;
    PathCapacityMetricsPtr capacity_metrics;
};

TEST_F(FileCacheTest, Main)
try
{
    Stopwatch sw;
    auto objects = genObjects(/*store_count*/ 1, /*table_count*/ 1, /*file_count*/ 1, basenames);
    auto total_size = objectsTotalSize(objects);
    LOG_DEBUG(log, "genObjects: count={} total_size={} cost={}s", objects.size(), total_size, sw.elapsedSeconds());

    auto cache_dir = fmt::format("{}/file_cache_all", tmp_dir);
    StorageRemoteCacheConfig cache_config{.dir = cache_dir, .dtfile_level = 100};
    calculateCacheCapacity(cache_config, total_size);
    LOG_DEBUG(log, "total_size={} dt_cache_capacity={}", total_size, cache_config.getDTFileCapacity());

    {
        LOG_DEBUG(log, "Cache all data");
        FileCache file_cache(capacity_metrics, cache_config);
        for (const auto & obj : objects)
        {
            auto s3_fname = ::DB::S3::S3FilenameView::fromKey(obj.key);
            ASSERT_TRUE(s3_fname.isDataFile()) << obj.key;
            ASSERT_EQ(file_cache.get(s3_fname), nullptr) << obj.key;
        }
        waitForBgDownload(file_cache);
        ASSERT_EQ(file_cache.bg_download_fail_count.load(std::memory_order_relaxed), 0);
        ASSERT_EQ(file_cache.bg_download_succ_count.load(std::memory_order_relaxed), objects.size());
        ASSERT_EQ(file_cache.cache_used, file_cache.cache_capacity);
        for (const auto & obj : objects)
        {
            auto s3_fname = ::DB::S3::S3FilenameView::fromKey(obj.key);
            ASSERT_TRUE(s3_fname.isDataFile()) << obj.key;
            auto file_seg = file_cache.get(s3_fname);
            ASSERT_NE(file_seg, nullptr) << obj.key;
            ASSERT_TRUE(file_seg->isReadyToRead());
            ASSERT_EQ(file_seg->getSize(), obj.size);
        }
    }

    {
        LOG_DEBUG(log, "Cache restore");
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_EQ(file_cache.cache_used, file_cache.cache_capacity);
        for (const auto & obj : objects)
        {
            auto s3_fname = ::DB::S3::S3FilenameView::fromKey(obj.key);
            ASSERT_TRUE(s3_fname.isDataFile()) << obj.key;
            auto file_seg = file_cache.get(s3_fname);
            ASSERT_NE(file_seg, nullptr) << obj.key;
            ASSERT_TRUE(file_seg->isReadyToRead());
            ASSERT_EQ(file_seg->getSize(), obj.size);
        }
    }

    auto meta_objects = genObjects(/*store_count*/ 2, /*table_count*/ 2, /*file_count*/ 2, {"meta"});
    ASSERT_EQ(meta_objects.size(), 2 * 2 * 2);
    {
        LOG_DEBUG(log, "Evict success");
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_LE(file_cache.cache_used, file_cache.cache_capacity);
        for (const auto & obj : meta_objects)
        {
            auto s3_fname = ::DB::S3::S3FilenameView::fromKey(obj.key);
            ASSERT_TRUE(s3_fname.isDataFile()) << obj.key;
            auto file_seg = file_cache.get(s3_fname);
            if (file_seg == nullptr)
            {
                waitForBgDownload(file_cache);
                file_seg = file_cache.get(s3_fname);
            }
            ASSERT_NE(file_seg, nullptr) << obj.key;
            ASSERT_TRUE(file_seg->isReadyToRead());
            ASSERT_EQ(file_seg->getSize(), obj.size);
        }
    }

    auto meta_objects2 = genObjects(/*store_count*/ 2, /*table_count*/ 2, /*file_count*/ 2, {"meta"});
    ASSERT_EQ(meta_objects2.size(), 2 * 2 * 2);
    {
        LOG_DEBUG(log, "Evict failed");
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_LE(file_cache.cache_used, file_cache.cache_capacity);
        UInt64 free_size = file_cache.cache_capacity - file_cache.cache_used;
        auto file_seg = file_cache.getAll(); // Prevent file_segment from evicted.
        for (const auto & obj : meta_objects2)
        {
            auto s3_fname = ::DB::S3::S3FilenameView::fromKey(obj.key);
            ASSERT_TRUE(s3_fname.isDataFile()) << obj.key;
            auto file_seg = file_cache.get(s3_fname);
            if (file_seg == nullptr)
            {
                waitForBgDownload(file_cache);
                file_seg = file_cache.get(s3_fname);
                if (free_size > obj.size)
                {
                    free_size -= obj.size;
                    ASSERT_EQ(free_size, file_cache.cache_capacity - file_cache.cache_used);
                    ASSERT_NE(file_seg, nullptr) << obj.key;
                    ASSERT_TRUE(file_seg->isReadyToRead());
                    ASSERT_EQ(file_seg->getSize(), obj.size);
                }
                else
                {
                    ASSERT_EQ(file_seg, nullptr) << obj.key;
                }
            }
        }
        waitForBgDownload(file_cache);
    }
}
CATCH

TEST_F(FileCacheTest, FileSystem)
{
    auto cache_dir = fmt::format("{}/filesystem", tmp_dir);
    StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = cache_level};
    FileCache file_cache(capacity_metrics, cache_config);
    DMFileOID dmfile_oid = {.store_id = 1, .table_id = 2, .file_id = 3};

    // s1/data/t_2/dmf_3
    auto s3_fname = S3Filename::fromDMFileOID(dmfile_oid).toFullKey();
    ASSERT_EQ(s3_fname, "s1/data/t_2/dmf_3");
    auto remote_fname1 = fmt::format("{}/1.dat", s3_fname);
    auto local_fname1 = file_cache.toLocalFilename(remote_fname1);
    ASSERT_EQ(local_fname1, fmt::format("{}/{}", cache_config.getDTFileCacheDir(), remote_fname1));
    auto tmp_remote_fname1 = file_cache.toS3Key(local_fname1);
    ASSERT_EQ(tmp_remote_fname1, remote_fname1);

    auto tmp_local_fname1 = FileCache::toTemporaryFilename(local_fname1);
    ASSERT_FALSE(FileCache::isTemporaryFilename(local_fname1));
    ASSERT_TRUE(FileCache::isTemporaryFilename(tmp_local_fname1));

    FileCache::prepareParentDir(local_fname1);
    std::filesystem::path local_file1(local_fname1);
    auto dmf = local_file1.parent_path();
    ASSERT_TRUE(std::filesystem::exists(dmf));

    // Create file 1.
    ASSERT_FALSE(std::filesystem::exists(local_file1));
    {
        std::ofstream ofs(local_fname1);
    }
    ASSERT_TRUE(std::filesystem::exists(local_file1));

    // Create file 2.
    auto remote_fname2 = fmt::format("{}/2.dat", s3_fname);
    auto local_fname2 = file_cache.toLocalFilename(remote_fname2);
    std::filesystem::path local_file2(local_fname2);
    ASSERT_FALSE(std::filesystem::exists(local_file2));
    {
        std::ofstream ofs(local_fname2);
    }
    ASSERT_TRUE(std::filesystem::exists(local_file2));

    file_cache.removeDiskFile(local_fname1, false);
    ASSERT_FALSE(std::filesystem::exists(local_file1)) << local_file1.generic_string();
    ASSERT_TRUE(std::filesystem::exists(local_file2)) << local_file2.generic_string();
    ASSERT_TRUE(std::filesystem::exists(dmf)) << dmf.generic_string();
    auto table = dmf.parent_path();
    ASSERT_TRUE(std::filesystem::exists(table)) << table.generic_string();
    auto store_data = table.parent_path();
    ASSERT_TRUE(std::filesystem::exists(store_data)) << store_data.generic_string();
    auto store = store_data.parent_path();
    ASSERT_TRUE(std::filesystem::exists(store)) << store.generic_string();
    auto cache_root = store.parent_path();
    ASSERT_TRUE(std::filesystem::exists(cache_root)) << cache_root.generic_string();
    ASSERT_EQ(cache_root.generic_string(), cache_config.getDTFileCacheDir());

    file_cache.removeDiskFile(local_fname2, false);
    ASSERT_FALSE(std::filesystem::exists(local_file2)) << local_file2.generic_string();
    ASSERT_FALSE(std::filesystem::exists(dmf)) << dmf.generic_string();
    ASSERT_FALSE(std::filesystem::exists(table)) << table.generic_string();
    ASSERT_FALSE(std::filesystem::exists(store_data)) << store_data.generic_string();
    ASSERT_FALSE(std::filesystem::exists(store)) << store.generic_string();
    ASSERT_TRUE(std::filesystem::exists(cache_root)) << cache_root.generic_string();
}

TEST_F(FileCacheTest, FileType)
try
{
    DMFileOID dmfile_oid = {.store_id = 1, .table_id = 2, .file_id = 3};
    // s1/t_2/dmf_3
    auto s3_fname = S3Filename::fromDMFileOID(dmfile_oid).toFullKey();
    auto meta_fname = fmt::format("{}/meta", s3_fname);
    ASSERT_EQ(FileCache::getFileType(meta_fname), FileType::Meta);
    auto data_fname = fmt::format("{}/1.dat", s3_fname);
    ASSERT_EQ(FileCache::getFileType(data_fname), FileType::ColData);
    auto mark_fname = fmt::format("{}/1.mrk", s3_fname);
    ASSERT_EQ(FileCache::getFileType(mark_fname), FileType::Mark);
    auto null_fname = fmt::format("{}/1.null.dat", s3_fname);
    ASSERT_EQ(FileCache::getFileType(null_fname), FileType::NullMap);
    auto null_mark_fname = fmt::format("{}/1.null.mrk", s3_fname);
    ASSERT_EQ(FileCache::getFileType(null_mark_fname), FileType::Mark);
    auto index_fname = fmt::format("{}/1.idx", s3_fname);
    ASSERT_EQ(FileCache::getFileType(index_fname), FileType::Index);
    auto handle_fname = fmt::format(
        "{}/{}.dat",
        s3_fname,
        IDataType::getFileNameForStream(std::to_string(MutSup::extra_handle_id), {}));
    ASSERT_EQ(FileCache::getFileType(handle_fname), FileType::HandleColData);
    auto vec_index_fname = fmt::format("{}/idx_{}.vector", s3_fname, /*index_id*/ 50); // DMFile::vectorIndexFileName
    ASSERT_EQ(FileCache::getFileType(vec_index_fname), FileType::VectorIndex);
    auto version_fname = fmt::format(
        "{}/{}.dat",
        s3_fname,
        IDataType::getFileNameForStream(std::to_string(MutSup::version_col_id), {}));
    ASSERT_EQ(FileCache::getFileType(version_fname), FileType::VersionColData);
    auto delmark_fname = fmt::format(
        "{}/{}.dat",
        s3_fname,
        IDataType::getFileNameForStream(std::to_string(MutSup::delmark_col_id), {}));
    ASSERT_EQ(FileCache::getFileType(delmark_fname), FileType::DeleteMarkColData);
    auto unknow_fname0 = fmt::format("{}/123456", s3_fname);
    ASSERT_EQ(FileCache::getFileType(unknow_fname0), FileType::Unknow);
    auto unknow_fname1 = fmt::format("{}/123456.lock", s3_fname);
    ASSERT_EQ(FileCache::getFileType(unknow_fname1), FileType::Unknow);

    {
        UInt64 level = 0;
        auto cache_dir = fmt::format("{}/filetype{}", tmp_dir, level);
        StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = level};
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_FALSE(file_cache.canCache(FileType::Unknow));
        ASSERT_FALSE(file_cache.canCache(FileType::Meta));
        ASSERT_FALSE(file_cache.canCache(FileType::Merged));
        ASSERT_FALSE(file_cache.canCache(FileType::Index));
        ASSERT_FALSE(file_cache.canCache(FileType::Mark));
        ASSERT_FALSE(file_cache.canCache(FileType::NullMap));
        ASSERT_FALSE(file_cache.canCache(FileType::DeleteMarkColData));
        ASSERT_FALSE(file_cache.canCache(FileType::VersionColData));
        ASSERT_FALSE(file_cache.canCache(FileType::HandleColData));
        ASSERT_FALSE(file_cache.canCache(FileType::ColData));
    }
    {
        UInt64 level = 1;
        auto cache_dir = fmt::format("{}/filetype{}", tmp_dir, level);
        StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = level};
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_FALSE(file_cache.canCache(FileType::Unknow));
        ASSERT_TRUE(file_cache.canCache(FileType::Meta));
        ASSERT_FALSE(file_cache.canCache(FileType::VectorIndex));
        ASSERT_FALSE(file_cache.canCache(FileType::Merged));
        ASSERT_FALSE(file_cache.canCache(FileType::Index));
        ASSERT_FALSE(file_cache.canCache(FileType::Mark));
        ASSERT_FALSE(file_cache.canCache(FileType::NullMap));
        ASSERT_FALSE(file_cache.canCache(FileType::DeleteMarkColData));
        ASSERT_FALSE(file_cache.canCache(FileType::VersionColData));
        ASSERT_FALSE(file_cache.canCache(FileType::HandleColData));
        ASSERT_FALSE(file_cache.canCache(FileType::ColData));
    }
    {
        UInt64 level = 2;
        auto cache_dir = fmt::format("{}/filetype{}", tmp_dir, level);
        StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = level};
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_FALSE(file_cache.canCache(FileType::Unknow));
        ASSERT_TRUE(file_cache.canCache(FileType::Meta));
        ASSERT_TRUE(file_cache.canCache(FileType::VectorIndex));
        ASSERT_FALSE(file_cache.canCache(FileType::Merged));
        ASSERT_FALSE(file_cache.canCache(FileType::Index));
        ASSERT_FALSE(file_cache.canCache(FileType::Mark));
        ASSERT_FALSE(file_cache.canCache(FileType::NullMap));
        ASSERT_FALSE(file_cache.canCache(FileType::DeleteMarkColData));
        ASSERT_FALSE(file_cache.canCache(FileType::VersionColData));
        ASSERT_FALSE(file_cache.canCache(FileType::HandleColData));
        ASSERT_FALSE(file_cache.canCache(FileType::ColData));
    }
    {
        UInt64 level = 3;
        auto cache_dir = fmt::format("{}/filetype{}", tmp_dir, level);
        StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = level};
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_FALSE(file_cache.canCache(FileType::Unknow));
        ASSERT_TRUE(file_cache.canCache(FileType::Meta));
        ASSERT_TRUE(file_cache.canCache(FileType::VectorIndex));
        ASSERT_TRUE(file_cache.canCache(FileType::Merged));
        ASSERT_FALSE(file_cache.canCache(FileType::Index));
        ASSERT_FALSE(file_cache.canCache(FileType::Mark));
        ASSERT_FALSE(file_cache.canCache(FileType::NullMap));
        ASSERT_FALSE(file_cache.canCache(FileType::DeleteMarkColData));
        ASSERT_FALSE(file_cache.canCache(FileType::VersionColData));
        ASSERT_FALSE(file_cache.canCache(FileType::HandleColData));
        ASSERT_FALSE(file_cache.canCache(FileType::ColData));
    }
    {
        UInt64 level = 4;
        auto cache_dir = fmt::format("{}/filetype{}", tmp_dir, level);
        StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = level};
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_FALSE(file_cache.canCache(FileType::Unknow));
        ASSERT_TRUE(file_cache.canCache(FileType::Meta));
        ASSERT_TRUE(file_cache.canCache(FileType::VectorIndex));
        ASSERT_TRUE(file_cache.canCache(FileType::Merged));
        ASSERT_TRUE(file_cache.canCache(FileType::Index));
        ASSERT_FALSE(file_cache.canCache(FileType::Mark));
        ASSERT_FALSE(file_cache.canCache(FileType::NullMap));
        ASSERT_FALSE(file_cache.canCache(FileType::DeleteMarkColData));
        ASSERT_FALSE(file_cache.canCache(FileType::VersionColData));
        ASSERT_FALSE(file_cache.canCache(FileType::HandleColData));
        ASSERT_FALSE(file_cache.canCache(FileType::ColData));
    }
    {
        UInt64 level = 5;
        auto cache_dir = fmt::format("{}/filetype{}", tmp_dir, level);
        StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = level};
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_FALSE(file_cache.canCache(FileType::Unknow));
        ASSERT_TRUE(file_cache.canCache(FileType::Meta));
        ASSERT_TRUE(file_cache.canCache(FileType::VectorIndex));
        ASSERT_TRUE(file_cache.canCache(FileType::Merged));
        ASSERT_TRUE(file_cache.canCache(FileType::Index));
        ASSERT_TRUE(file_cache.canCache(FileType::Mark));
        ASSERT_FALSE(file_cache.canCache(FileType::NullMap));
        ASSERT_FALSE(file_cache.canCache(FileType::DeleteMarkColData));
        ASSERT_FALSE(file_cache.canCache(FileType::VersionColData));
        ASSERT_FALSE(file_cache.canCache(FileType::HandleColData));
        ASSERT_FALSE(file_cache.canCache(FileType::ColData));
    }
    {
        UInt64 level = 6;
        auto cache_dir = fmt::format("{}/filetype{}", tmp_dir, level);
        StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = level};
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_FALSE(file_cache.canCache(FileType::Unknow));
        ASSERT_TRUE(file_cache.canCache(FileType::Meta));
        ASSERT_TRUE(file_cache.canCache(FileType::VectorIndex));
        ASSERT_TRUE(file_cache.canCache(FileType::Merged));
        ASSERT_TRUE(file_cache.canCache(FileType::Index));
        ASSERT_TRUE(file_cache.canCache(FileType::Mark));
        ASSERT_TRUE(file_cache.canCache(FileType::NullMap));
        ASSERT_FALSE(file_cache.canCache(FileType::DeleteMarkColData));
        ASSERT_FALSE(file_cache.canCache(FileType::VersionColData));
        ASSERT_FALSE(file_cache.canCache(FileType::HandleColData));
        ASSERT_FALSE(file_cache.canCache(FileType::ColData));
    }
    {
        UInt64 level = 7;
        auto cache_dir = fmt::format("{}/filetype{}", tmp_dir, level);
        StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = level};
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_FALSE(file_cache.canCache(FileType::Unknow));
        ASSERT_TRUE(file_cache.canCache(FileType::Meta));
        ASSERT_TRUE(file_cache.canCache(FileType::VectorIndex));
        ASSERT_TRUE(file_cache.canCache(FileType::Merged));
        ASSERT_TRUE(file_cache.canCache(FileType::Index));
        ASSERT_TRUE(file_cache.canCache(FileType::Mark));
        ASSERT_TRUE(file_cache.canCache(FileType::NullMap));
        ASSERT_TRUE(file_cache.canCache(FileType::DeleteMarkColData));
        ASSERT_FALSE(file_cache.canCache(FileType::VersionColData));
        ASSERT_FALSE(file_cache.canCache(FileType::HandleColData));
        ASSERT_FALSE(file_cache.canCache(FileType::ColData));
    }
    {
        UInt64 level = 8;
        auto cache_dir = fmt::format("{}/filetype{}", tmp_dir, level);
        StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = level};
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_FALSE(file_cache.canCache(FileType::Unknow));
        ASSERT_TRUE(file_cache.canCache(FileType::Meta));
        ASSERT_TRUE(file_cache.canCache(FileType::VectorIndex));
        ASSERT_TRUE(file_cache.canCache(FileType::Merged));
        ASSERT_TRUE(file_cache.canCache(FileType::Index));
        ASSERT_TRUE(file_cache.canCache(FileType::Mark));
        ASSERT_TRUE(file_cache.canCache(FileType::NullMap));
        ASSERT_TRUE(file_cache.canCache(FileType::DeleteMarkColData));
        ASSERT_TRUE(file_cache.canCache(FileType::VersionColData));
        ASSERT_FALSE(file_cache.canCache(FileType::HandleColData));
        ASSERT_FALSE(file_cache.canCache(FileType::ColData));
    }
    {
        UInt64 level = 9;
        auto cache_dir = fmt::format("{}/filetype{}", tmp_dir, level);
        StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = level};
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_FALSE(file_cache.canCache(FileType::Unknow));
        ASSERT_TRUE(file_cache.canCache(FileType::Meta));
        ASSERT_TRUE(file_cache.canCache(FileType::VectorIndex));
        ASSERT_TRUE(file_cache.canCache(FileType::Merged));
        ASSERT_TRUE(file_cache.canCache(FileType::Index));
        ASSERT_TRUE(file_cache.canCache(FileType::Mark));
        ASSERT_TRUE(file_cache.canCache(FileType::NullMap));
        ASSERT_TRUE(file_cache.canCache(FileType::DeleteMarkColData));
        ASSERT_TRUE(file_cache.canCache(FileType::VersionColData));
        ASSERT_TRUE(file_cache.canCache(FileType::HandleColData));
        ASSERT_FALSE(file_cache.canCache(FileType::ColData));
    }
    {
        UInt64 level = 10;
        auto cache_dir = fmt::format("{}/filetype{}", tmp_dir, level);
        StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = level};
        FileCache file_cache(capacity_metrics, cache_config);
        ASSERT_FALSE(file_cache.canCache(FileType::Unknow));
        ASSERT_TRUE(file_cache.canCache(FileType::Meta));
        ASSERT_TRUE(file_cache.canCache(FileType::VectorIndex));
        ASSERT_TRUE(file_cache.canCache(FileType::Merged));
        ASSERT_TRUE(file_cache.canCache(FileType::Index));
        ASSERT_TRUE(file_cache.canCache(FileType::Mark));
        ASSERT_TRUE(file_cache.canCache(FileType::NullMap));
        ASSERT_TRUE(file_cache.canCache(FileType::DeleteMarkColData));
        ASSERT_TRUE(file_cache.canCache(FileType::VersionColData));
        ASSERT_TRUE(file_cache.canCache(FileType::HandleColData));
        ASSERT_TRUE(file_cache.canCache(FileType::ColData));
    }
}
CATCH

TEST_F(FileCacheTest, Space)
{
    auto cache_dir = fmt::format("{}/space", tmp_dir);
    StorageRemoteCacheConfig cache_config{.dir = cache_dir, .capacity = cache_capacity, .dtfile_level = cache_level};
    FileCache file_cache(capacity_metrics, cache_config);
    auto dt_cache_capacity = cache_config.getDTFileCapacity();
    ASSERT_TRUE(file_cache.reserveSpace(FileType::Meta, dt_cache_capacity - 1024, FileCache::EvictMode::NoEvict));
    ASSERT_TRUE(file_cache.reserveSpace(FileType::Meta, 512, FileCache::EvictMode::NoEvict));
    ASSERT_TRUE(file_cache.reserveSpace(FileType::Meta, 256, FileCache::EvictMode::NoEvict));
    ASSERT_TRUE(file_cache.reserveSpace(FileType::Meta, 256, FileCache::EvictMode::NoEvict));
    ASSERT_FALSE(file_cache.reserveSpace(FileType::Meta, 1, FileCache::EvictMode::NoEvict));
    ASSERT_FALSE(file_cache.finalizeReservedSize(FileType::Meta, /*reserved_size*/ 512, /*content_length*/ 513));
    ASSERT_TRUE(file_cache.finalizeReservedSize(FileType::Meta, /*reserved_size*/ 512, /*content_length*/ 511));
    ASSERT_TRUE(file_cache.reserveSpace(FileType::Meta, 1, FileCache::EvictMode::NoEvict));
    ASSERT_FALSE(file_cache.reserveSpace(FileType::Meta, 1, FileCache::EvictMode::NoEvict));
    file_cache.releaseSpace(dt_cache_capacity);
    ASSERT_TRUE(file_cache.reserveSpace(FileType::Meta, dt_cache_capacity, FileCache::EvictMode::NoEvict));
    ASSERT_FALSE(file_cache.reserveSpace(FileType::Meta, 1, FileCache::EvictMode::NoEvict));
}

TEST_F(FileCacheTest, LRUFileTable)
{
    LRUFileTable table;

    ASSERT_EQ(table.get("aaa"), nullptr);

    auto file_seg = std::make_shared<FileSegment>("filename", FileSegment::Status::Complete, 1, FileType::Meta);
    table.set("aaa", file_seg);

    auto f = table.get("aaa");
    ASSERT_NE(f, nullptr);
    ASSERT_EQ(f.get(), file_seg.get());

    table.set("bbb", file_seg);
    table.set("ccc", file_seg);
    table.set("ddd", file_seg);

    {
        std::vector<String> seqs{"aaa", "bbb", "ccc", "ddd"};
        auto seqs_itr = seqs.begin();
        for (auto itr = table.begin(); itr != table.end(); ++itr, ++seqs_itr)
        {
            ASSERT_EQ(*itr, *seqs_itr);
        }
    }

    ASSERT_NE(table.get("aaa"), nullptr);
    ASSERT_NE(table.get("ccc"), nullptr);

    {
        std::vector<String> seqs{"bbb", "ddd", "aaa", "ccc"};
        auto seqs_itr = seqs.begin();
        for (auto itr = table.begin(); itr != table.end(); ++itr, ++seqs_itr)
        {
            ASSERT_EQ(*itr, *seqs_itr);
        }
    }

    table.set("ccc", file_seg);
    table.set("ddd", file_seg);

    {
        std::vector<String> seqs{"bbb", "aaa", "ccc", "ddd"};
        auto seqs_itr = seqs.begin();
        for (auto itr = table.begin(); itr != table.end(); ++itr, ++seqs_itr)
        {
            ASSERT_EQ(*itr, *seqs_itr);
        }

        seqs_itr = seqs.begin();
        for (auto itr = table.begin(); itr != table.end(); ++seqs_itr)
        {
            ASSERT_EQ(*itr, *seqs_itr);
            if (*itr == "aaa" || *itr == "ccc")
            {
                itr = table.remove(*itr);
            }
            else
            {
                ++itr;
            }
        }
        ASSERT_EQ(seqs_itr, seqs.end());
    }

    {
        std::vector<String> seqs{"bbb", "ddd"};
        auto seqs_itr = seqs.begin();
        for (auto itr = table.begin(); itr != table.end(); ++seqs_itr)
        {
            ASSERT_EQ(*itr, *seqs_itr);
            itr = table.remove(*itr);
        }
        ASSERT_EQ(seqs_itr, seqs.end());
        ASSERT_EQ(table.begin(), table.end());
    }
}

TEST_F(FileCacheTest, EvictEmptyFile)
try
{
    Stopwatch sw;
    auto objects = genObjects(/*store_count*/ 1, /*table_count*/ 1, /*file_count*/ 10, {"meta"});
    auto total_size = objectsTotalSize(objects);
    LOG_DEBUG(log, "genObjects: count={} total_size={} cost={}s", objects.size(), total_size, sw.elapsedSeconds());
    auto cache_dir = fmt::format("{}/evict_empty_file", tmp_dir);
    StorageRemoteCacheConfig cache_config{.dir = cache_dir, .dtfile_level = 100};
    calculateCacheCapacity(cache_config, total_size);
    LOG_DEBUG(log, "total_size={} dt_cache_capacity={}", total_size, cache_config.getDTFileCapacity());
    FileCache file_cache(capacity_metrics, cache_config);

    // Cache empty files
    DMFileOID empty_file_oid{.store_id = 111, .table_id = 2222, .file_id = 33333};
    auto empty_s3_dmfile_path = ::DB::S3::S3Filename::fromDMFileOID(empty_file_oid).toFullKey();
    std::vector<String> empty_s3_keys;
    for (int i = 0; i < 5; ++i)
    {
        auto s3_key = fmt::format("{}/{}.dat", empty_s3_dmfile_path, i);
        uploadEmptyFile(*s3_client, s3_key);
        empty_s3_keys.push_back(s3_key);
    }
    for (const auto & s3_key : empty_s3_keys)
    {
        auto s3_fname = ::DB::S3::S3FilenameView::fromKey(s3_key);
        ASSERT_EQ(file_cache.get(s3_fname), nullptr) << s3_key;
        waitForBgDownload(file_cache);
    }

    // Cache empty files succ
    for (const auto & s3_key : empty_s3_keys)
    {
        auto s3_fname = ::DB::S3::S3FilenameView::fromKey(s3_key);
        auto file_seg = file_cache.get(s3_fname);
        ASSERT_NE(file_seg, nullptr) << s3_key;
        ASSERT_TRUE(file_seg->isReadyToRead());
        ASSERT_EQ(file_seg->getSize(), 0);
    }
    ASSERT_EQ(file_cache.cache_used, 0);

    // Make cache full
    for (const auto & obj : objects)
    {
        auto s3_fname = ::DB::S3::S3FilenameView::fromKey(obj.key);
        ASSERT_TRUE(s3_fname.isDataFile()) << obj.key;
        ASSERT_EQ(file_cache.get(s3_fname), nullptr) << obj.key;
    }
    waitForBgDownload(file_cache);
    ASSERT_EQ(file_cache.bg_download_fail_count.load(std::memory_order_relaxed), 0);
    ASSERT_EQ(file_cache.bg_download_succ_count.load(std::memory_order_relaxed), objects.size() + empty_s3_keys.size());
    ASSERT_EQ(file_cache.cache_used, file_cache.cache_capacity);
    for (const auto & obj : objects)
    {
        auto s3_fname = ::DB::S3::S3FilenameView::fromKey(obj.key);
        ASSERT_TRUE(s3_fname.isDataFile()) << obj.key;
        auto file_seg = file_cache.get(s3_fname);
        ASSERT_NE(file_seg, nullptr) << obj.key;
        ASSERT_TRUE(file_seg->isReadyToRead());
        ASSERT_EQ(file_seg->getSize(), obj.size);
    }
    ASSERT_EQ(file_cache.cache_used, file_cache.cache_capacity);

    // Evict empty files
    auto objects2 = genObjects(/*store_count*/ 1, /*table_count*/ 1, /*file_count*/ 1, {"meta"});
    for (const auto & obj : objects2)
    {
        auto s3_fname = ::DB::S3::S3FilenameView::fromKey(obj.key);
        ASSERT_TRUE(s3_fname.isDataFile()) << obj.key;
        ASSERT_EQ(file_cache.get(s3_fname), nullptr) << obj.key;
    }
    waitForBgDownload(file_cache);

    // Evict empty files succ
    for (const auto & s3_key : empty_s3_keys)
    {
        auto s3_fname = ::DB::S3::S3FilenameView::fromKey(s3_key);
        ASSERT_EQ(file_cache.get(s3_fname), nullptr) << s3_key;
    }
    waitForBgDownload(file_cache);
}
CATCH

TEST_F(FileCacheTest, ManualDropCachedFiles)
try
{
    Stopwatch sw;
    auto objects = genObjects(/*store_count*/ 1, /*table_count*/ 1, /*file_count*/ 5, {"meta"});
    auto total_size = objectsTotalSize(objects);
    LOG_DEBUG(log, "genObjects: count={} total_size={} cost={}s", objects.size(), total_size, sw.elapsedSeconds());
    auto cache_dir = fmt::format("{}/evict_empty_file", tmp_dir);
    StorageRemoteCacheConfig cache_config{.dir = cache_dir, .dtfile_level = 100};
    calculateCacheCapacity(cache_config, total_size);
    LOG_DEBUG(log, "total_size={} dt_cache_capacity={}", total_size, cache_config.getDTFileCapacity());
    FileCache file_cache(capacity_metrics, cache_config);

    // Make cache full
    for (const auto & obj : objects)
    {
        auto s3_fname = ::DB::S3::S3FilenameView::fromKey(obj.key);
        ASSERT_TRUE(s3_fname.isDataFile()) << obj.key;
        ASSERT_EQ(file_cache.get(s3_fname), nullptr) << obj.key;
    }
    waitForBgDownload(file_cache);
    ASSERT_EQ(file_cache.bg_download_fail_count.load(std::memory_order_relaxed), 0);
    ASSERT_EQ(file_cache.bg_download_succ_count.load(std::memory_order_relaxed), objects.size());
    ASSERT_EQ(file_cache.cache_used, file_cache.cache_capacity);

    // Drop cache manually
    ASSERT_TRUE(std::filesystem::exists(cache_config.getDTFileCacheDir()));
    std::filesystem::remove_all(cache_config.getDTFileCacheDir());
    ASSERT_FALSE(std::filesystem::exists(cache_config.getDTFileCacheDir()));
    ASSERT_EQ(file_cache.cache_used, file_cache.cache_capacity);

    // Remove droped-files
    Settings settings;
    settings.set("dt_filecache_max_downloading_count_scale", "0.0"); // Disable download file from S3
    file_cache.updateConfig(settings);
    ASSERT_DOUBLE_EQ(file_cache.max_downloading_count_scale, 0.0);
    UInt64 released_size = 0;
    for (const auto & obj : objects)
    {
        auto s3_fname = ::DB::S3::S3FilenameView::fromKey(obj.key);
        auto file_seg = file_cache.get(s3_fname);
        ASSERT_NE(file_seg, nullptr) << obj.key;
        try
        {
            auto file = file_cache.getRandomAccessFile(S3FilenameView::fromKey(obj.key), obj.size);
            FAIL();
        }
        catch (DB::Exception & e)
        {
            ASSERT_EQ(e.code(), ErrorCodes::FILE_DOESNT_EXIST);
        }
        released_size += file_seg->getSize();
        ASSERT_EQ(file_cache.cache_used + released_size, file_cache.cache_capacity);

        ASSERT_EQ(file_cache.get(s3_fname), nullptr); // Has been removed.
    }
    ASSERT_EQ(file_cache.cache_used, 0);
    ASSERT_EQ(released_size, file_cache.cache_capacity);
}
CATCH

TEST_F(FileCacheTest, ForceEvict)
try
{
    // Generate multiple files for each different file-types.
    struct ObjDesc
    {
        String name;
        size_t size;
    };
    const std::vector<ObjDesc> objects = {
        {.name = "1.meta", .size = 10},
        {.name = "1.idx", .size = 1},
        {.name = "2.idx", .size = 2},
        {.name = "1.mrk", .size = 3},
        {.name = "2.meta", .size = 5},
        {.name = "3.meta", .size = 20},
        {.name = "2.mrk", .size = 10},
        {.name = "4.meta", .size = 3},
        {.name = "4.idx", .size = 10},
        {.name = "4.mrk", .size = 7},
        {.name = "3.mrk", .size = 1},
        {.name = "3.idx", .size = 5},
    };

    const auto s3_dir = S3Filename::fromTableID(0, 0, 1);
    for (const auto & obj : objects)
        writeS3FileWithSize(s3_dir, obj.name, obj.size);

    // Create a large enough cache
    auto cache_dir = fmt::format("{}/force_evict_1", tmp_dir);
    auto cache_config = StorageRemoteCacheConfig{
        .dir = cache_dir,
        .capacity = 100,
        .dtfile_level = 100,
        .delta_rate = 0,
        .reserved_rate = 0,
    };
    FileCache file_cache(capacity_metrics, cache_config);

    ASSERT_EQ(file_cache.getAll().size(), 0);

    // Put everything in cache
    for (const auto & obj : objects)
    {
        auto full_path = fmt::format("{}/{}", s3_dir.toFullKey(), obj.name);
        auto s3_fname = S3FilenameView::fromKey(full_path);
        auto guard = file_cache.downloadFileForLocalRead(s3_fname, obj.size);
        ASSERT_NE(guard, nullptr);
    }

    ASSERT_EQ(file_cache.getAll().size(), 12);

    // Ensure the LRU order is correct.
    for (const auto & obj : objects)
    {
        auto full_path = fmt::format("{}/{}", s3_dir.toFullKey(), obj.name);
        auto s3_fname = S3FilenameView::fromKey(full_path);
        ASSERT_TRUE(file_cache.getOrWait(s3_fname, obj.size));
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); // Avoid possible same lastAccessTime.
    }

    ASSERT_EQ(file_cache.getAll().size(), 12);

    auto cache_not_contains = [&](const String & file) {
        const auto all = file_cache.getAll();
        for (const auto & file_seg : all)
            if (file_seg->getLocalFileName().contains(file))
                return false;
        return true;
    };
    ASSERT_FALSE(cache_not_contains("1.meta"));

    // Now, we want space=5, should evict:
    // {.name = "1.meta", .size = 10},
    auto evicted = file_cache.forceEvict(5);
    ASSERT_EQ(evicted, 10);

    ASSERT_EQ(file_cache.getAll().size(), 11);
    ASSERT_TRUE(cache_not_contains("1.meta"));

    // Evict 5 space again, should evict:
    // {.name = "1.idx", .size = 1},
    // {.name = "2.idx", .size = 2},
    // {.name = "1.mrk", .size = 3},
    evicted = file_cache.forceEvict(5);
    ASSERT_EQ(evicted, 6);

    ASSERT_EQ(file_cache.getAll().size(), 8);
    ASSERT_TRUE(cache_not_contains("1.idx"));
    ASSERT_TRUE(cache_not_contains("2.idx"));
    ASSERT_TRUE(cache_not_contains("1.mrk"));

    // Evict 0
    evicted = file_cache.forceEvict(0);
    ASSERT_EQ(evicted, 0);

    ASSERT_EQ(file_cache.getAll().size(), 8);

    // Evict 1, should evict:
    // {.name = "2.meta", .size = 5},
    evicted = file_cache.forceEvict(1);
    ASSERT_EQ(evicted, 5);

    ASSERT_EQ(file_cache.getAll().size(), 7);
    ASSERT_TRUE(cache_not_contains("2.meta"));

    // Use get(), it should not evict anything.
    {
        auto full_path = fmt::format("{}/not_exist", s3_dir.toFullKey());
        ASSERT_FALSE(file_cache.get(S3FilenameView::fromKey(full_path), 999));
        ASSERT_EQ(file_cache.getAll().size(), 7);
    }

    // Use getOrWait(), it should force evict everything and then fail.
    {
        auto full_path = fmt::format("{}/not_exist", s3_dir.toFullKey());
        try
        {
            file_cache.getOrWait(S3FilenameView::fromKey(full_path), 999);
            FAIL();
        }
        catch (Exception & e)
        {
            ASSERT_TRUE(e.message().contains("Cannot reserve 999 space for object"));
        }
        ASSERT_EQ(file_cache.getAll().size(), 0);
    }
}
CATCH


} // namespace DB::tests::S3
