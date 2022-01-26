#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <common/logger_useful.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
class PathPool_test : public ::testing::Test
{
public:
    PathPool_test()
        : log(&Poco::Logger::get("PathPool_test"))
    {}

    static void SetUpTestCase() {}

    static constexpr const char * DIR_PREFIX_OF_TABLE = "/data/t/";
    static constexpr size_t TEST_NUMBER_FOR_FOLDER = 6;
    static constexpr size_t TEST_NUMBER_FOR_CHOOSE = 1000;
    static Strings getMultiTestPaths()
    {
        Strings paths;
        for (size_t i = 0; i < TEST_NUMBER_FOR_FOLDER; ++i)
            paths.emplace_back(Poco::Path{TiFlashTestEnv::getTemporaryPath() + "/path_pool_test/data" + toString(i)}.toString());
        return paths;
    }

protected:
    Poco::Logger * log;
};

TEST_F(PathPool_test, AlignPaths)
try
{
    Strings paths = getMultiTestPaths();
    auto ctx = TiFlashTestEnv::getContext();

    PathPool pool(paths, paths, Strings{}, ctx.getPathCapacity(), ctx.getFileProvider());
    auto spool = pool.withTable("test", "t", false);

    // Stable delegate
    {
        auto delegate = spool.getStableDiskDelegator();
        auto res = delegate.listPaths();
        EXPECT_EQ(res.size(), paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + DIR_PREFIX_OF_TABLE + StoragePathPool::STABLE_FOLDER_NAME);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            auto chosen = delegate.choosePath();
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate.addDTFile(i, 200, chosen);
            auto path_get = delegate.getDTFilePath(i);
            ASSERT_EQ(path_get, chosen);
        }

        for (size_t i = 0; i < res.size(); ++i)
        {
            auto stat = ctx.getPathCapacity()->getFsStatsOfPath(res[i]);
            LOG_INFO(log, "[path=" << res[i] << "] [used_size=" << stat.used_size << "]");
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            delegate.removeDTFile(i);
        }
    }
    // PS-multi delegate
    {
        auto delegate = spool.getPSDiskDelegatorMulti("log");
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + DIR_PREFIX_OF_TABLE + "log");
        }
        EXPECT_EQ(delegate->numPaths(), res.size());

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (size_t i = 0; i < res.size(); ++i)
        {
            auto stat = ctx.getPathCapacity()->getFsStatsOfPath(res[i]);
            LOG_INFO(log, "[path=" << res[i] << "] [used_size=" << stat.used_size << "]");
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
    // PS-single delegate
    {
        auto delegate = spool.getPSDiskDelegatorSingle("meta");
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), 1UL);
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + DIR_PREFIX_OF_TABLE + "meta");
        }
        EXPECT_EQ(delegate->numPaths(), 1UL);

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (size_t i = 0; i < res.size(); ++i)
        {
            auto stat = ctx.getPathCapacity()->getFsStatsOfPath(res[i]);
            LOG_INFO(log, "[path=" << res[i] << "] [used_size=" << stat.used_size << "]");
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
    // PS-Raft delegate
    {
        auto delegate = pool.getPSDiskDelegatorRaft();
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + "/kvstore");
        }
        EXPECT_EQ(delegate->numPaths(), res.size());

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (size_t i = 0; i < res.size(); ++i)
        {
            auto stat = ctx.getPathCapacity()->getFsStatsOfPath(res[i]);
            LOG_INFO(log, "[path=" << res[i] << "] [used_size=" << stat.used_size << "]");
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
}
CATCH

TEST_F(PathPool_test, UnalignPaths)
try
{
    Strings paths = getMultiTestPaths();
    Strings latest_paths(paths.begin(), paths.begin() + 1);
    auto ctx = TiFlashTestEnv::getContext();

    PathPool pool(paths, latest_paths, Strings{}, ctx.getPathCapacity(), ctx.getFileProvider());
    auto spool = pool.withTable("test", "t", false);
    // Stable delegate
    {
        auto delegate = spool.getStableDiskDelegator();
        auto res = delegate.listPaths();
        EXPECT_EQ(res.size(), paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + DIR_PREFIX_OF_TABLE + StoragePathPool::STABLE_FOLDER_NAME);
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            auto chosen = delegate.choosePath();
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate.addDTFile(i, 200, chosen);
            auto path_get = delegate.getDTFilePath(i);
            ASSERT_EQ(path_get, chosen);
        }

        for (size_t i = 0; i < res.size(); ++i)
        {
            auto stat = ctx.getPathCapacity()->getFsStatsOfPath(res[i]);
            LOG_INFO(log, "[path=" << res[i] << "] [used_size=" << stat.used_size << "]");
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            delegate.removeDTFile(i);
        }
    }
    // PS-multi delegate
    {
        auto delegate = spool.getPSDiskDelegatorMulti("log");
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), latest_paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], latest_paths[i] + DIR_PREFIX_OF_TABLE + "log");
        }
        EXPECT_EQ(delegate->numPaths(), res.size());

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (size_t i = 0; i < res.size(); ++i)
        {
            auto stat = ctx.getPathCapacity()->getFsStatsOfPath(res[i]);
            LOG_INFO(log, "[path=" << res[i] << "] [used_size=" << stat.used_size << "]");
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
    // PS-single delegate
    {
        auto delegate = spool.getPSDiskDelegatorSingle("meta");
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), 1UL);
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + DIR_PREFIX_OF_TABLE + "meta");
        }
        EXPECT_EQ(delegate->numPaths(), 1UL);

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (size_t i = 0; i < res.size(); ++i)
        {
            auto stat = ctx.getPathCapacity()->getFsStatsOfPath(res[i]);
            LOG_INFO(log, "[path=" << res[i] << "] [used_size=" << stat.used_size << "]");
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
    // PS-Raft delegate
    {
        auto delegate = pool.getPSDiskDelegatorRaft();
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), latest_paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], latest_paths[i] + "/kvstore");
        }
        EXPECT_EQ(delegate->numPaths(), res.size());

        size_t bytes_written = 200;
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            auto chosen = delegate->choosePath(id);
            ASSERT_NE(std::find(res.begin(), res.end(), chosen), res.end());
            delegate->addPageFileUsedSize(id, bytes_written, chosen, true);
            auto path_get = delegate->getPageFilePath(id);
            ASSERT_EQ(path_get, chosen);
        }

        for (size_t i = 0; i < res.size(); ++i)
        {
            auto stat = ctx.getPathCapacity()->getFsStatsOfPath(res[i]);
            LOG_INFO(log, "[path=" << res[i] << "] [used_size=" << stat.used_size << "]");
        }

        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written, false, false);
        }
    }
}
CATCH

static void createIfNotExist(const String & path)
{
    if (Poco::File file(path); !file.exists())
        file.createDirectories();
}

TEST(PathCapcatity, FsStats)
try
{
    std::string main_data_path = TiFlashTestEnv::getTemporaryPath() + "/main";
    createIfNotExist(main_data_path);

    std::string latest_data_path = TiFlashTestEnv::getTemporaryPath() + "/lastest";
    createIfNotExist(latest_data_path);
    
    size_t global_capacity_quota = 10;
    size_t capacity = 100;
    {
        PathCapacityMetrics path_capacity(global_capacity_quota, {main_data_path}, {capacity}, {latest_data_path}, {capacity});

        FsStats fs_stats = path_capacity.getFsStats();
        EXPECT_EQ(fs_stats.capacity_size, 2 * capacity); // summing the capacity of main and latest path
    }

    {
        PathCapacityMetrics path_capacity(global_capacity_quota, {main_data_path}, {}, {latest_data_path}, {});

        FsStats fs_stats = path_capacity.getFsStats();
        EXPECT_EQ(fs_stats.capacity_size, global_capacity_quota); // Use `global_capacity_quota` when `main_capacity_quota_` is empty
    }
}
CATCH

} // namespace tests
} // namespace DB
