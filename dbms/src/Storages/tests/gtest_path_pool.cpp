#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>
#include <test_utils/TiflashTestBasic.h>

namespace DB
{
namespace tests
{

class PathPool_test : public ::testing::Test
{
public:
    PathPool_test() = default;

    static void SetUpTestCase() { DB::tests::TiFlashTestEnv::setupLogger(); }

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
};

TEST_F(PathPool_test, AlignPaths)
try
{
    Strings paths = getMultiTestPaths();
    auto & ctx = TiFlashTestEnv::getContext();

    PathPool pool(paths, paths, ctx.getPathCapacity(), ctx.getFileProvider());
    auto spool = pool.withTable("test", "t", false);

    // Stable delegate
    {
        auto delegate = spool.getStableDelegate();
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
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            delegate.removeDTFile(i);
        }
    }
    // Delta delegate
    {
        auto delegate = spool.getDeltaDelegate();
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], paths[i] + DIR_PREFIX_OF_TABLE + StoragePathPool::DELTA_FOLDER_NAME);
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
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written);
        }
    }
    // Normal delegate
    {
        auto delegate = spool.getNormalDelegate("meta");
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
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written);
        }
    }
}
CATCH

TEST_F(PathPool_test, UnalignPaths)
try
{
    Strings paths = getMultiTestPaths();
    Strings latest_paths(paths.begin(), paths.begin() + 1);
    auto & ctx = TiFlashTestEnv::getContext();

    PathPool pool(paths, latest_paths, ctx.getPathCapacity(), ctx.getFileProvider());
    auto spool = pool.withTable("test", "t", false);
    // Stable delegate
    {
        auto delegate = spool.getStableDelegate();
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
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            delegate.removeDTFile(i);
        }
    }
    // Delta delegate
    {
        auto delegate = spool.getDeltaDelegate();
        auto res = delegate->listPaths();
        EXPECT_EQ(res.size(), latest_paths.size());
        for (size_t i = 0; i < res.size(); ++i)
        {
            EXPECT_EQ(res[i], latest_paths[i] + DIR_PREFIX_OF_TABLE + StoragePathPool::DELTA_FOLDER_NAME);
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
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written);
        }
    }
    // Normal delegate
    {
        auto delegate = spool.getNormalDelegate("meta");
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
        for (size_t i = 0; i < TEST_NUMBER_FOR_CHOOSE; ++i)
        {
            PageFileIdAndLevel id{i, 0};
            delegate->removePageFile(id, bytes_written);
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
