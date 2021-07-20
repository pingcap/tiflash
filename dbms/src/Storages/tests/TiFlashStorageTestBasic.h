#pragma once
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace base
{
using namespace DB::tests;

class TiFlashStorageTestBasic : public ::testing::Test
{
public:
    static std::string getCurrentFullTestName()
    {
        std::string buffer;
        if (!testing::UnitTest::GetInstance())
        {
            throw "not in GTEST context scope.";
        }

        if (const auto * info = testing::UnitTest::GetInstance()->current_test_info())
        {
            if (info->test_case_name())
                buffer += info->test_case_name();
            if (info->name())
            {
                if (!buffer.empty())
                    buffer += '.';
                buffer += info->name();
            }
            if (info->type_param())
            {
                if (!buffer.empty())
                    buffer += '.';
                buffer += info->type_param();
            }
        }
        else
        {
            throw "Can not get current test info";
        }
        return buffer;
    }

    static String getTemporaryPath()
    {
        /**
         * Sometimes we need to generate some data for testing and move them to "tests/testdata". And run test with those files by
         * TiFlashTestEnv::findTestDataPath. We may need to check the files on "./tmp/xxx" if some storage test failed.
         * So instead of dropping data after cases run, we drop data before running each test case.
         */
        return TiFlashTestEnv::getTemporaryPath(getCurrentFullTestName().c_str());
    }

protected:
    void dropDataOnDisk(String path)
    {
        if (Poco::File file(path); file.exists())
        {
            file.remove(true);
        }
    }

    void SetUp() override
    {
        dropDataOnDisk(getTemporaryPath());
        reload();
    }

    void reload(DB::Settings && db_settings = DB::Settings())
    {
        Strings test_paths;
        test_paths.push_back(getTemporaryPath());
        db_context = std::make_unique<Context>(TiFlashTestEnv::getContext(db_settings, test_paths));
    }

protected:
    std::unique_ptr<Context> db_context;
};
} // namespace base
} // namespace DB