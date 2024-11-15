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

#pragma once

#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace base
{
class TiFlashStorageTestBasic : public ::testing::Test
{
public:
    static std::string getCurrentFullTestName()
    {
        std::string buffer;
        if (!testing::UnitTest::GetInstance())
        {
            throw DB::Exception("not in GTEST context scope.", DB::ErrorCodes::LOGICAL_ERROR);
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
            throw DB::Exception("Can not get current test info", DB::ErrorCodes::LOGICAL_ERROR);
        }
        return buffer;
    }

    static std::string getCurrentTestName()
    {
        std::string buffer;
        if (!testing::UnitTest::GetInstance())
        {
            throw DB::Exception("not in GTEST context scope.", DB::ErrorCodes::LOGICAL_ERROR);
        }

        if (const auto * info = testing::UnitTest::GetInstance()->current_test_info())
        {
            buffer = info->name();
        }
        else
        {
            throw DB::Exception("Can not get current test info", DB::ErrorCodes::LOGICAL_ERROR);
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
        return DB::tests::TiFlashTestEnv::getTemporaryPath(getCurrentFullTestName());
    }

protected:
    static void dropDataOnDisk(const String & path)
    {
        if (Poco::File file(path); file.exists())
        {
            file.remove(true);
        }
    }

    static void createIfNotExist(const String & path)
    {
        if (Poco::File file(path); !file.exists())
            file.createDirectories();
    }

    void SetUp() override
    {
        dropDataOnDisk(getTemporaryPath());
        reload();
    }

    void reload();

    void reload(const DB::Settings & db_settings)
    {
        Strings test_paths;
        test_paths.push_back(getTemporaryPath());
        db_context = DB::tests::TiFlashTestEnv::getContext(db_settings, test_paths);
    }

protected:
    ContextPtr db_context;
};

} // namespace base
} // namespace DB
