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

#include <IO/FileProvider/FileProvider_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Environment.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/SortedDirectoryIterator.h>
#include <Storages/Page/PageConstants.h>
#include <Storages/PathPool_fwd.h>
#include <TestUtils/TiFlashTestException.h>
#include <fmt/core.h>


namespace DB
{
struct Settings;
class DAGContext;
class MockStorage;
namespace S3
{
class TiFlashS3Client;
}
} // namespace DB

namespace DB::tests
{
enum class TestType
{
    EXECUTOR_TEST,
    INTERPRETER_TEST,
};
class TiFlashTestEnv
{
public:
    static constexpr uint64_t DEFAULT_BG_POOL_SIZE = 2;
    static String getTemporaryPath(std::string_view test_case = "", bool get_abs = true);

    static void tryCreatePath(const std::string & path);

    static void tryRemovePath(const std::string & path, bool recreate = false);

    static std::pair<Strings, Strings> getPathPool(const Strings & testdata_path = {})
    {
        Strings result;
        if (!testdata_path.empty())
            for (const auto & p : testdata_path)
                result.push_back(Poco::Path{p}.absolute().toString());
        else
            result.push_back(Poco::Path{getTemporaryPath()}.absolute().toString());
        return std::make_pair(result, result);
    }

    static void setupLogger(const String & level = "trace", std::ostream & os = std::cerr, bool enable_colors = true);
    static void setUpTestContext(
        Context & context,
        DAGContext * dag_context,
        MockStorage * mock_storage,
        const TestType & test_type);

    static std::unique_ptr<PathPool> createCleanPathPool(const String & path);

    // If you want to run these tests, you should set this envrionment variablle
    // For example:
    //     ALSO_RUN_WITH_TEST_DATA=1 ./dbms/gtests_dbms --gtest_filter='IDAsPath*'
    static bool isTestsWithDataEnabled() { return (Poco::Environment::get("ALSO_RUN_WITH_TEST_DATA", "0") == "1"); }

    static Strings findTestDataPath(const String & name)
    {
        const static std::vector<String> SEARCH_PATH = {"../tests/testdata/", "/tests/testdata/"};
        for (const auto & prefix : SEARCH_PATH)
        {
            String path = Poco::Path{prefix + name}.absolute().toString();
            if (auto f = Poco::File(path); f.exists() && f.isDirectory())
            {
                Strings paths;
                Poco::SortedDirectoryIterator dir_end;
                for (Poco::SortedDirectoryIterator dir_it(f); dir_it != dir_end; ++dir_it)
                    paths.emplace_back(path + "/" + dir_it.name() + "/");
                return paths;
            }
        }
        throw Exception("Can not find testdata with name[" + name + "]");
    }

    static ContextPtr getContext();

    /// Returns a fresh ContextPtr.
    static ContextPtr getContext(const DB::Settings & settings, Strings testdata_path = {});

    static FileProviderPtr getDefaultFileProvider();

    static void initializeGlobalContext(
        Strings testdata_path = {},
        PageStorageRunMode ps_run_mode = PageStorageRunMode::ONLY_V3,
        uint64_t bg_thread_count = DEFAULT_BG_POOL_SIZE);
    static void addGlobalContext(
        const DB::Settings & settings,
        Strings testdata_path = {},
        PageStorageRunMode ps_run_mode = PageStorageRunMode::ONLY_V3,
        uint64_t bg_thread_count = DEFAULT_BG_POOL_SIZE);
    static Context & getGlobalContext() { return *global_contexts[0]; }
    static Context & getGlobalContext(int idx) { return *global_contexts[idx]; }
    static int globalContextSize() { return global_contexts.size(); }
    static void shutdown();

    static FileProviderPtr getMockFileProvider();

    static void setIsMockedS3Client(bool mock) { is_mocked_s3_client = mock; }
    static bool isMockedS3Client() { return is_mocked_s3_client; }
    static bool createBucketIfNotExist(::DB::S3::TiFlashS3Client & s3_client);
    static void deleteBucket(::DB::S3::TiFlashS3Client & s3_client);

    static void disableS3Config();
    static void enableS3Config();

    TiFlashTestEnv() = delete; // no instance allow

private:
    static std::vector<ContextPtr> global_contexts;
    static bool is_mocked_s3_client;
};
} // namespace DB::tests
