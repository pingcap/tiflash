#pragma once

#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/SortedDirectoryIterator.h>
#include <TestUtils/TiFlashTestException.h>
#include <fmt/core.h>

namespace DB::tests
{
class TiFlashTestEnv
{
public:
    static String getTemporaryPath(const std::string_view test_case = "")
    {
        String path = "./tmp/";
        if (!test_case.empty())
            path += std::string(test_case);

        return Poco::Path(path).absolute().toString();
    }

    static void tryRemovePath(const std::string & path)
    {
        try
        {
            if (Poco::File p(path); p.exists())
            {
                p.remove(true);
            }
        }
        catch (...)
        {
            tryLogCurrentException("gtest", fmt::format("while removing dir `{}`", path));
        }
    }

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

    static void setupLogger(const String & level = "trace", std::ostream & os = std::cerr);

    // If you want to run these tests, you should set this envrionment variablle
    // For example:
    //     ALSO_RUN_WITH_TEST_DATA=1 ./dbms/gtests_dbms --gtest_filter='IDAsPath*'
    static bool isTestsWithDataEnabled() { return (Poco::Environment::get("ALSO_RUN_WITH_TEST_DATA", "0") == "1"); }

    static Strings findTestDataPath(const String & name)
    {
        const static std::vector<String> SEARCH_PATH = {"../tests/testdata/", "/tests/testdata/"};
        for (auto & prefix : SEARCH_PATH)
        {
            String path = prefix + name;
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

    static Context getContext(const DB::Settings & settings = DB::Settings(), Strings testdata_path = {});

    static void initializeGlobalContext(Strings testdata_path = {});
    static Context & getGlobalContext() { return *global_context; }
    static void shutdown();

private:
    static std::unique_ptr<Context> global_context;

private:
    TiFlashTestEnv() = delete;
};
} // namespace DB::tests