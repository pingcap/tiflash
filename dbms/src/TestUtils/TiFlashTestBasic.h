#pragma once

#include <Common/UnifiedLogPatternFormatter.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Path.h>
#include <Poco/PatternFormatter.h>
#include <Poco/SortedDirectoryIterator.h>

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#else
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
#endif

#include <gtest/gtest.h>

#if !__clang__
#pragma GCC diagnostic pop
#else
#pragma clang diagnostic pop
#endif


namespace DB
{
namespace tests
{

#define CATCH                                                                                      \
    catch (const Exception & e)                                                                    \
    {                                                                                              \
        std::string text = e.displayText();                                                        \
                                                                                                   \
        auto embedded_stack_trace_pos = text.find("Stack trace");                                  \
        std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;               \
        if (std::string::npos == embedded_stack_trace_pos)                                         \
            std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString() << std::endl; \
                                                                                                   \
        throw;                                                                                     \
    }

/// helper functions for comparing DataType
::testing::AssertionResult DataTypeCompare( //
    const char * lhs_expr,
    const char * rhs_expr,
    const DataTypePtr & lhs,
    const DataTypePtr & rhs);
#define ASSERT_DATATYPE_EQ(val1, val2) ASSERT_PRED_FORMAT2(::DB::tests::DataTypeCompare, val1, val2)
#define EXPECT_DATATYPE_EQ(val1, val2) EXPECT_PRED_FORMAT2(::DB::tests::DataTypeCompare, val1, val2)

// A simple helper for getting DataType from type name
inline DataTypePtr typeFromString(const String & str)
{
    auto & data_type_factory = DataTypeFactory::instance();
    return data_type_factory.get(str);
}

inline DataTypes typesFromString(const String & str)
{
    DataTypes data_types;
    std::istringstream data_types_stream(str);
    std::string data_type;
    while (data_types_stream >> data_type)
        data_types.push_back(typeFromString(data_type));

    return data_types;
}

class TiFlashTestEnv
{
public:
    static String getTemporaryPath() { return Poco::Path("./tmp/").absolute().toString(); }

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

    static void setupLogger(const String & level = "trace")
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
        Poco::AutoPtr<UnifiedLogPatternFormatter> formatter(new UnifiedLogPatternFormatter());
        formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i [%I] <%p> %s: %t");
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Logger::root().setChannel(formatting_channel);
        Logger::root().setLevel(level);
    }

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

    static void initializeGlobalContext();
    static Context & getGlobalContext() { return *global_context; }
    static void shutdown();

private:
    static std::unique_ptr<Context> global_context;

private:
    TiFlashTestEnv() = delete;
};

#define CHECK_TESTS_WITH_DATA_ENABLED                                                                        \
    if (!TiFlashTestEnv::isTestsWithDataEnabled())                                                           \
    {                                                                                                        \
        LOG_INFO(&Logger::get("GTEST"),                                                                      \
            "Test: " << ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name() << "."     \
                     << ::testing::UnitTest::GetInstance()->current_test_info()->name() << " is disabled."); \
        return;                                                                                              \
    }

} // namespace tests
} // namespace DB
