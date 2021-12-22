#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/SortedDirectoryIterator.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TestUtils/TiFlashTestException.h>
#include <fmt/core.h>

#include <string>

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
#define CATCH                                                                        \
    catch (const DB::tests::TiFlashTestException & e)                                \
    {                                                                                \
        std::string text = e.displayText();                                          \
        text += "\n\n";                                                              \
        if (text.find("Stack trace") == std::string::npos)                           \
            text += fmt::format("Stack trace:\n{}\n", e.getStackTrace().toString()); \
        FAIL() << text;                                                              \
    }                                                                                \
    catch (const DB::Exception & e)                                                  \
    {                                                                                \
        std::string text = e.displayText();                                          \
        fmt::print(stderr, "Code: {}. {}\n\n", e.code(), text);                      \
        auto embedded_stack_trace_pos = text.find("Stack trace");                    \
        if (std::string::npos == embedded_stack_trace_pos)                           \
            fmt::print(stderr, "Stack trace:\n{}\n", e.getStackTrace().toString());  \
        throw;                                                                       \
    }

/// helper functions for comparing DataType
::testing::AssertionResult DataTypeCompare(
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

#define CHECK_TESTS_WITH_DATA_ENABLED                                                     \
    if (!TiFlashTestEnv::isTestsWithDataEnabled())                                        \
    {                                                                                     \
        const auto * test_info = ::testing::UnitTest::GetInstance()->current_test_info(); \
        LOG_INFO(&Poco::Logger::get("GTEST"),                                             \
                 fmt::format(                                                             \
                     "Test: {}.{} is disabled.",                                          \
                     test_info->test_case_name(),                                         \
                     test_info->name()));                                                 \
        return;                                                                           \
    }
} // namespace tests
} // namespace DB
