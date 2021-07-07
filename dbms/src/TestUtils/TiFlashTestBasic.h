#pragma once

#include <Common/UnifiedLogPatternFormatter.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Path.h>
#include <Poco/PatternFormatter.h>
#include <Poco/SortedDirectoryIterator.h>
#include <TestUtils/TiFlashTestException.h>
#include <fmt/core.h>

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic ignored "-Wdeprecated-copy"
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
    catch (const DB::tests::TiFlashTestException & e)                                              \
    {                                                                                              \
        std::string text = e.displayText();                                                        \
                                                                                                   \
        text += "\n\n";                                                                            \
        if (text.find("Stack trace") == std::string::npos)                                         \
            text += fmt::format("Stack trace:\n{}\n", e.getStackTrace().toString());               \
                                                                                                   \
        FAIL() << text;                                                                            \
    }                                                                                              \
    catch (const DB::Exception & e)                                                                \
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
inline ::testing::AssertionResult DataTypeCompare( //
    const char * lhs_expr,
    const char * rhs_expr,
    const DataTypePtr & lhs,
    const DataTypePtr & rhs)
{
    if (lhs->equals(*rhs))
        return ::testing::AssertionSuccess();
    else
        return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs->getName(), rhs->getName(), false);
}
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

template <typename DataType, typename... Args>
DataTypePtr makeDataType(const Args &... args)
{
    if constexpr (IsDecimal<DataType>)
        return std::make_shared<DataTypeDecimal<DataType>>(args...);
    else
        return std::make_shared<DataType>(args...);
}

template <typename DataType, typename... Args>
DataTypePtr makeNullableDataType(const Args &... args)
{
    return makeNullable(makeDataType<DataType, Args...>(args...));
}

template <typename FieldType>
Field makeField(const std::optional<FieldType> & value)
{
    if (value.has_value())
        return Field(static_cast<FieldType>(value.value()));
    else
        return Null();
}

template <typename T>
using DataVector = std::vector<std::optional<T>>;

using DataVectorUInt64 = DataVector<UInt64>;
using DataVectorInt64 = DataVector<Int64>;
using DataVectorFloat64 = DataVector<Float64>;
using DataVectorString = DataVector<String>;
using DataVectorDecimal32 = DataVector<Decimal32>;
using DataVectorDecimal64 = DataVector<Decimal64>;
using DataVectorDecimal128 = DataVector<Decimal128>;
using DataVectorDecimal256 = DataVector<Decimal256>;

/// if data_type is nullable, FieldType can be either T or std::optional<T>.
template <typename FieldType>
ColumnPtr makeColumn(size_t size, const DataTypePtr & data_type, const DataVector<FieldType> & column_data)
{
    auto makeAndCheckField = [&](const std::optional<FieldType> & value)
    {
        auto f = makeField(value);
        if (f.isNull() && !data_type->isNullable())
            throw TiFlashTestException("Try to insert NULL into a non-nullable column");
        return f;
    };

    if (size != column_data.size() && (size < column_data.size() || column_data.size() != 1))
        throw TiFlashTestException(fmt::format("Mismatch between column size ({}) and data size ({}) ", size, column_data.size()));

    if (column_data.size() == 1)
    {
        return data_type->createColumnConst(size, makeAndCheckField(column_data[0]));
    }
    else
    {
        auto column = data_type->createColumn();

        for (const auto & data : column_data)
            column->insert(makeAndCheckField(data));

        return column;
    }
}

template <typename FieldType>
ColumnWithTypeAndName makeColumnWithTypeAndName(
    const String & name,
    size_t size,
    const DataTypePtr & data_type,
    const DataVector<FieldType> & column_data)
{
    auto column = makeColumn(size, data_type, column_data);
    return ColumnWithTypeAndName(std::move(column), data_type, name);
}

ColumnWithTypeAndName executeFunction(const String & func_name, const ColumnsWithTypeAndName & columns);

// e.g. data_type = DataTypeUInt64, FieldType = UInt64.
// if data vector contains only 1 element, a const column will be created.
// otherwise, two columns are expected to be of the same size.
// use std::nullopt for null values.
template <typename FieldType1, typename FieldType2>
ColumnWithTypeAndName executeFunction(
    const String & function_name,
    const DataTypePtr & data_type_1,
    const DataTypePtr & data_type_2,
    const DataVector<FieldType1> & column_data_1,
    const DataVector<FieldType2> & column_data_2,
    size_t column_size = std::numeric_limits<size_t>::max())
{
    if (column_size == std::numeric_limits<size_t>::max())
        column_size = std::max(column_data_1.size(), column_data_2.size());

    auto input_1 = makeColumnWithTypeAndName("input_1", column_size, data_type_1, column_data_1);
    auto input_2 = makeColumnWithTypeAndName("input_2", column_size, data_type_2, column_data_2);

    return executeFunction(function_name, {input_1, input_2});
}

// e.g. data_type = DataTypeUInt64, FieldType = UInt64.
// if data vector contains only 1 element, a const column will be created.
// otherwise, two columns are expected to be of the same size.
// use std::nullopt for null values.
template <typename FieldType>
ColumnWithTypeAndName executeFunction(
    const String & function_name,
    const DataTypePtr & data_type,
    const DataVector<FieldType> & column_data,
    size_t column_size = std::numeric_limits<size_t>::max())
{
    if (column_size == std::numeric_limits<size_t>::max())
        column_size = column_data.size();

    auto input = makeColumnWithTypeAndName("input", column_size, data_type, column_data);

    return executeFunction(function_name, {input});
}

/// TiFlashTestBase provides utilities for writing a TiFlash gtest.
class TiFlashTestBase : public ::testing::Test
{
protected:

    void assertDataTypeEqual(const DataTypePtr & actual, const DataTypePtr & expect);

    void assertColumnEqual(const ColumnPtr & actual, const ColumnPtr & expect);

    /// ignore column name
    void assertColumnEqual(const ColumnWithTypeAndName & actual, const ColumnWithTypeAndName & expect);

    // e.g. data_type = DataTypeUInt64, FieldType = UInt64.
    // if data vector contains only 1 element, a const column will be created.
    // otherwise, two columns are expected to be of the same size.
    // use std::nullopt for null values.
    template <typename FieldType1, typename FieldType2, typename ResultFieldType>
    void executeBinaryFunctionAndCheck(
        const String & function_name,
        const DataTypePtr & data_type_1,
        const DataTypePtr & data_type_2,
        const DataTypePtr & expected_data_type,
        const DataVector<FieldType1> & column_data_1,
        const DataVector<FieldType2> & column_data_2,
        const DataVector<ResultFieldType> & expected_data)
    {
        auto result = executeFunction(function_name, data_type_1, data_type_2, column_data_1, column_data_2);
        auto expect = makeColumnWithTypeAndName("ignore", expected_data.size(), expected_data_type, expected_data);

        assertColumnEqual(result, expect);
    }

    template <typename FieldType, typename ResultFieldType>
    void executeUnaryFunctionAndCheck(
        const String & function_name,
        const DataTypePtr & data_type,
        const DataTypePtr & expected_data_type,
        const DataVector<FieldType> & column_data,
        const DataVector<ResultFieldType> & expected_data)
    {
        auto result = executeFunction(function_name, data_type, column_data);
        auto expect = makeColumnWithTypeAndName("ignore", expected_data.size(), expected_data_type, expected_data);

        assertColumnEqual(result, expect);
    }
};


class TiFlashTestEnv
{
public:
    static String getTemporaryPath(const char * test_case = nullptr)
    {
        String path = "./tmp/";
        if (test_case)
            path += std::string(test_case);

        return Poco::Path(path).absolute().toString();
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

#define EXECUTE_BINARY_FUNCTION_AND_CHECK(function_name, ...) \
    do {\
        auto fn = (function_name);\
        auto desc = fmt::format("Execute binary function {}", fn);\
        SCOPED_TRACE(desc.c_str());\
        executeBinaryFunctionAndCheck(function_name, ##__VA_ARGS__);\
    } while (false)

#define EXECUTE_UNARY_FUNCTION_AND_CHECK(function_name, ...) \
    do {\
        auto fn = (function_name);\
        auto desc = fmt::format("Execute unary function {}", fn);\
        SCOPED_TRACE(desc.c_str());\
        executeUnaryFunctionAndCheck(function_name, ##__VA_ARGS__);\
    } while (false)

} // namespace tests
} // namespace DB
