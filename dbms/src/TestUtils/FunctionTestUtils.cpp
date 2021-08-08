#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/core.h>

namespace DB
{
namespace tests
{

#define ASSERT_EQUAL_1(expected_value, actual_value) \
    do {\
        auto expected_v = (expected_value);\
        auto actual_v = (actual_value);\
        if (expected_v != actual_v)\
        {\
            auto expected_str = fmt::format("\n{}: {}", #expected_value, expected_v);\
            auto actual_str = fmt::format("\n{}: {}", #actual_value, actual_v);\
            return ::testing::AssertionFailure() << expected_str << actual_str;\
        }\
    } while (false)

#define ASSERT_EQUAL_2(expected_value, actual_value, title) \
    do {\
        auto expected_v = (expected_value);\
        auto actual_v = (actual_value);\
        if (expected_v != actual_v)\
        {\
            auto expected_str = fmt::format("\n{}: {}", #expected_value, expected_v);\
            auto actual_str = fmt::format("\n{}: {}", #actual_value, actual_v);\
            return ::testing::AssertionFailure() << (title) << expected_str << actual_str;\
        }\
    } while (false)

#define ASSERT_EQUAL_3(expected_value, actual_value, expected_display, actual_display) \
    do {\
        auto expected_v = (expected_value);\
        auto actual_v = (actual_value);\
        if (expected_v != actual_v)\
        {\
            auto expected_str = fmt::format("\n{}: {}", #expected_value, (expected_display));\
            auto actual_str = fmt::format("\n{}: {}", #actual_value, (actual_display));\
            return ::testing::AssertionFailure() << expected_str << actual_str;\
        }\
    } while (false)

#define ASSERT_EQUAL_4(expected_value, actual_value, title, expected_display, actual_display) \
    do {\
        auto expected_v = (expected_value);\
        auto actual_v = (actual_value);\
        if (expected_v != actual_v)\
        {\
            auto expected_str = fmt::format("\n{}: {}", #expected_value, (expected_display));\
            auto actual_str = fmt::format("\n{}: {}", #actual_value, (actual_display));\
            return ::testing::AssertionFailure() << (title) << expected_str << actual_str;\
        }\
    } while (false)

::testing::AssertionResult dataTypeEqual(
    const DataTypePtr & expected,
    const DataTypePtr & actual)
{
    ASSERT_EQUAL_2(expected->getName(), actual->getName(), "DataType name mismatch");
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult columnEqual(
    const ColumnPtr & expected,
    const ColumnPtr & actual)
{
    ASSERT_EQUAL_2(expected->getName(), actual->getName(), "Column name mismatch");
    ASSERT_EQUAL_2(expected->size(), actual->size(), "Column size mismatch");

    for (size_t i = 0, size = expected->size(); i < size; ++i)
    {
        auto expected_field = (*expected)[i];
        auto actual_field = (*actual)[i];

        ASSERT_EQUAL_4(expected_field, actual_field, fmt::format("Value {} mismatch", i), expected_field.toString(), actual_field.toString());
    }
    return ::testing::AssertionSuccess();
}

::testing::AssertionResult columnEqual(
    const ColumnWithTypeAndName & expected,
    const ColumnWithTypeAndName & actual)
{
    auto ret = dataTypeEqual(expected.type, actual.type);
    if (!ret)
        return ret;

    return columnEqual(expected.column, actual.column);
}

} // namespace tests
} // namespace DB

