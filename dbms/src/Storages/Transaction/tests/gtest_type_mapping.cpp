#include <DataTypes/DataTypeFactory.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{

namespace
{
DataTypePtr typeFromString(const String & str)
{
    auto & data_type_factory = DataTypeFactory::instance();
    return data_type_factory.get(str);
}
} // namespace


TEST(TypeMapping_test, ColumnInfoToDataType)
{
    // TODO fill this test
}

TEST(TypeMapping_test, DataTypeToColumnInfo)
{
    try
    {
        TiDB::ColumnInfo column_info;
        const Strings numeric_types = {"Int8", "Int16", "Int32", "Int64"};
        for (const auto & numeric_type : numeric_types)
        {
            for (bool sign : {false, true})
            {
                for (bool nullable : {false, true})
                {
                    String actual_test_type = numeric_type;
                    if (!sign)
                        actual_test_type = "U" + actual_test_type;
                    if (nullable)
                        actual_test_type = "Nullable(" + actual_test_type + ")";

                    column_info = getColumnInfoByDataType(typeFromString(actual_test_type));
                    ASSERT_EQ(!sign, column_info.hasUnsignedFlag()) << actual_test_type;
                    ASSERT_EQ(!nullable, column_info.hasNotNullFlag()) << actual_test_type;

                    if (numeric_type == numeric_types[0])
                        ASSERT_EQ(column_info.tp, TiDB::TypeTiny) << actual_test_type;
                    else if (numeric_type == numeric_types[1])
                        ASSERT_EQ(column_info.tp, TiDB::TypeShort) << actual_test_type;
                    else if (numeric_type == numeric_types[2])
                        ASSERT_EQ(column_info.tp, TiDB::TypeLong) << actual_test_type;
                    else if (numeric_type == numeric_types[3])
                        ASSERT_EQ(column_info.tp, TiDB::TypeLonglong) << actual_test_type;
                }
            }
        }

        column_info = getColumnInfoByDataType(typeFromString("String"));
        ASSERT_EQ(column_info.tp, TiDB::TypeString);
    }
    catch (const Exception & e)
    {
        std::string text = e.displayText();

        bool print_stack_trace = true;

        auto embedded_stack_trace_pos = text.find("Stack trace");
        if (std::string::npos != embedded_stack_trace_pos && !print_stack_trace)
            text.resize(embedded_stack_trace_pos);

        std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

        if (print_stack_trace && std::string::npos == embedded_stack_trace_pos)
        {
            std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString();
        }

        throw;
    }
}

} // namespace tests
} // namespace DB
