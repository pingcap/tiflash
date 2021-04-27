#include <DataTypes/DataTypeFactory.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{

TEST(TypeMapping_test, ColumnInfoToDataType)
{
    // TODO fill this test
}

TEST(TypeMapping_test, DataTypeToColumnInfo)
try
{
    String name = "col";
    Field default_field;

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

                column_info = reverseGetColumnInfo(NameAndTypePair{name, typeFromString(actual_test_type)}, 1, default_field, true);
                ASSERT_EQ(!sign, column_info.hasUnsignedFlag()) << actual_test_type;
                ASSERT_EQ(!nullable, column_info.hasNotNullFlag()) << actual_test_type;

                if (numeric_type == numeric_types[0])
                {
                    ASSERT_EQ(column_info.tp, TiDB::TypeTiny) << actual_test_type;
                }
                else if (numeric_type == numeric_types[1])
                {
                    ASSERT_EQ(column_info.tp, TiDB::TypeShort) << actual_test_type;
                }
                else if (numeric_type == numeric_types[2])
                {
                    ASSERT_EQ(column_info.tp, TiDB::TypeLong) << actual_test_type;
                }
                else if (numeric_type == numeric_types[3])
                {
                    ASSERT_EQ(column_info.tp, TiDB::TypeLongLong) << actual_test_type;
                }
            }
        }
    }

    column_info = reverseGetColumnInfo(NameAndTypePair{name, typeFromString("String")}, 1, default_field, true);
    ASSERT_EQ(column_info.tp, TiDB::TypeString);
}
CATCH

} // namespace tests
} // namespace DB
