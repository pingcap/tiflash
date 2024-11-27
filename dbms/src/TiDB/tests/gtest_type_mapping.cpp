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

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>

namespace DB
{
namespace tests
{

TEST(TypeMappingTest, DataTypeToColumnInfo)
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

                column_info = reverseGetColumnInfo(
                    NameAndTypePair{name, typeFromString(actual_test_type)},
                    1,
                    default_field,
                    true);
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

                auto data_type = getDataTypeByColumnInfo(column_info);
                ASSERT_EQ(data_type->getName(), actual_test_type);
            }
        }
    }

    {
        auto legacy_str_type = typeFromString(DataTypeString::LegacyName);
        ASSERT_EQ(legacy_str_type->getName(), DataTypeString::LegacyName);
        column_info = reverseGetColumnInfo(NameAndTypePair{name, legacy_str_type}, 1, default_field, true);
        ASSERT_EQ(column_info.tp, TiDB::TypeString);
        auto data_type = getDataTypeByColumnInfo(column_info);
        // Get data type by column_info always returns the default type.
        ASSERT_EQ(data_type->getName(), DataTypeString::getDefaultName());
    }
    {
        auto str_type = typeFromString(DataTypeString::NameV1);
        ASSERT_EQ(str_type->getName(), DataTypeString::NameV1);
        column_info = reverseGetColumnInfo(NameAndTypePair{name, str_type}, 1, default_field, true);
        ASSERT_EQ(column_info.tp, TiDB::TypeString);
        auto data_type = getDataTypeByColumnInfo(column_info);
        // Get data type by column_info always returns the default type.
        ASSERT_EQ(data_type->getName(), DataTypeString::getDefaultName());
    }

    // TODO: test decimal, datetime, enum
}
CATCH

} // namespace tests
} // namespace DB
