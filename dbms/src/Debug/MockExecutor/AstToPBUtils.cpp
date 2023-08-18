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

#include <Debug/MockExecutor/AstToPBUtils.h>

namespace DB
{
ColumnName splitQualifiedName(const String & s)
{
    ColumnName ret;
    Poco::StringTokenizer string_tokens(s, ".");

    switch (string_tokens.count())
    {
    case 1:
        ret.column_name = s;
        break;
    case 2:
        ret.table_name = string_tokens[0];
        ret.column_name = string_tokens[1];
        break;
    case 3:
        ret.db_name = string_tokens[0];
        ret.table_name = string_tokens[1];
        ret.column_name = string_tokens[2];
        break;
    default:
        throw Exception("Invalid identifier name " + s);
    }
    return ret;
}

DAGSchema::const_iterator checkSchema(const DAGSchema & input, const String & checked_column)
{
    auto ft = std::find_if(input.begin(), input.end(), [&checked_column](const auto & field) {
        try
        {
            auto [checked_db_name, checked_table_name, checked_column_name] = splitQualifiedName(checked_column);
            auto [db_name, table_name, column_name] = splitQualifiedName(field.first);
            if (checked_table_name.empty())
                return column_name == checked_column_name;
            else
                return table_name == checked_table_name && column_name == checked_column_name;
        }
        catch (...)
        {
            return false;
        }
    });
    return ft;
}

DAGColumnInfo toNullableDAGColumnInfo(const DAGColumnInfo & input)
{
    DAGColumnInfo output = input;
    output.second.clearNotNullFlag();
    return output;
}

namespace Debug
{
String LOCAL_HOST = "127.0.0.1:3930";

void setServiceAddr(const std::string & addr)
{
    LOCAL_HOST = addr;
}
} // namespace Debug
} // namespace DB
