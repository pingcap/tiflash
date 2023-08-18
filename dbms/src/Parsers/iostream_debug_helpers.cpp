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

#include "iostream_debug_helpers.h"
#include <Parsers/IParser.h>
#include <Parsers/Lexer.h>
#include <Parsers/TokenIterator.h>

namespace DB
{

<<<<<<< HEAD:dbms/src/Parsers/iostream_debug_helpers.cpp

std::ostream & operator<<(std::ostream & stream, const Token & what) {
    stream << "Token (type="<< static_cast<int>(what.type) <<"){"<< std::string{what.begin, what.end} << "}";
    return stream;
=======
TEST(TestColumnGenerator, run)
try
{
    std::vector<String> type_vec
        = {"Int8",
           "Int16",
           "Int32",
           "Int64",
           "UInt8",
           "UInt16",
           "UInt32",
           "UInt64",
           "Float32",
           "Float64",
           "String",
           "MyDateTime",
           "MyDate",
           "Decimal"};
    for (size_t i = 10; i <= 100000; i *= 10)
    {
        for (auto type : type_vec)
            ASSERT_EQ(ColumnGenerator::instance().generate({i, type, RANDOM}).column->size(), i);
    }
>>>>>>> 6638f2067b (Fix license and format coding style (#7962)):dbms/src/TestUtils/tests/gtest_column_generator.cpp
}

std::ostream & operator<<(std::ostream & stream, const Expected & what) {
    stream << "Expected {variants=";
    dumpValue(stream, what.variants)
       << "; max_parsed_pos=" << what.max_parsed_pos << "}";
    return stream;
}

}
