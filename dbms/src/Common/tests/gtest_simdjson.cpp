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

#include <gtest/gtest.h>
#include <simdjson.h>

namespace DB::tests
{
TEST(TestSIMDJson, error)
{
    simdjson::dom::parser parser;
    {
        std::string json_str{};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.error());
    }
    {
        std::string json_str{"[]]"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.error());
    }
    {
        std::string json_str{"fsdfhsdjhfjsdhfj"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.error());
    }
    {
        std::string json_str{"{}}"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.error());
    }
    {
        std::string json_str{"[[], [[fdjfhdjf]]]"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.error());
    }
}

TEST(TestSIMDJson, literal)
{
    simdjson::dom::parser parser;
    {
        std::string json_str{"0"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_number());
        auto actual = res.get_double();
        ASSERT_TRUE(!actual.error());
        ASSERT_EQ(actual.value_unsafe(), 0);
    }
    {
        std::string json_str{"1"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_number());
        auto actual = res.get_double();
        ASSERT_TRUE(!actual.error());
        ASSERT_EQ(actual.value_unsafe(), 1);
    }
    {
        std::string json_str{"-1"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_number());
        auto actual = res.get_double();
        ASSERT_TRUE(!actual.error());
        ASSERT_EQ(actual.value_unsafe(), -1);
    }
    {
        std::string json_str{"1.111"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_number());
        auto actual = res.get_double();
        ASSERT_TRUE(!actual.error());
        ASSERT_EQ(actual.value_unsafe(), 1.111);
    }
    {
        std::string json_str{"-1.111"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_number());
        auto actual = res.get_double();
        ASSERT_TRUE(!actual.error());
        ASSERT_EQ(actual.value_unsafe(), -1.111);
    }
    {
        std::string json_str{"true"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_bool());
        auto actual = res.get_bool();
        ASSERT_TRUE(!actual.error());
        ASSERT_EQ(actual.value_unsafe(), true);
    }
    {
        std::string json_str{"false"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_bool());
        auto actual = res.get_bool();
        ASSERT_TRUE(!actual.error());
        ASSERT_EQ(actual.value_unsafe(), false);
    }
    {
        std::string json_str{"null"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_null());
    }
    {
        std::string json_str{"\"a\""};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_string());
        auto actual = res.get_string();
        ASSERT_TRUE(!actual.error());
        ASSERT_EQ(std::string(actual.value_unsafe()), "a");
    }
}

TEST(TestSIMDJson, array)
{
    simdjson::dom::parser parser;
    {
        std::string json_str{"[]"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_array());
        auto array = res.get_array();
        ASSERT_TRUE(!array.error());
        const auto & actual = array.value_unsafe();
        ASSERT_EQ(actual.size(), 0);
    }
    {
        std::string json_str{"[1, 2]"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_array());
        auto array = res.get_array();
        ASSERT_TRUE(!array.error());
        const auto & actual = array.value_unsafe();
        ASSERT_EQ(actual.size(), 2);
    }
    {
        std::string json_str{"[1,2]"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_array());
        auto array = res.get_array();
        ASSERT_TRUE(!array.error());
        const auto & actual = array.value_unsafe();
        ASSERT_EQ(actual.size(), 2);
    }
    {
        std::string json_str{"[[]]"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_array());
        auto array = res.get_array();
        ASSERT_TRUE(!array.error());
        const auto & actual = array.value_unsafe();
        ASSERT_EQ(actual.size(), 1);
        ASSERT_TRUE(actual.at(0).is_array());
    }
}

TEST(TestSIMDJson, object)
{
    simdjson::dom::parser parser;
    {
        std::string json_str{"{}"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_object());
        auto obj = res.get_object();
        ASSERT_TRUE(!obj.error());
        const auto & actual = obj.value_unsafe();
        ASSERT_EQ(actual.size(), 0);
    }
    {
        std::string json_str{R"({"a":"b"})"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_object());
        auto obj = res.get_object();
        ASSERT_TRUE(!obj.error());
        const auto & actual = obj.value_unsafe();
        ASSERT_EQ(actual.size(), 1);
        const auto & value = actual.at_key("a");
        ASSERT_TRUE(value.is_string());
        ASSERT_EQ(std::string(value.get_string().value_unsafe()), "b");
    }
    {
        std::string json_str{R"({"a" : "b"})"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_object());
        auto obj = res.get_object();
        ASSERT_TRUE(!obj.error());
        const auto & actual = obj.value_unsafe();
        ASSERT_EQ(actual.size(), 1);
        const auto & value = actual.at_key("a");
        ASSERT_TRUE(value.is_string());
        ASSERT_EQ(std::string(value.get_string().value_unsafe()), "b");
    }
    {
        std::string json_str{R"({"a" : "b", "c":"d"})"};
        auto res = parser.parse(json_str);
        ASSERT_TRUE(res.is_object());
        auto obj = res.get_object();
        ASSERT_TRUE(!obj.error());
        const auto & actual = obj.value_unsafe();
        ASSERT_EQ(actual.size(), 2);
        const auto & value = actual.at_key("c");
        ASSERT_TRUE(value.is_string());
        ASSERT_EQ(std::string(value.get_string().value_unsafe()), "d");
    }
}

} // namespace DB::tests
