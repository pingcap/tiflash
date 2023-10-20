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

#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>
#include <cpptoml.h>

#include <sstream>

namespace DB::tests
{

TEST(CPPTomlTest, ContainsQualifiedArray)
{
    auto * log = &Poco::Logger::get("CPPTomlTest");

    Strings failure_tests = {
        R"(
[a]
[a.b]
c = "abc"
        )",
        R"(
[a]
[a.b]
c = 123
        )",
        R"(
[a]
[a.b]
c = 123.45
        )",
    };

    for (size_t i = 0; i < failure_tests.size(); ++i)
    {
        const auto & test_case = failure_tests[i];
        SCOPED_TRACE(fmt::format("[index={}] [content={}]", i, test_case));
        LOG_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        std::istringstream ss(test_case);
        cpptoml::parser p(ss);
        auto table = p.parse();

        const char * key = "a.b.c";
        ASSERT_TRUE(table->contains_qualified(key));
        auto qualified = table->get_qualified(key);
        ASSERT_TRUE(qualified);
        // not array
        ASSERT_FALSE(qualified->is_array());
        // try to parse as vector<string>, return false
        cpptoml::option<Strings> array = table->get_qualified_array_of<String>(key);
        ASSERT_FALSE(array);
    }
}

TEST(CPPTomlTest, ContainsQualifiedStringArray)
{
    auto * log = &Poco::Logger::get("CPPTomlTest");

    Strings failure_tests = {
        R"(
[a]
[a.b]
c = [["abc", "def"], ["z"]]
        )",
        R"(
[a]
[a.b]
c = [123, 456]
        )",
    };

    for (size_t i = 0; i < failure_tests.size(); ++i)
    {
        const auto & test_case = failure_tests[i];
        SCOPED_TRACE(fmt::format("[index={}] [content={}]", i, test_case));
        LOG_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        std::istringstream ss(test_case);
        cpptoml::parser p(ss);
        auto table = p.parse();

        const char * key = "a.b.c";
        ASSERT_TRUE(table->contains_qualified(key));
        auto qualified = table->get_qualified(key);
        ASSERT_TRUE(qualified);
        // is non-empty array but not string array
        ASSERT_TRUE(qualified->is_array());
        auto qualified_array = qualified->as_array();
        ASSERT_NE(qualified_array->begin(), qualified_array->end());
        // try to parse as vector<string>, return false
        cpptoml::option<Strings> array = table->get_qualified_array_of<String>(key);
        ASSERT_FALSE(array);
    }
}

TEST(CPPTomlTest, ContainsQualifiedStringArrayOrEmpty)
{
    auto * log = &Poco::Logger::get("CPPTomlTest");

    Strings failure_tests = {
        // a.b.c is not empty
        R"(
[a]
[a.b]
c = ["abc", "def", "z"]
        )",
        // a.b.c is empty
        R"(
[a]
[a.b]
c = []
        )",
    };

    for (size_t i = 0; i < failure_tests.size(); ++i)
    {
        const auto & test_case = failure_tests[i];
        SCOPED_TRACE(fmt::format("[index={}] [content={}]", i, test_case));
        LOG_INFO(log, "parsing [index={}] [content={}]", i, test_case);

        std::istringstream ss(test_case);
        cpptoml::parser p(ss);
        auto table = p.parse();

        const char * key = "a.b.c";
        ASSERT_TRUE(table->contains_qualified(key));
        auto qualified = table->get_qualified(key);
        ASSERT_TRUE(qualified);
        // is non-empty array but not string array
        ASSERT_TRUE(qualified->is_array());

        // try to parse as vector<string>, return true
        cpptoml::option<Strings> array = table->get_qualified_array_of<String>(key);
        ASSERT_TRUE(array);
        if (auto qualified_array = qualified->as_array(); qualified_array->begin() != qualified_array->end())
        {
            ASSERT_EQ(array->size(), 3);
        }
    }
}

} // namespace DB::tests
