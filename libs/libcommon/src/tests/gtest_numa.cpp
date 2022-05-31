// Copyright 2022 PingCAP, Ltd.
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
#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <common/numa.h>
#include <gtest/gtest.h>

class NumaTest : public ::testing::Test
{
public:
    static Poco::Logger * getLogger()
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cout);
        // Poco::AutoPtr<Poco::FileChannel> channel(new Poco::FileChannel("/tmp/logger_test"));
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("[%H:%M:%S.%i %Z] [%p] [%U(%u)]: %t"));
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Poco::Logger::root().setChannel(formatting_channel);
        Poco::Logger::root().setLevel(Poco::Message::PRIO_TRACE);
        return &Poco::Logger::get("NumaTest");
    }
};


TEST_F(NumaTest, Initialization)
{
    using namespace common::numa;
    auto log = getLogger();
    {
        NumaCTL test("wrong file", log);
    }
    NumaCTL test(nullptr, log);
    if (!test.numa_available || test.numa_available() == -1)
    {
        LOG_FMT_INFO(log, "skip the test as numa library/syscalls are not available on this system");
    }
}

TEST_F(NumaTest, StringParse)
{
    using namespace common::numa;
    auto log = getLogger();
    NumaCTL test(nullptr, log);
    if (!test.numa_parse_nodestring)
    {
        LOG_FMT_INFO(log, "skip the test as numa library/syscalls are not available on this system");
    }
    {
        auto * mask = test.numa_parse_nodestring("0");
        EXPECT_TRUE(mask);
        test.free_nodemask(mask);
    }
    {
        auto * mask = test.numa_parse_nodestring("all");
        EXPECT_TRUE(mask);
        test.free_nodemask(mask);
    }
}