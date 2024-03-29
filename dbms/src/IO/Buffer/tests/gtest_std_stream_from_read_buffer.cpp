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

#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/StdStreamFromReadBuffer.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>


namespace DB::tests
{

TEST(StdStreamFromReadBuffer, TestRead)
{
    std::string data = "Hello, world!";
    ReadBufferFromString buf(data);
    StdStreamFromReadBuffer stream(buf, data.size());

    char c;
    stream.read(&c, 1);
    EXPECT_EQ(c, 'H');

    char buf2[5];
    stream.read(buf2, 5);
    EXPECT_EQ(std::string(buf2, 5), "ello,");

    stream.read(buf2, 5);
    EXPECT_EQ(std::string(buf2, 5), " worl");

    stream.read(buf2, 5);
    EXPECT_EQ(std::string(buf2, 5), "d!orl");

    stream.read(buf2, 5);
    EXPECT_EQ(stream.gcount(), 0);
}

TEST(StdStreamFromReadBuffer, TestGetLine)
{
    std::string data = "Hello, world!";
    ReadBufferFromString buf(data);
    StdStreamFromReadBuffer stream(buf, data.size());

    std::string line;
    std::getline(stream, line);
    EXPECT_EQ(line, data);
}

TEST(StdStreamFromReadBuffer, TestReadLine)
{
    std::string data = "Hello, world!";
    ReadBufferFromString buf(data);
    StdStreamFromReadBuffer stream(buf, data.size());

    std::string line;
    std::getline(stream, line);
    EXPECT_EQ(line, data);
}

TEST(StdStreamFromReadBuffer, TestReadLineWithDelimiter)
{
    std::string data = "Hello, world!";
    ReadBufferFromString buf(data);
    StdStreamFromReadBuffer stream(buf, data.size());

    std::string line;
    std::getline(stream, line, ',');
    EXPECT_EQ(line, "Hello");
}

TEST(StdStreamFromReadBuffer, TestIgnore)
{
    std::string data = "Hello, world!";
    auto temp_dir = base::TiFlashStorageTestBasic::getTemporaryPath() + "/";
    if (Poco::File file(temp_dir); !file.exists())
        file.createDirectories();
    auto file_path = temp_dir + base::TiFlashStorageTestBasic::getCurrentTestName();
    auto * file_ptr = std::fopen(file_path.c_str(), "w");
    std::fwrite(data.c_str(), 1, data.size(), file_ptr);
    std::fclose(file_ptr);
    ReadBufferFromFile buf(file_path);
    StdStreamFromReadBuffer stream(buf, data.size());

    stream.ignore(7);
    char c;
    stream.read(&c, 1);
    EXPECT_EQ(c, 'w');
}

} // namespace DB::tests
