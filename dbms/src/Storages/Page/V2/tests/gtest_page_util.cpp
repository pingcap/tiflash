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

#include <IO/BaseFile/PosixRandomAccessFile.h>
#include <IO/BaseFile/PosixWritableFile.h>
#include <Poco/Logger.h>
#include <Storages/Page/PageUtil.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>

namespace DB
{
namespace FailPoints
{
extern const char force_split_io_size_4k[];
} // namespace FailPoints

namespace tests
{
static const std::string FileName = "page_util_test";

TEST(PageUtilsTest, ReadWriteFile)
try
{
    ::remove(FileName.c_str());

    size_t buff_size = 1024;
    char buff_write[buff_size];

    for (size_t i = 0; i < buff_size; i++)
    {
        buff_write[i] = i % 0xFF;
    }
    WritableFilePtr file_for_write = std::make_shared<PosixWritableFile>(FileName, true, -1, 0666);
    PageUtil::writeFile(
        file_for_write,
        0,
        buff_write,
        buff_size,
        /*write_limiter*/ nullptr,
        /*background*/ false,
        /*truncate_if_failed*/ true,
        /*enable_failpoint*/ true);
    PageUtil::syncFile(file_for_write);
    file_for_write->close();

    char buff_read[buff_size];
    RandomAccessFilePtr file_for_read = std::make_shared<PosixRandomAccessFile>(FileName, -1, nullptr);
    PageUtil::readFile(file_for_read, 0, buff_read, buff_size, nullptr);
    ASSERT_EQ(strcmp(buff_write, buff_read), 0);

    ::remove(FileName.c_str());
}
CATCH

TEST(PageUtilsTest, FileNotExists)
{
    ::remove(FileName.c_str());

    int fd = PageUtil::openFile<true, false>(FileName);
    ASSERT_EQ(fd, 0);
}

TEST(PageUtilsTest, BigReadWriteFile)
{
    ::remove(FileName.c_str());

    FailPointHelper::enableFailPoint(FailPoints::force_split_io_size_4k);
    try
    {
        WritableFilePtr file_for_write = std::make_shared<PosixWritableFile>(FileName, true, -1, 0666);
        size_t buff_size = 13 * 1024 + 123;
        char buff_write[buff_size];
        char buff_read[buff_size];

        for (size_t i = 0; i < buff_size; i++)
        {
            buff_write[i] = i % 0xFF;
        }

        PageUtil::writeFile(
            file_for_write,
            0,
            buff_write,
            buff_size,
            nullptr,
            /*background*/ false,
            /*truncate_if_failed*/ true,
            /*enable_failpoint*/ false);
        PageUtil::syncFile(file_for_write);
        file_for_write->close();

        RandomAccessFilePtr file_for_read = std::make_shared<PosixRandomAccessFile>(FileName, -1, nullptr);
        PageUtil::readFile(file_for_read, 0, buff_read, buff_size, nullptr);
        ASSERT_EQ(strcmp(buff_write, buff_read), 0);

        ::remove(FileName.c_str());
        FailPointHelper::disableFailPoint(FailPoints::force_split_io_size_4k);
    }
    catch (DB::Exception & e)
    {
        ::remove(FileName.c_str());
        FailPointHelper::disableFailPoint(FailPoints::force_split_io_size_4k);
        FAIL() << e.getStackTrace().toString();
    }
}

} // namespace tests
} // namespace DB
