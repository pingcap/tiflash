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

#include <string>

#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>

#include <Core/Types.h>
#include <Common/Stopwatch.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


int main(int, char **)
{
    try
    {
        std::cout << std::fixed << std::setprecision(2);

        size_t n = 100000000;
        Stopwatch stopwatch;

        {
            DB::WriteBufferFromFile buf("test1", DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_TRUNC);
            DB::CompressedWriteBuffer compressed_buf(buf);

            stopwatch.restart();
            for (size_t i = 0; i < n; ++i)
            {
                DB::writeIntText(i, compressed_buf);
                DB::writeChar('\t', compressed_buf);
            }
            stopwatch.stop();
            std::cout << "Writing done (1). Elapsed: " << stopwatch.elapsedSeconds()
                << ", " << (compressed_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s"
                << std::endl;
        }

        {
            DB::ReadBufferFromFile buf("test1");
            DB::CompressedReadBuffer compressed_buf(buf);

            stopwatch.restart();
            for (size_t i = 0; i < n; ++i)
            {
                size_t x;
                DB::readIntText(x, compressed_buf);
                compressed_buf.ignore();

                if (x != i)
                {
                    std::stringstream s;
                    s << "Failed!, read: " << x << ", expected: " << i;
                    throw DB::Exception(s.str());
                }
            }
            stopwatch.stop();
            std::cout << "Reading done (1). Elapsed: " << stopwatch.elapsedSeconds()
                << ", " << (compressed_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s"
                << std::endl;
        }
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
