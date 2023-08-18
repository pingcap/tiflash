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

#include <Common/Stopwatch.h>
#include <Core/Types.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ZlibDeflatingWriteBuffer.h>
#include <IO/ZlibInflatingReadBuffer.h>

#include <iomanip>
#include <iostream>
#include <string>


int main(int, char **)
try
{
    std::cout << std::fixed << std::setprecision(2);

    size_t n = 100000000;
    Stopwatch stopwatch;

    {
        DB::WriteBufferFromFile buf("test_zlib_buffers.gz", DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_TRUNC);
        DB::ZlibDeflatingWriteBuffer deflating_buf(buf, DB::ZlibCompressionMethod::Gzip, /* compression_level = */ 3);

        stopwatch.restart();
        for (size_t i = 0; i < n; ++i)
        {
            DB::writeIntText(i, deflating_buf);
            DB::writeChar('\t', deflating_buf);
        }
        deflating_buf.finish();

        stopwatch.stop();
        std::cout << "Writing done. Elapsed: " << stopwatch.elapsedSeconds() << " s."
                  << ", " << (deflating_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s" << std::endl;
    }

    {
        DB::ReadBufferFromFile buf("test_zlib_buffers.gz");
        DB::ZlibInflatingReadBuffer inflating_buf(buf, DB::ZlibCompressionMethod::Gzip);

        stopwatch.restart();
        for (size_t i = 0; i < n; ++i)
        {
            size_t x;
            DB::readIntText(x, inflating_buf);
            inflating_buf.ignore();

            if (x != i)
                throw DB::Exception("Failed!, read: " + std::to_string(x) + ", expected: " + std::to_string(i));
        }
        stopwatch.stop();
        std::cout << "Reading done. Elapsed: " << stopwatch.elapsedSeconds() << " s."
                  << ", " << (inflating_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s" << std::endl;
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
