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
#include <IO/AsynchronousWriteBuffer.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteIntText.h>
#include <common/types.h>

#include <iomanip>
#include <iostream>


UInt64 rdtsc()
{
#if __x86_64__
    UInt64 val;
    __asm__ __volatile__("rdtsc" : "=A"(val) :);
    return val;
#else
    // TODO: make for arm
    return 0;
#endif
}


int main(int argc, char ** argv)
{
    try
    {
        if (argc < 2)
        {
            std::cerr << "Usage: program n\n";
            return 1;
        }

        using T = UInt8;

        size_t n = atoi(argv[1]);
        std::vector<T> data(n);
        std::vector<T> data2(n);

        {
            Stopwatch watch;

            for (size_t i = 0; i < n; ++i)
                data[i] = lrand48(); // / lrand48();// ^ (lrand48() << 24) ^ (lrand48() << 48);

            watch.stop();
            std::cerr << std::fixed << std::setprecision(2) << "Generated " << n << " numbers ("
                      << data.size() * sizeof(data[0]) / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
                      << data.size() * sizeof(data[0]) / watch.elapsedSeconds() / 1000000 << " MB/s." << std::endl;
        }

        std::vector<char> formatted;
        formatted.reserve(n * 21);

        {
            DB::WriteBufferFromVector<> wb(formatted);
            //    DB::CompressedWriteBuffer wb2(wb1);
            //    DB::AsynchronousWriteBuffer wb(wb2);
            Stopwatch watch;

            UInt64 tsc = rdtsc();

            for (size_t i = 0; i < n; ++i)
            {
                //writeIntTextTable(data[i], wb);
                DB::writeIntText(data[i], wb);
                //DB::writeIntText(data[i], wb);
                DB::writeChar('\t', wb);
            }

            tsc = rdtsc() - tsc;

            watch.stop();
            std::cerr << std::fixed << std::setprecision(2) << "Written " << n << " numbers (" << wb.count() / 1000000.0
                      << " MB) in " << watch.elapsedSeconds() << " sec., " << n / watch.elapsedSeconds() << " num/s., "
                      << wb.count() / watch.elapsedSeconds() / 1000000 << " MB/s., " << watch.elapsed() / n
                      << " ns/num., " << tsc / n << " ticks/num., " << watch.elapsed() / wb.count() << " ns/byte., "
                      << tsc / wb.count() << " ticks/byte." << std::endl;
        }

        {
            DB::ReadBuffer rb(&formatted[0], formatted.size(), 0);
            //    DB::CompressedReadBuffer rb(rb_);
            Stopwatch watch;

            for (size_t i = 0; i < n; ++i)
            {
                DB::readIntText(data2[i], rb);
                DB::assertChar('\t', rb);
            }

            watch.stop();
            std::cerr << std::fixed << std::setprecision(2) << "Read " << n << " numbers (" << rb.count() / 1000000.0
                      << " MB) in " << watch.elapsedSeconds() << " sec., "
                      << rb.count() / watch.elapsedSeconds() / 1000000 << " MB/s." << std::endl;
        }

        std::cerr << (0 == memcmp(&data[0], &data2[0], data.size()) ? "Ok." : "Fail.") << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
