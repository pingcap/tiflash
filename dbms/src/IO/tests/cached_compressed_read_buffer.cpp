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

#include <iostream>
#include <iomanip>
#include <limits>

#include <IO/CachedCompressedReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>

#include <Common/Stopwatch.h>


int main(int argc, char ** argv)
{
    using namespace DB;

    if (argc < 2)
    {
        std::cerr << "Usage: program path\n";
        return 1;
    }

    try
    {
        UncompressedCache cache(1024);
        std::string path = argv[1];

        std::cerr << std::fixed << std::setprecision(3);

        size_t hits = 0;
        size_t misses = 0;

        {
            Stopwatch watch;
            CachedCompressedReadBuffer in(path, &cache, 0, 0);
            WriteBufferFromFile out("/dev/null");
            copyData(in, out);

            std::cerr << "Elapsed: " << watch.elapsedSeconds() << std::endl;
        }

        cache.getStats(hits, misses);
        std::cerr << "Hits: " << hits << ", misses: " << misses << std::endl;

        {
            Stopwatch watch;
            CachedCompressedReadBuffer in(path, &cache, 0, 0);
            WriteBufferFromFile out("/dev/null");
            copyData(in, out);

            std::cerr << "Elapsed: " << watch.elapsedSeconds() << std::endl;
        }

        cache.getStats(hits, misses);
        std::cerr << "Hits: " << hits << ", misses: " << misses << std::endl;
    }
    catch (const Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
