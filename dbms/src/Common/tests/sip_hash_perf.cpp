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

#include <Common/SipHash.h>
#include <Common/Stopwatch.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>

#include <iomanip>
#include <string>
#include <vector>


/** Test this way:
  *
  * clickhouse-client --query="SELECT SearchPhrase AS k FROM test.hits WHERE k != ''" > phrases.tsv
  * clickhouse-client --query="SELECT URL AS k FROM test.hits" > urls.tsv
  * clickhouse-client --query="SELECT SearchPhrase AS k FROM test.hits" > phrases_with_empty.tsv
  * clickhouse-client --query="SELECT Title AS k FROM test.hits" > titles.tsv
  * clickhouse-client --query="SELECT PageCharset AS k FROM test.hits" > charset.tsv
  *
  * for i in {1..1000}; do ./sip_hash_perf < titles.tsv 2>&1 | grep Processed | grep -oP '\d+\.\d+ rows/sec'; done | awk '{ if ($1 > x) { x = $1; print x } }'
  */


int main(int, char **)
{
    std::vector<std::string> data;
    DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);

    std::cerr << std::fixed << std::setprecision(3);

    {
        Stopwatch watch;

        while (!in.eof())
        {
            data.emplace_back();
            DB::readEscapedString(data.back(), in);
            DB::assertChar('\n', in);
        }

        double seconds = watch.elapsedSeconds();
        std::cerr << "Read "
                  << data.size() << " rows, "
                  << (in.count() / 1048576.0) << " MiB "
                  << " in " << seconds << " sec., "
                  << (data.size() / seconds) << " rows/sec., "
                  << (in.count() / 1048576.0 / seconds) << " MiB/sec.\n";
    }

    {
        size_t res = 0;
        Stopwatch watch;

        for (const auto & s : data)
        {
            SipHash hash;
            hash.update(s.data(), s.size());
            res += hash.get64();
        }

        double seconds = watch.elapsedSeconds();
        std::cerr << "Processed "
                  << data.size() << " rows, "
                  << (in.count() / 1048576.0) << " MiB "
                  << " in " << seconds << " sec., "
                  << (data.size() / seconds) << " rows/sec., "
                  << (in.count() / 1048576.0 / seconds) << " MiB/sec. "
                  << "(res = " << res << ")\n";
    }

    return 0;
}
