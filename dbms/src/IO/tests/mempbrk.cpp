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

#include <string>
#include <iostream>
#include <iomanip>

#include <Common/Stopwatch.h>

#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>

#include <common/find_symbols.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
}
}


namespace test
{
    void readEscapedString(DB::String & s, DB::ReadBuffer & buf)
    {
        s = "";
        while (!buf.eof())
        {
            const char * next_pos = find_first_symbols<'\b', '\f', '\n', '\r', '\t', '\0', '\\'>(buf.position(), buf.buffer().end());

            s.append(buf.position(), next_pos - buf.position());
            buf.position() += next_pos - buf.position();

            if (!buf.hasPendingData())
                continue;

            if (*buf.position() == '\t' || *buf.position() == '\n')
                return;

            if (*buf.position() == '\\')
            {
                ++buf.position();
                if (buf.eof())
                    throw DB::Exception("Cannot parse escape sequence", DB::ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
                s += DB::parseEscapeSequence(*buf.position());
                ++buf.position();
            }
        }
    }
}


int main(int, char **)
{
    try
    {
        DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);
//        DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);
        std::string s;
        size_t rows = 0;

        Stopwatch watch;

        while (!in.eof())
        {
            test::readEscapedString(s, in);
            in.ignore();

            ++rows;

/*            DB::writeEscapedString(s, out);
            DB::writeChar('\n', out);*/
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "Read " << rows << " rows (" << in.count() / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
            << rows / watch.elapsedSeconds() << " rows/sec. (" << in.count() / watch.elapsedSeconds() / 1000000 << " MB/s.)"
            << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
