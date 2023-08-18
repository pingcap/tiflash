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

#include <Core/Types.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteHelpers.h>

#include <fstream>
#include <iostream>
#include <string>


int main(int, char **)
{
    try
    {
        DB::Int64 a = -123456;
        DB::Float64 b = 123.456;
        DB::String c = "вася пе\tтя";
        DB::String d = "'xyz\\";

        std::ofstream s("test");
        DB::WriteBufferFromOStream out(s);

        for (int i = 0; i < 1000000; ++i)
        {
            DB::writeIntText(a, out);
            DB::writeChar(' ', out);

            DB::writeFloatText(b, out);
            DB::writeChar(' ', out);

            DB::writeEscapedString(c, out);
            DB::writeChar('\t', out);

            DB::writeQuotedString(d, out);
            DB::writeChar('\n', out);
        }
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
