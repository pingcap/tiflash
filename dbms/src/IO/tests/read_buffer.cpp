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
#include <sstream>

#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>


int main(int, char **)
{
    try
    {
        std::string s = "-123456 123.456 вася пе\\tтя\t'\\'xyz\\\\'";
        DB::ReadBufferFromString in(s);

        DB::Int64 a;
        DB::Float64 b;
        DB::String c, d;

        DB::readIntText(a, in);
        in.ignore();

        DB::readFloatText(b, in);
        in.ignore();

        DB::readEscapedString(c, in);
        in.ignore();

        DB::readQuotedString(d, in);

        std::cout << a << ' ' << b << ' ' << c << '\t' << '\'' << d << '\'' << std::endl;
        std::cout << in.count() << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
