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

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferValidUTF8.h>
#include <IO/WriteHelpers.h>

#include <cstdio>
#include <iostream>
#include <streambuf>
#include <string>

int main(int, char **)
{
    try
    {
        std::string test1 = "kjhsgdfkjhg2378rtzgvxkz877%^&^*%&^*&*";
        std::string test2 = "{\"asd\" = \"qw1\",\"qwe24\" = \"3asd\"}";
        test2[test2.find('1')] = char(127 + 64);
        test2[test2.find('2')] = char(127 + 64 + 32);
        test2[test2.find('3')] = char(127 + 64 + 32 + 16);
        test2[test2.find('4')] = char(127 + 64 + 32 + 16 + 8);

        std::string str;
        {
            DB::WriteBufferFromString str_buf(str);
            {
                DB::WriteBufferValidUTF8 utf_buf(str_buf, true, "-");
                DB::writeEscapedString(test1, utf_buf);
            }
        }
        std::cout << str << std::endl;

        str = "";
        {
            DB::WriteBufferFromString str_buf(str);
            {
                DB::WriteBufferValidUTF8 utf_buf(str_buf, true, "-");
                DB::writeEscapedString(test2, utf_buf);
            }
        }
        std::cout << str << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
