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

#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ConcatReadBuffer.h>


using namespace DB;

int main(int, char **)
try
{
    std::string s1 = "abc\\x\n";
    std::string s2 = "\tdef";

    ReadBufferFromMemory rb1(s1.data(), 3);
    ReadBufferFromMemory rb2(s2.data(), s2.size());

    ConcatReadBuffer rb3(rb1, rb2);

    std::string read_s1;
    std::string read_s2;

    readEscapedString(read_s1, rb3);
    assertChar('\t', rb3);
    readEscapedString(read_s2, rb3);

    std::cerr << read_s1 << ", " << read_s2 << std::endl;
    std::cerr << ((read_s1 == "abc" && read_s2 == "def") ? "Ok." : "Fail.") << std::endl;

    return 0;
}
catch (const Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
