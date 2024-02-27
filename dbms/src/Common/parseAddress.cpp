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

#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include <IO/ReadHelpers.h>
#include <common/find_symbols.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

std::pair<std::string, UInt16> parseAddress(const std::string & str, UInt16 default_port)
{
    if (str.empty())
        throw Exception("Empty address passed to function parseAddress", ErrorCodes::BAD_ARGUMENTS);

    const char * begin = str.data();
    const char * end = begin + str.size();
    const char * port = nullptr;

    if (begin[0] == '[')
    {
        const char * closing_square_bracket = find_first_symbols<']'>(begin + 1, end);
        if (closing_square_bracket >= end)
            throw Exception(
                "Illegal address passed to function parseAddress: "
                "the address begins with opening square bracket, but no closing square bracket found",
                ErrorCodes::BAD_ARGUMENTS);

        port = find_first_symbols<':'>(closing_square_bracket + 1, end);
    }
    else
        port = find_first_symbols<':'>(begin, end);

    if (port != end)
    {
        auto port_number = parse<UInt16>(port + 1);
        return {std::string(begin, port), port_number};
    }
    else if (default_port)
    {
        return {str, default_port};
    }
    else
        throw Exception(
            "The address passed to function parseAddress doesn't contain port number "
            "and no 'default_port' was passed",
            ErrorCodes::BAD_ARGUMENTS);
}

} // namespace DB
