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

#include "iostream_debug_helpers.h"
#include <Parsers/IParser.h>
#include <Parsers/Lexer.h>
#include <Parsers/TokenIterator.h>

namespace DB
{


std::ostream & operator<<(std::ostream & stream, const Token & what) {
    stream << "Token (type="<< static_cast<int>(what.type) <<"){"<< std::string{what.begin, what.end} << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const Expected & what) {
    stream << "Expected {variants=";
    dumpValue(stream, what.variants)
       << "; max_parsed_pos=" << what.max_parsed_pos << "}";
    return stream;
}

}
