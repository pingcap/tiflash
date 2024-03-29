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

#include <Common/hex.h>
#include <DataStreams/verbosePrintString.h>
#include <IO/Operators.h>


namespace DB
{

void verbosePrintString(const char * begin, const char * end, WriteBuffer & out)
{
    if (end == begin)
    {
        out << "<EMPTY>";
        return;
    }

    out << "\"";

    for (const auto * pos = begin; pos < end; ++pos)
    {
        switch (*pos)
        {
        case '\0':
            out << "<ASCII NUL>";
            break;
        case '\b':
            out << "<BACKSPACE>";
            break;
        case '\f':
            out << "<FORM FEED>";
            break;
        case '\n':
            out << "<LINE FEED>";
            break;
        case '\r':
            out << "<CARRIAGE RETURN>";
            break;
        case '\t':
            out << "<TAB>";
            break;
        case '\\':
            out << "<BACKSLASH>";
            break;
        case '"':
            out << "<DOUBLE QUOTE>";
            break;
        case '\'':
            out << "<SINGLE QUOTE>";
            break;

        default:
        {
            if (static_cast<unsigned char>(*pos) < 32) /// ASCII control characters
                out << "<0x" << hexDigitUppercase(*pos / 16) << hexDigitUppercase(*pos % 16) << ">";
            else
                out << *pos;
        }
        }
    }

    out << "\"";
}

} // namespace DB
