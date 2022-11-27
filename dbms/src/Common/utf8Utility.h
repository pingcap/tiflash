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

#pragma once

#include <common/likely.h>
#include <Core/Types.h>

namespace DB
{

static const UInt32 UTF8Self  = 0x80;       // characters below UTF8Self are represented as themselves in a single byte.
static const UInt32 UNICODEMax = 0x0010FFFF;  // Maximum valid Unicode code point.
static const UInt32 UTF8Error = UNICODEMax + 1;     // the "error" code

static const char * JsonHexChars[] = {
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "a",
    "b",
    "c",
    "d",
    "e",
    "f"};

/// JsonSafeSet holds the value true if the ASCII character with the given array
/// position can be represented inside a JSON string without any further
/// escaping.

/// All values are true except for the ASCII control characters (0-31), the
/// double quote ("), and the backslash character ("\").
static const bool JsonSafeAscii[] = {
    false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false,
    true, true, /* '"' */ false, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, /* '\\' */ false, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true
};

/// Based on a public domain branch-less UTF-8 decoder by Christopher Wellons:
/// https://github.com/skeeto/branchless-utf8 (Unlicensed)
/// Changes:
/// 1. check byte length check branch and padding zeros inside the function if input string length < 4
/// 2. If 'buf' is empty it returns (UTF8Error, 0). Otherwise, if the encoding is invalid, it returns (UTF8Error, 1)
///  (UTF8Error, 1), 1 to be aligned with go's DecodeRune library behavior
/* Decode the next character, C, from BUF, reporting errors in E.
 *
 * Errors are reported in E, which will be non-zero if the parsed
 * character was somehow invalid: invalid byte sequence, non-canonical
 * encoding, or a surrogate half.
 *
 * The function returns <UTFChar, ConsumedSize> when correctly decoded. When an error
 * occurs, behavior is described as Changes 2 above.
 */
std::pair<UInt32, UInt32> utf8Decode(const char * buf, UInt32 buf_length);

void utf8Encode(char * buf, size_t & used_length, UInt32 unicode);
} // namespace DB