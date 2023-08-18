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

#include <Common/UTF8Helpers.h>

namespace DB
{
namespace UTF8
{
#define IS_SURROGATE(c) ((c) >= 0xD800U && (c) <= 0xDFFFU)

void utf8Encode(char * buf, size_t & used_length, UInt32 unicode)
{
    auto * s = reinterpret_cast<unsigned char *>(buf);
    if (unicode > UNICODE_Max || IS_SURROGATE(unicode))
        unicode = 0xFFFD; /// 'Unknown character'

    if (unicode >= (1L << 16))
    {
        s[0] = 0xf0 | (unicode >> 18);
        s[1] = 0x80 | ((unicode >> 12) & 0x3f);
        s[2] = 0x80 | ((unicode >> 6) & 0x3f);
        s[3] = 0x80 | ((unicode >> 0) & 0x3f);
        used_length = 4;
    }
    else if (unicode >= (1L << 11))
    {
        s[0] = 0xe0 | (unicode >> 12);
        s[1] = 0x80 | ((unicode >> 6) & 0x3f);
        s[2] = 0x80 | ((unicode >> 0) & 0x3f);
        used_length = 3;
    }
    else if (unicode >= (1L << 7))
    {
        s[0] = 0xc0 | (unicode >> 6);
        s[1] = 0x80 | ((unicode >> 0) & 0x3f);
        used_length = 2;
    }
    else
    {
        s[0] = static_cast<UInt8>(unicode);
        used_length = 1;
    }
}

std::pair<UInt32, UInt32> utf8Decode(const char * buf, UInt32 buf_length)
{
    /// Avoid expanding const arrays one element per line
    // clang-format off
    static const unsigned char lengths[] = {
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 3, 3, 4, 0
    };
    // clang-format on
    static const int masks[] = {0x00, 0x7f, 0x1f, 0x0f, 0x07};
    static const UInt32 mins[] = {4194304, 0, 128, 2048, 65536};
    static const int shiftc[] = {0, 18, 12, 6, 0};
    static const int shifte[] = {0, 6, 4, 2, 0};

    if unlikely (buf_length == 0)
        return std::make_pair(UTF8_Error, 0);

    const auto * s = reinterpret_cast<const unsigned char *>(buf);
    UInt32 len = lengths[s[0] >> 3];
    if unlikely (buf_length < len || len == 0)
        return std::make_pair(UTF8_Error, 1);

    /// buf_length >= len > 0
    UInt32 c = static_cast<UInt32>(s[0] & masks[len]) << 18;
    UInt32 err = 0;
    switch (len)
    {
    case 1:
        c >>= shiftc[1];
        break;
    case 2:
        c |= static_cast<UInt32>(s[1] & 0x3f) << 12;
        c >>= shiftc[2];
        err |= (s[1] & 0xc0) >> 2;
        break;
    case 3:
        c |= static_cast<UInt32>(s[1] & 0x3f) << 12;
        c |= static_cast<UInt32>(s[2] & 0x3f) << 6;
        c >>= shiftc[3];
        err |= (s[1] & 0xc0) >> 2;
        err |= (s[2] & 0xc0) >> 4;
        break;
    case 4:
    default:
        c |= static_cast<UInt32>(s[1] & 0x3f) << 12;
        c |= static_cast<UInt32>(s[2] & 0x3f) << 6;
        c |= static_cast<UInt32>(s[3] & 0x3f);
        c >>= shiftc[len];
        err |= (s[1] & 0xc0) >> 2;
        err |= (s[2] & 0xc0) >> 4;
        err |= s[3] >> 6;
        break;
    }

    /* Accumulate the various error conditions. */
    err |= (c < mins[len]) << 6; // non-canonical encoding
    err |= ((c >> 11) == 0x1b) << 7; // surrogate half?
    err |= (c > UNICODE_Max) << 8; // out of range?
    err ^= 0x2a; // top two bits of each tail byte correct?
    err >>= shifte[len];

    if likely (err == 0)
        return std::make_pair(c, len);
    else
        return std::make_pair(UTF8_Error, 1);
}
} // namespace UTF8
} // namespace DB