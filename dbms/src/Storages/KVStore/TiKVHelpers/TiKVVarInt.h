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

#pragma once

#include <Core/Types.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/WriteBuffer.h>
#include <common/likely.h>

#include <iostream>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace TiKV
{

using DB::Exception;
using DB::Int16;
using DB::Int32;
using DB::Int64;
using DB::Int8;
using DB::ReadBuffer;
using DB::UInt16;
using DB::UInt32;
using DB::UInt64;
using DB::UInt8;
using DB::WriteBuffer;

void writeVarUInt(UInt64 x, std::ostream & ostr);
void writeVarUInt(UInt64 x, WriteBuffer & ostr);
char * writeVarUInt(UInt64 x, char * ostr);


void readVarUInt(UInt64 & x, std::istream & istr);
void readVarUInt(UInt64 & x, ReadBuffer & istr);
const char * readVarUInt(UInt64 & x, const char * istr, size_t size);


size_t getLengthOfVarUInt(UInt64 x);

size_t getLengthOfVarInt(Int64 x);

template <typename OUT>
inline void writeVarInt(Int64 x, OUT & ostr)
{
    TiKV::writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

inline char * writeVarInt(Int64 x, char * ostr)
{
    return writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

template <typename IN>
inline void readVarInt(Int64 & x, IN & istr)
{
    TiKV::readVarUInt(*reinterpret_cast<UInt64 *>(&x), istr);
    x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
}

inline const char * readVarInt(Int64 & x, const char * istr, size_t size)
{
    const char * res = readVarUInt(*reinterpret_cast<UInt64 *>(&x), istr, size);
    x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
    return res;
}


inline void writeVarT(UInt64 x, std::ostream & ostr)
{
    writeVarUInt(x, ostr);
}
inline void writeVarT(Int64 x, std::ostream & ostr)
{
    writeVarInt(x, ostr);
}
inline void writeVarT(UInt64 x, WriteBuffer & ostr)
{
    TiKV::writeVarUInt(x, ostr);
}
inline void writeVarT(Int64 x, WriteBuffer & ostr)
{
    TiKV::writeVarInt(x, ostr);
}
inline char * writeVarT(UInt64 x, char *& ostr)
{
    return writeVarUInt(x, ostr);
}
inline char * writeVarT(Int64 x, char *& ostr)
{
    return writeVarInt(x, ostr);
}

inline void readVarT(UInt64 & x, std::istream & istr)
{
    readVarUInt(x, istr);
}
inline void readVarT(Int64 & x, std::istream & istr)
{
    readVarInt(x, istr);
}
inline void readVarT(UInt64 & x, ReadBuffer & istr)
{
    TiKV::readVarUInt(x, istr);
}
inline void readVarT(Int64 & x, ReadBuffer & istr)
{
    TiKV::readVarInt(x, istr);
}
inline const char * readVarT(UInt64 & x, const char * istr, size_t size)
{
    return readVarUInt(x, istr, size);
}
inline const char * readVarT(Int64 & x, const char * istr, size_t size)
{
    return readVarInt(x, istr, size);
}


/// For [U]Int32, [U]Int16, size_t.

inline void readVarUInt(UInt32 & x, ReadBuffer & istr)
{
    UInt64 tmp = 0;
    TiKV::readVarUInt(tmp, istr);
    x = static_cast<UInt32>(tmp);
}

inline void readVarInt(Int32 & x, ReadBuffer & istr)
{
    Int64 tmp = 0;
    TiKV::readVarInt(tmp, istr);
    x = static_cast<Int32>(tmp);
}

inline void readVarUInt(UInt16 & x, ReadBuffer & istr)
{
    UInt64 tmp = 0;
    TiKV::readVarUInt(tmp, istr);
    x = static_cast<UInt16>(tmp);
}

inline void readVarInt(Int16 & x, ReadBuffer & istr)
{
    Int64 tmp = 0;
    TiKV::readVarInt(tmp, istr);
    x = static_cast<Int16>(tmp);
}

template <typename T>
inline std::enable_if_t<!std::is_same_v<T, UInt64>, void> readVarUInt(T & x, ReadBuffer & istr)
{
    UInt64 tmp = 0;
    TiKV::readVarUInt(tmp, istr);
    x = tmp;
}

inline void throwReadAfterEOF()
{
    throw Exception("Attempt to read after eof", DB::ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
}

inline void throwReadUnfinished()
{
    throw Exception("Read unfinished", DB::ErrorCodes::LOGICAL_ERROR);
}

inline void readVarUInt(UInt64 & x, ReadBuffer & istr)
{
    x = 0;
    for (size_t i = 0; i < 10; ++i)
    {
        if (unlikely(istr.eof()))
            throwReadAfterEOF();

        UInt64 byte = *istr.position();
        ++istr.position();
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80))
            return;
    }
    throwReadUnfinished();
}


inline void readVarUInt(UInt64 & x, std::istream & istr)
{
    x = 0;
    for (size_t i = 0; i < 10; ++i)
    {
        auto byte = static_cast<UInt64>(istr.get());
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80))
            return;
    }
    throwReadUnfinished();
}

inline const char * readVarUInt(UInt64 & x, const char * istr, size_t size)
{
    const char * end = istr + size;

    x = 0;
    for (size_t i = 0; i < 10; ++i)
    {
        if (unlikely(istr == end))
            throwReadAfterEOF();

        auto byte = static_cast<UInt64>(*istr);
        ++istr;
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80))
            return istr;
    }
    throwReadUnfinished();
    return istr;
}


inline void writeVarUInt(UInt64 x, WriteBuffer & ostr)
{
    while (x >= 0x80)
    {
        ostr.nextIfAtEnd();
        *ostr.position() = static_cast<char>(static_cast<UInt8>(x) | 0x80);
        ++ostr.position();
        x >>= 7;
    }
    ostr.nextIfAtEnd();
    *ostr.position() = static_cast<char>(x);
    ++ostr.position();
}


inline void writeVarUInt(UInt64 x, std::ostream & ostr)
{
    while (x >= 0x80)
    {
        ostr.put(static_cast<char>(static_cast<UInt8>(x) | 0x80));
        x >>= 7;
    }
    ostr.put(static_cast<char>(x));
}


inline char * writeVarUInt(UInt64 x, char * ostr)
{
    while (x >= 0x80)
    {
        *ostr = static_cast<char>(static_cast<UInt8>(x) | 0x80);
        ++ostr;
        x >>= 7;
    }
    *ostr = static_cast<char>(x);
    ++ostr;
    return ostr;
}


inline void writeVarUInt(UInt64 x, std::string & ostr)
{
    while (x >= 0x80)
    {
        ostr += static_cast<char>(static_cast<UInt8>(x) | 0x80);
        x >>= 7;
    }
    ostr += static_cast<char>(x);
}


inline size_t getLengthOfVarUInt(UInt64 x)
{
    return x < (1ULL << 7)
        ? 1
        : (x < (1ULL << 14)
               ? 2
               : (x < (1ULL << 21)
                      ? 3
                      : (x < (1ULL << 28)
                             ? 4
                             : (x < (1ULL << 35)
                                    ? 5
                                    : (x < (1ULL << 42)
                                           ? 6
                                           : (x < (1ULL << 49)
                                                  ? 7
                                                  : (x < (1ULL << 56) ? 8 : (x < (1ULL << 63) ? 9 : 10))))))));
}


inline size_t getLengthOfVarInt(Int64 x)
{
    return getLengthOfVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)));
}

} // namespace TiKV
