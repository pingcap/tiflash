// Copyright 2024 PingCAP, Inc.
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

#include <Core/Defines.h>
#include <common/unaligned.h>

namespace DB::ALP
{

class Constants
{
public:
    static constexpr UInt16 SAMPLES_PER_VECTOR = 32;

    static constexpr UInt8 FRAME_OF_REFERENCE_SIZE = sizeof(UInt64);
    static constexpr UInt8 BIT_WIDTH_SIZE = sizeof(UInt8);
    static constexpr UInt8 EXPONENT_SIZE = sizeof(UInt8);
    static constexpr UInt8 FACTOR_SIZE = sizeof(UInt8);
    static constexpr UInt8 FOR_SIZE = sizeof(UInt64);
    static constexpr UInt8 EXCEPTION_POSITION_SIZE = sizeof(UInt16);
    static constexpr UInt8 HEADER_SIZE = FRAME_OF_REFERENCE_SIZE + BIT_WIDTH_SIZE + EXPONENT_SIZE + FACTOR_SIZE;

    static constexpr UInt8 SAMPLING_EARLY_EXIT_THRESHOLD = 2;

    static void writeHeader(
        char *& dest,
        UInt64 frame_of_reference,
        UInt8 bit_width,
        UInt8 vector_exponent,
        UInt8 vector_factor)
    {
        unalignedStore<UInt64>(dest, frame_of_reference);
        dest += FRAME_OF_REFERENCE_SIZE;
        unalignedStore<UInt8>(dest, bit_width);
        dest += BIT_WIDTH_SIZE;
        unalignedStore<UInt8>(dest, vector_exponent);
        dest += EXPONENT_SIZE;
        unalignedStore<UInt8>(dest, vector_factor);
        dest += FACTOR_SIZE;
    }

    static std::tuple<UInt64, UInt8, UInt8, UInt8> readHeader(const char *& source)
    {
        auto frame_of_reference = unalignedLoad<UInt64>(source);
        source += FRAME_OF_REFERENCE_SIZE;
        auto bit_width = unalignedLoad<UInt8>(source);
        source += BIT_WIDTH_SIZE;
        auto vector_exponent = unalignedLoad<UInt8>(source);
        source += EXPONENT_SIZE;
        auto vector_factor = unalignedLoad<UInt8>(source);
        source += FACTOR_SIZE;
        return {frame_of_reference, bit_width, vector_exponent, vector_factor};
    }

    // Largest double which fits into an int64
    static constexpr double ENCODING_UPPER_LIMIT = 9223372036854774784.0;
    static constexpr double ENCODING_LOWER_LIMIT = -9223372036854774784.0;

    static constexpr UInt8 MAX_COMBINATIONS = 5;

    static constexpr const Int64 FACT_ARR[]
        = {1,
           10,
           100,
           1000,
           10000,
           100000,
           1000000,
           10000000,
           100000000,
           1000000000,
           10000000000,
           100000000000,
           1000000000000,
           10000000000000,
           100000000000000,
           1000000000000000,
           10000000000000000,
           100000000000000000,
           1000000000000000000};
};

template <class T>
struct TypedConstants
{
};

template <>
struct TypedConstants<float>
{
    static constexpr float MAGIC_NUMBER = 12582912.0; //! 2^22 + 2^23
    static constexpr UInt8 MAX_EXPONENT = 10;
    // exceptions positions + exceptions values
    static constexpr UInt8 EXCEPTIONS_PAIR_SIZE = sizeof(UInt16) + sizeof(float);

    static constexpr const float EXP_ARR[]
        = {1.0F,
           10.0F,
           100.0F,
           1000.0F,
           10000.0F,
           100000.0F,
           1000000.0F,
           10000000.0F,
           100000000.0F,
           1000000000.0F,
           10000000000.0F};

    static constexpr float FRAC_ARR[]
        = {1.0F,
           0.1F,
           0.01F,
           0.001F,
           0.0001F,
           0.00001F,
           0.000001F,
           0.0000001F,
           0.00000001F,
           0.000000001F,
           0.0000000001F};
};

template <>
struct TypedConstants<double>
{
    static constexpr double MAGIC_NUMBER = 6755399441055744.0; //! 2^51 + 2^52
    static constexpr UInt8 MAX_EXPONENT = 18; //! 10^18 is the maximum int64
    // exceptions positions + exceptions values
    static constexpr UInt8 EXCEPTIONS_PAIR_SIZE = sizeof(UInt16) + sizeof(double);

    static constexpr const double EXP_ARR[]
        = {1.0,
           10.0,
           100.0,
           1000.0,
           10000.0,
           100000.0,
           1000000.0,
           10000000.0,
           100000000.0,
           1000000000.0,
           10000000000.0,
           100000000000.0,
           1000000000000.0,
           10000000000000.0,
           100000000000000.0,
           1000000000000000.0,
           10000000000000000.0,
           100000000000000000.0,
           1000000000000000000.0,
           10000000000000000000.0,
           100000000000000000000.0,
           1000000000000000000000.0,
           10000000000000000000000.0,
           100000000000000000000000.0};

    static constexpr double FRAC_ARR[]
        = {1.0,
           0.1,
           0.01,
           0.001,
           0.0001,
           0.00001,
           0.000001,
           0.0000001,
           0.00000001,
           0.000000001,
           0.0000000001,
           0.00000000001,
           0.000000000001,
           0.0000000000001,
           0.00000000000001,
           0.000000000000001,
           0.0000000000000001,
           0.00000000000000001,
           0.000000000000000001,
           0.0000000000000000001,
           0.00000000000000000001};
};

} // namespace DB::ALP
