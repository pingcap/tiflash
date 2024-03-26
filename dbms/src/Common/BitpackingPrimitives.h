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

#include <common/types.h>

namespace DB
{

class BitpackingPrimitives
{
public:
    static constexpr const size_t BITPACKING_ALGORITHM_GROUP_SIZE = 32;
    static constexpr const size_t BITPACKING_HEADER_SIZE = sizeof(UInt64);
    static constexpr const bool BYTE_ALIGNED = false;

    // To ensure enough data is available, use GetRequiredSize() to determine the correct size for dst buffer
    // Note: input should be aligned to BITPACKING_ALGORITHM_GROUP_SIZE for good performance.
    template <typename T, bool ASSUME_INPUT_ALIGNED = false>
    static void packBuffer(unsigned char * dst, const T * src, size_t count, UInt8 width);

    // Unpacks a block of BITPACKING_ALGORITHM_GROUP_SIZE values
    // Assumes both src and dst to be of the correct size
    template <typename T>
    static void unPackBuffer(
        unsigned char * dst,
        const unsigned char * src,
        size_t count,
        UInt8 width,
        bool skip_sign_extension = false);

    // Packs a block of BITPACKING_ALGORITHM_GROUP_SIZE values
    template <typename T>
    static void packBlock(unsigned char * dst, const T * src, UInt8 width);

    // Unpacks a block of BITPACKING_ALGORITHM_GROUP_SIZE values
    template <typename T>
    static void unPackBlock(unsigned char * dst, const unsigned char * src, UInt8 width, bool skip_sign_extension = false);

    // Calculates the minimum required number of bits per value that can store all values
    template <typename T, bool is_signed = std::numeric_limits<T>::IsSigned()>
    static UInt8 minimumBitWidth(T value);

    // Calculates the minimum required number of bits per value that can store all values
    template <typename T, bool is_signed = std::numeric_limits<T>::IsSigned()>
    static UInt8 minimumBitWidth(const T * values, size_t count);

    // Calculates the minimum required number of bits per value that can store all values,
    // given a predetermined minimum and maximum value of the buffer
    template <typename T, bool is_signed = std::numeric_limits<T>::IsSigned()>
    static UInt8 minimumBitWidth(T minimum, T maximum);

    static size_t getRequiredSize(size_t count, UInt8 width);

    template <typename T>
    static T roundUpToAlgorithmGroupSize(T num_to_round);

private:
    template <typename T, bool is_signed, bool round_to_next_byte = false>
    static UInt8 findMinimumBitWidth(const T * values, size_t count);

    template <typename T, bool is_signed, bool round_to_next_byte = false>
    static UInt8 findMinimumBitWidth(T min_value, T max_value);

    // Sign bit extension
    template <typename T, typename TU = typename std::make_unsigned<T>::type>
    static void signExtend(unsigned char * dst, UInt8 width);

    // Prevent compression at widths that are ineffective
    template <typename T>
    static UInt8 getEffectiveWidth(UInt8 width);

    template <typename T>
    static void packGroup(unsigned char * dst, const T * values, UInt8 width);

    template <typename T>
    static void unPackGroup(unsigned char * dst, const unsigned char * src, UInt8 width, bool skip_sign_extension = false);
};

} // namespace DB
