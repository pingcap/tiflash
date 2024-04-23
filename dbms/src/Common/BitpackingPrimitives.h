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

#include <Common/Exception.h>
#include <bitpackinghelpers.h>
#include <common/types.h>
#include <common/unaligned.h>

namespace DB
{

/**
 * class BitpackingPrimitives provides basic functions to do bitpacking.
 * For signed integers, it will apply the zigzag encoding first.
 */
class BitpackingPrimitives
{
public:
    static constexpr const size_t BITPACKING_ALGORITHM_GROUP_SIZE = 32;
    static constexpr const bool BYTE_ALIGNED = false;

    // To ensure enough data is available, use GetRequiredSize() to determine the correct size for dst buffer
    // Note: input should be aligned to BITPACKING_ALGORITHM_GROUP_SIZE for good performance.
    template <typename T, bool ASSUME_INPUT_ALIGNED = false>
    static void packBuffer(unsigned char * dst, const T * src, size_t count, UInt8 width)
    {
        if constexpr (ASSUME_INPUT_ALIGNED)
        {
            for (size_t i = 0; i < count; i += BITPACKING_ALGORITHM_GROUP_SIZE)
            {
                packGroup(dst + (i * width) / 8, src + i, width);
            }
        }
        else
        {
            size_t misaligned_count = count % BITPACKING_ALGORITHM_GROUP_SIZE;
            count -= misaligned_count;
            for (size_t i = 0; i < count; i += BITPACKING_ALGORITHM_GROUP_SIZE)
            {
                packGroup(dst + (i * width) / 8, src + i, width);
            }
            // Input was not aligned to BITPACKING_ALGORITHM_GROUP_SIZE, we need a copy
            if (misaligned_count)
            {
                T tmp_buffer[BITPACKING_ALGORITHM_GROUP_SIZE]; // TODO: maybe faster on the heap?
                memcpy(tmp_buffer, src + count, misaligned_count * sizeof(T));
                packGroup(dst + (count * width) / 8, tmp_buffer, width);
            }
        }
    }

    // Unpacks a block of BITPACKING_ALGORITHM_GROUP_SIZE values
    // Assumes both src and dst to be of the correct size
    template <typename T>
    static void unPackBuffer(
        unsigned char * dst,
        const unsigned char * src,
        size_t count,
        UInt8 width,
        bool skip_sign_extension = false)
    {
        for (size_t i = 0; i < count; i += BITPACKING_ALGORITHM_GROUP_SIZE)
        {
            unPackGroup<T>(dst + i * sizeof(T), src + (i * width) / 8, width, skip_sign_extension);
        }
    }

    // Packs a block of BITPACKING_ALGORITHM_GROUP_SIZE values
    template <typename T>
    static void packBlock(unsigned char * dst, const T * src, UInt8 width)
    {
        return packGroup(dst, src, width);
    }

    // Unpacks a block of BITPACKING_ALGORITHM_GROUP_SIZE values
    template <typename T>
    static void unPackBlock(
        unsigned char * dst,
        const unsigned char * src,
        UInt8 width,
        bool skip_sign_extension = false)
    {
        return unPackGroup<T>(dst, src, width, skip_sign_extension);
    }

    // Calculates the minimum required number of bits per value that can store all values
    template <typename T, bool is_signed = std::numeric_limits<T>::is_signed>
    constexpr static UInt8 minimumBitWidth(T value)
    {
        return findMinimumBitWidth<T, is_signed, BYTE_ALIGNED>(value, value);
    }

    // Calculates the minimum required number of bits per value that can store all values
    template <typename T, bool is_signed = std::numeric_limits<T>::is_signed>
    constexpr static UInt8 minimumBitWidth(const T * values, size_t count)
    {
        return findMinimumBitWidth<T, is_signed, BYTE_ALIGNED>(values, count);
    }

    // Calculates the minimum required number of bits per value that can store all values,
    // given a predetermined minimum and maximum value of the buffer
    template <typename T, bool is_signed = std::numeric_limits<T>::is_signed>
    constexpr static UInt8 minimumBitWidth(T minimum, T maximum)
    {
        return findMinimumBitWidth<T, is_signed, BYTE_ALIGNED>(minimum, maximum);
    }

    constexpr static size_t getRequiredSize(size_t count, UInt8 width)
    {
        count = roundUpToAlgorithmGroupSize(count);
        return ((count * width) / 8);
    }

    // round up to nearest multiple of BITPACKING_ALGORITHM_GROUP_SIZE
    template <typename T>
    constexpr static T roundUpToAlgorithmGroupSize(T num_to_round)
    {
        static_assert(
            (BITPACKING_ALGORITHM_GROUP_SIZE & (BITPACKING_ALGORITHM_GROUP_SIZE - 1)) == 0,
            "BITPACKING_ALGORITHM_GROUP_SIZE must be a power of 2");
        constexpr T mask = BITPACKING_ALGORITHM_GROUP_SIZE - 1;
        return (num_to_round + mask) & ~mask;
    }

private:
    template <typename T, bool is_signed, bool round_to_next_byte = false>
    constexpr static UInt8 findMinimumBitWidth(const T * values, size_t count)
    {
        T min_value = values[0];
        T max_value = *std::max_element(values, values + count);
        if constexpr (is_signed)
        {
            min_value = *std::min_element(values, values + count);
        }
        return findMinimumBitWidth<T, is_signed, round_to_next_byte>(min_value, max_value);
    }

    template <typename T, bool is_signed, bool round_to_next_byte = false>
    constexpr static UInt8 findMinimumBitWidth(T min_value, T max_value)
    {
        UInt8 bitwidth;
        T value;

        if constexpr (is_signed)
        {
            if (min_value == std::numeric_limits<T>::min())
            {
                // handle special case of the minimal value, as it cannot be negated like all other values.
                return sizeof(T) * 8;
            }
            else
            {
                value = std::max(static_cast<T>(-min_value), max_value);
            }
        }
        else
        {
            value = max_value;
        }

        if (value == 0)
        {
            return 0;
        }

        if constexpr (is_signed)
        {
            bitwidth = 1;
        }
        else
        {
            bitwidth = 0;
        }

        while (value)
        {
            bitwidth++;
            value >>= 1;
        }

        bitwidth = getEffectiveWidth<T>(bitwidth);

        // Assert results are correct
        if (bitwidth < sizeof(T) * 8 && bitwidth != 0)
        {
            if constexpr (is_signed)
            {
                RUNTIME_ASSERT(max_value <= (T(1) << (bitwidth - 1)) - 1);
                RUNTIME_ASSERT(min_value >= (T(-1) * ((T(1) << (bitwidth - 1)) - 1) - 1));
            }
            else
            {
                RUNTIME_ASSERT(max_value <= (T(1) << (bitwidth)) - 1);
            }
        }
        if constexpr (round_to_next_byte)
        {
            return (bitwidth / 8 + (bitwidth % 8 != 0)) * 8;
        }
        return bitwidth;
    }

    // Sign bit extension
    template <typename T, typename TU = typename std::make_unsigned<T>::type>
    static void signExtend(unsigned char * dst, UInt8 width)
    {
        T const mask = static_cast<T>(TU(1) << (width - 1));
        for (size_t i = 0; i < BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE; ++i)
        {
            T value = unalignedLoad<T>(dst + i * sizeof(T));
            value = static_cast<T>(value & ((TU(1) << width) - TU(1)));
            T result = (value ^ mask) - mask;
            unalignedStore<T>(dst + i * sizeof(T), result);
        }
    }

    // Prevent compression at widths that are ineffective
    template <typename T>
    constexpr static UInt8 getEffectiveWidth(UInt8 width)
    {
        UInt8 bits_of_type = sizeof(T) * 8;
        UInt8 type_size = sizeof(T);
        if (width + type_size > bits_of_type)
        {
            return bits_of_type;
        }
        return width;
    }

    template <typename T>
    static void packGroup(unsigned char * dst, const T * values, UInt8 width)
    {
        if constexpr (std::is_same<T, Int8>::value || std::is_same<T, UInt8>::value)
        {
            fastpforlib::fastpack(
                reinterpret_cast<const UInt8 *>(values),
                reinterpret_cast<UInt8 *>(dst),
                static_cast<UInt32>(width));
        }
        else if constexpr (std::is_same<T, Int16>::value || std::is_same<T, UInt16>::value)
        {
            fastpforlib::fastpack(
                reinterpret_cast<const UInt16 *>(values),
                reinterpret_cast<UInt16 *>(dst),
                static_cast<UInt32>(width));
        }
        else if constexpr (std::is_same<T, Int32>::value || std::is_same<T, UInt32>::value)
        {
            fastpforlib::fastpack(
                reinterpret_cast<const UInt32 *>(values),
                reinterpret_cast<UInt32 *>(dst),
                static_cast<UInt32>(width));
        }
        else if constexpr (std::is_same<T, Int64>::value || std::is_same<T, UInt64>::value)
        {
            fastpforlib::fastpack(
                reinterpret_cast<const UInt64 *>(values),
                reinterpret_cast<UInt32 *>(dst),
                static_cast<UInt32>(width));
        }
        else
        {
            // TODO: use static_assert(false, xxx) instead until the toolchain upgrade to clang 17.0
            static_assert(sizeof(T *) == 0, "Unsupported type for bitpacking");
        }
    }

    template <typename T>
    static void unPackGroup(
        unsigned char * dst,
        const unsigned char * src,
        UInt8 width,
        bool skip_sign_extension = false)
    {
        if constexpr (std::is_same<T, Int8>::value || std::is_same<T, UInt8>::value)
        {
            fastpforlib::fastunpack(
                reinterpret_cast<const UInt8 *>(src),
                reinterpret_cast<UInt8 *>(dst),
                static_cast<UInt32>(width));
        }
        else if constexpr (std::is_same<T, Int16>::value || std::is_same<T, UInt16>::value)
        {
            fastpforlib::fastunpack(
                reinterpret_cast<const UInt16 *>(src),
                reinterpret_cast<UInt16 *>(dst),
                static_cast<UInt32>(width));
        }
        else if constexpr (std::is_same<T, Int32>::value || std::is_same<T, UInt32>::value)
        {
            fastpforlib::fastunpack(
                reinterpret_cast<const UInt32 *>(src),
                reinterpret_cast<UInt32 *>(dst),
                static_cast<UInt32>(width));
        }
        else if constexpr (std::is_same<T, Int64>::value || std::is_same<T, UInt64>::value)
        {
            fastpforlib::fastunpack(
                reinterpret_cast<const UInt32 *>(src),
                reinterpret_cast<UInt64 *>(dst),
                static_cast<UInt32>(width));
        }
        else
        {
            // TODO: use static_assert(false, xxx) instead until the toolchain upgrade to clang 17.0
            static_assert(sizeof(T *) == 0, "Unsupported type for bitpacking");
        }

        if (std::numeric_limits<T>::is_signed && !skip_sign_extension && width > 0 && width < sizeof(T) * 8)
        {
            signExtend<T>(dst, width);
        }
    }
};

} // namespace DB
