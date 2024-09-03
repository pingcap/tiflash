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

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/TargetSpecific.h>
#include <Common/UTF8Helpers.h>
#include <Common/Volnitsky.h>
#include <Core/AccurateComparison.h>
#include <DataTypes/DataTypeArray.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Functions/CharUtil.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/FunctionsRound.h>
#include <Functions/FunctionsString.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/StringUtil.h>
#include <Functions/castTypeToEither.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/printf.h>

#include <boost/algorithm/string/predicate.hpp>
#include <ext/range.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

using namespace GatherUtils;

template <bool negative = false>
struct EmptyImpl
{
    /// If the function will return constant value for FixedString data type.
    static constexpr auto is_fixed_to_constant = false;

    static void vector(const ColumnString::Chars_t & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res)
    {
        size_t size = offsets.size();
        ColumnString::Offset prev_offset = 1;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = negative ^ (offsets[i] == prev_offset);
            prev_offset = offsets[i] + 1;
        }
    }

    /// Only make sense if is_fixed_to_constant.
    static void vectorFixedToConstant(const ColumnString::Chars_t & /*data*/, size_t /*n*/, UInt8 & /*res*/)
    {
        throw Exception("Logical error: 'vector_fixed_to_constant method' is called", ErrorCodes::LOGICAL_ERROR);
    }

    static void vectorFixedToVector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt8> & res)
    {
        std::vector<char> empty_chars(n);
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
            res[i] = negative ^ (0 == memcmp(&data[i * size], empty_chars.data(), n));
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res)
    {
        size_t size = offsets.size();
        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = negative ^ (offsets[i] == prev_offset);
            prev_offset = offsets[i];
        }
    }
};


/** Calculates the length of a string in bytes.
  */
struct LengthImpl
{
    static constexpr auto is_fixed_to_constant = true;

    static void vector(const ColumnString::Chars_t & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = i == 0 ? (offsets[i] - 1) : (offsets[i] - 1 - offsets[i - 1]);
    }

    static void vectorFixedToConstant(const ColumnString::Chars_t & /*data*/, size_t n, UInt64 & res)
    {
        res = n;
    }

    static void vectorFixedToVector(const ColumnString::Chars_t & /*data*/, size_t /*n*/, PaddedPODArray<UInt64> & /*res*/)
    {
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = i == 0 ? (offsets[i]) : (offsets[i] - offsets[i - 1]);
    }
};


/** If the string is UTF-8 encoded text, it returns the length of the text in code points.
  * (not in characters: the length of the text "ё" can be either 1 or 2, depending on the normalization)
 * (not in characters: the length of the text "" can be either 1 or 2, depending on the normalization)
  * Otherwise, the behavior is undefined.
  */
struct LengthUTF8Impl
{
    static constexpr auto is_fixed_to_constant = false;

    static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = UTF8::countCodePoints(&data[prev_offset], offsets[i] - prev_offset - 1);
            prev_offset = offsets[i];
        }
    }

    static void vectorFixedToConstant(const ColumnString::Chars_t & /*data*/, size_t /*n*/, UInt64 & /*res*/)
    {
    }

    static void vectorFixedToVector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt64> & res)
    {
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
        {
            res[i] = UTF8::countCodePoints(&data[i * n], n);
        }
    }

    static void array(const ColumnString::Offsets &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function lengthUTF8 to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

namespace
{
template <class T>
using Ptr = T *;

template <class T>
using ConstPtr = T const *;

// there is a bug in tree-optimizer for GCC < 7.3.1,
// which will result in wrong code generation for avx512.
#if defined(__GNUC_PREREQ) && defined(__GNUC_PATCHLEVEL__)
#define TIFLASH_UPPER_LOWER_ASCII_NO_GCC_WORK_AROUND_WITH_SHIFT \
    (__GNUC_PREREQ(7, 4) || (__GNUC_PREREQ(7, 3) && __GNUC_PATCHLEVEL__ >= 1))
#else
#define TIFLASH_UPPER_LOWER_ASCII_NO_GCC_WORK_AROUND_WITH_SHIFT 1
#endif

TIFLASH_DECLARE_MULTITARGET_FUNCTION_TP(
    (char not_case_lower_bound, char not_case_upper_bound, char flip_case_mask),
    (not_case_lower_bound, not_case_upper_bound, flip_case_mask),
    void,
    lowerUpperAsciiArrayImpl,
    (src, src_end, dst),
    (ConstPtr<UInt8> src,
     const ConstPtr<UInt8> src_end,
     Ptr<UInt8> dst),
    {
#if TIFLASH_UPPER_LOWER_ASCII_NO_GCC_WORK_AROUND_WITH_SHIFT
        for (; src < src_end; ++src, ++dst)
            if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
                *dst = *src ^ flip_case_mask;
            else
                *dst = *src;
#else
        static constexpr UInt8 mask_shift = __builtin_ctz(flip_case_mask);
        for (; src < src_end; ++src, ++dst)
        {
            auto data = static_cast<UInt8>(*src <= not_case_upper_bound)
                & static_cast<UInt8>(*src >= not_case_lower_bound);
            *dst = *src ^ (data << mask_shift);
        }
#endif
    })
} // namespace

template <char not_case_lower_bound, char not_case_upper_bound>
struct LowerUpperImpl
{
    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        array(data.data(), data.data() + data.size(), res_data.data());
    }

    static void vectorFixed(const ColumnString::Chars_t & data, size_t /*n*/, ColumnString::Chars_t & res_data)
    {
        res_data.resize(data.size());
        array(data.data(), data.data() + data.size(), res_data.data());
    }

private:
    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
    {
        static constexpr auto flip_case_mask = 'A' ^ 'a';
        lowerUpperAsciiArrayImpl<not_case_lower_bound, not_case_upper_bound, flip_case_mask>(src, src_end, dst);
    }
};

/** Expands the string in bytes.
  */
struct ReverseImpl
{
    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (size_t j = prev_offset; j < offsets[i] - 1; ++j)
                res_data[j] = data[offsets[i] + prev_offset - 2 - j];
            res_data[offsets[i] - 1] = 0;
            prev_offset = offsets[i];
        }
    }

    static void vectorFixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data)
    {
        res_data.resize(data.size());
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
            for (size_t j = i * n; j < (i + 1) * n; ++j)
                res_data[j] = data[(i * 2 + 1) * n - j - 1];
    }
};


/** Expands the sequence of code points in a UTF-8 encoded string.
  * The result may not match the expected result, because modifying code points (for example, diacritics) may be applied to another symbols.
  * If the string is not encoded in UTF-8, then the behavior is undefined.
  */
struct ReverseUTF8Impl
{
    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset j = prev_offset;
            while (j < offsets[i] - 1)
            {
                if (data[j] < 0xBF)
                {
                    res_data[offsets[i] + prev_offset - 2 - j] = data[j];
                    j += 1;
                }
                else if (data[j] < 0xE0)
                {
                    memcpy(&res_data[offsets[i] + prev_offset - 2 - j - 1], &data[j], 2);
                    j += 2;
                }
                else if (data[j] < 0xF0)
                {
                    memcpy(&res_data[offsets[i] + prev_offset - 2 - j - 2], &data[j], 3);
                    j += 3;
                }
                else
                {
                    res_data[offsets[i] + prev_offset - 2 - j] = data[j];
                    j += 1;
                }
            }

            res_data[offsets[i] - 1] = 0;
            prev_offset = offsets[i];
        }
    }

    static void vectorFixed(const ColumnString::Chars_t &, size_t, ColumnString::Chars_t &)
    {
        throw Exception("Cannot apply function reverseUTF8 to fixed string.", ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int),
          void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::vector(
    const ColumnString::Chars_t & data,
    const IColumn::Offsets & offsets,
    ColumnString::Chars_t & res_data,
    IColumn::Offsets & res_offsets)
{
    res_data.resize(data.size());
    res_offsets.assign(offsets);
    array(data.data(), data.data() + data.size(), res_data.data());
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int),
          void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::vectorFixed(
    const ColumnString::Chars_t & data,
    size_t /*n*/,
    ColumnString::Chars_t & res_data)
{
    res_data.resize(data.size());
    array(data.data(), data.data() + data.size(), res_data.data());
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int),
          void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::constant(
    const std::string & data,
    std::string & res_data)
{
    res_data.resize(data.size());
    array(reinterpret_cast<const UInt8 *>(data.data()),
          reinterpret_cast<const UInt8 *>(data.data() + data.size()),
          reinterpret_cast<UInt8 *>(&res_data[0]));
}

namespace
{
template <char not_case_lower_bound,
          char not_case_upper_bound,
          char ascii_upper_bound,
          char flip_case_mask,
          int to_case(int),
          void cyrillic_to_case(ConstPtr<UInt8> &, Ptr<UInt8> &)>
__attribute__((always_inline)) inline void toCaseImpl(
    ConstPtr<UInt8> & src,
    const ConstPtr<UInt8> src_end,
    Ptr<UInt8> & dst)
{
    if (src[0] <= ascii_upper_bound)
    {
        if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
            *dst++ = *src++ ^ flip_case_mask;
        else
            *dst++ = *src++;
    }
    else if (src + 1 < src_end
             && ((src[0] == 0xD0u && (src[1] >= 0x80u && src[1] <= 0xBFu)) || (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x9Fu))))
    {
        cyrillic_to_case(src, dst);
    }
    else if (src + 1 < src_end && src[0] == 0xC2u)
    {
        /// Punctuation U+0080 - U+00BF, UTF-8: C2 80 - C2 BF
        *dst++ = *src++;
        *dst++ = *src++;
    }
    else if (src + 2 < src_end && src[0] == 0xE2u)
    {
        /// Characters U+2000 - U+2FFF, UTF-8: E2 80 80 - E2 BF BF
        *dst++ = *src++;
        *dst++ = *src++;
        *dst++ = *src++;
    }
    else
    {
        static const Poco::UTF8Encoding utf8;

        if (const auto chars = utf8.convert(to_case(utf8.convert(src)), dst, src_end - src))
            src += chars, dst += chars;
        else
            ++src, ++dst;
    }
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          char ascii_upper_bound,
          char flip_case_mask,
          int to_case(int)>
__attribute__((always_inline)) inline void toCaseImplTiDB(
    const UInt8 *& src,
    const UInt8 * src_end,
    size_t offsets_pos,
    ColumnString::Chars_t & dst_data,
    IColumn::Offsets & dst_offsets,
    bool & is_diff_offsets)
{
    if (*src <= ascii_upper_bound)
    {
        size_t dst_size = dst_data.size();
        dst_data.resize(dst_size + 1);
        if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
            dst_data[dst_size] = *src++ ^ flip_case_mask;
        else
            dst_data[dst_size] = *src++;
    }
    else
    {
        static const Poco::UTF8Encoding utf8;

        int src_sequence_length = utf8.sequenceLength(src, 1);
        assert(src_sequence_length > 0);
        if unlikely (src + src_sequence_length > src_end)
        {
            /// If this row has invalid utf-8 characters, just copy it to dst string and do not influence others
            size_t dst_size = dst_data.size();
            dst_data.resize(src_end - src + dst_size);
            memcpy(&dst_data[dst_size], src, src_end - src);
            src = src_end;
            return;
        }

        int src_ch = utf8.convert(src);
        if unlikely (src_ch == -1)
        {
            /// If this row has invalid utf-8 characters, just copy it to dst string and do not influence others
            size_t dst_size = dst_data.size();
            dst_data.resize(dst_size + src_sequence_length);
            memcpy(&dst_data[dst_size], src, src_sequence_length);
            src += src_sequence_length;
            return;
        }
        int dst_ch = to_case(src_ch);
        int dst_sequence_length = utf8.convert(dst_ch, nullptr, 0);
        size_t dst_size = dst_data.size();
        dst_data.resize(dst_size + dst_sequence_length);
        utf8.convert(dst_ch, &dst_data[dst_size], dst_sequence_length);

        if (dst_sequence_length != src_sequence_length)
        {
            assert((Int64)dst_offsets[offsets_pos] + dst_sequence_length - src_sequence_length >= 0);
            dst_offsets[offsets_pos] += dst_sequence_length - src_sequence_length;
            is_diff_offsets = true;
        }

        src += src_sequence_length;
    }
}

} // namespace

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int),
          void cyrillic_to_case(ConstPtr<UInt8> &, Ptr<UInt8> &)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::toCase(
    ConstPtr<UInt8> & src,
    const ConstPtr<UInt8> src_end,
    Ptr<UInt8> & dst)
{
    toCaseImpl<
        not_case_lower_bound,
        not_case_upper_bound,
        ascii_upper_bound,
        flip_case_mask,
        to_case,
        cyrillic_to_case>(src, src_end, dst);
}

namespace
{
TIFLASH_DECLARE_MULTITARGET_FUNCTION_TP(
    (char not_case_lower_bound,
     char not_case_upper_bound,
     char ascii_upper_bound,
     char flip_case_mask,
     int to_case(int),
     void cyrillic_to_case(const UInt8 *&, UInt8 *&)),
    (not_case_lower_bound,
     not_case_upper_bound,
     ascii_upper_bound,
     flip_case_mask,
     to_case,
     cyrillic_to_case),
    void,
    lowerUpperUTF8ArrayImpl,
    (src, src_end, dst),
    (ConstPtr<UInt8> & src,
     const ConstPtr<UInt8> src_end,
     Ptr<UInt8> & dst),
    {
        static const auto flip_mask = SimdWord::template fromSingle<int8_t>(flip_case_mask);
        while (src + WORD_SIZE < src_end)
        {
            auto word = SimdWord::fromUnaligned(src);
            auto ascii_check = SimdWord{};
            ascii_check.as_int8 = word.as_int8 >= 0;
            if (ascii_check.isByteAllMarked())
            {
                auto range_check = SimdWord{};
                auto selected = SimdWord{};
                auto lower_bounds = SimdWord::template fromSingle<int8_t>(not_case_lower_bound);
                auto upper_bounds = SimdWord::template fromSingle<int8_t>(not_case_upper_bound);
                range_check.as_int8 = (word.as_int8 >= lower_bounds.as_int8) & (word.as_int8 <= upper_bounds.as_int8);
                selected.as_int8 = range_check.as_int8 & flip_mask.as_int8;
                word.as_int8 ^= selected.as_int8;
                word.toUnaligned(dst);
                src += WORD_SIZE;
                dst += WORD_SIZE;
            }
            else
            {
                auto expected_end = src + WORD_SIZE;
                while (src < expected_end)
                {
                    toCaseImpl<
                        not_case_lower_bound,
                        not_case_upper_bound,
                        ascii_upper_bound,
                        flip_case_mask,
                        to_case,
                        cyrillic_to_case>(src, src_end, dst);
                }
            }
        }
        while (src < src_end)
            toCaseImpl<
                not_case_lower_bound,
                not_case_upper_bound,
                ascii_upper_bound,
                flip_case_mask,
                to_case,
                cyrillic_to_case>(src, src_end, dst);
    })

TIFLASH_DECLARE_MULTITARGET_FUNCTION_TP(
    (char not_case_lower_bound,
     char not_case_upper_bound,
     char ascii_upper_bound,
     char flip_case_mask,
     int to_case(int)),
    (not_case_lower_bound,
     not_case_upper_bound,
     ascii_upper_bound,
     flip_case_mask,
     to_case),
    void,
    lowerUpperUTF8ArrayImplTiDB,
    (src_data, src_offsets, dst_data, dst_offsets),
    (const ColumnString::Chars_t & src_data,
     const IColumn::Offsets & src_offsets,
     ColumnString::Chars_t & dst_data,
     IColumn::Offsets & dst_offsets),
    {
        dst_data.reserve(src_data.size());
        dst_offsets.assign(src_offsets);
        static const auto flip_mask = SimdWord::template fromSingle<int8_t>(flip_case_mask);
        const UInt8 *src = src_data.data(), *src_end = src_data.data() + src_data.size();
        auto * begin = src;
        bool is_diff_offsets = false;
        size_t offsets_pos = 0;
        while (src + WORD_SIZE < src_end)
        {
            auto word = SimdWord::fromUnaligned(src);
            auto ascii_check = SimdWord{};
            ascii_check.as_int8 = word.as_int8 >= 0;
            if (ascii_check.isByteAllMarked())
            {
                auto range_check = SimdWord{};
                auto selected = SimdWord{};
                auto lower_bounds = SimdWord::template fromSingle<int8_t>(not_case_lower_bound);
                auto upper_bounds = SimdWord::template fromSingle<int8_t>(not_case_upper_bound);
                range_check.as_int8 = (word.as_int8 >= lower_bounds.as_int8) & (word.as_int8 <= upper_bounds.as_int8);
                selected.as_int8 = range_check.as_int8 & flip_mask.as_int8;
                word.as_int8 ^= selected.as_int8;
                size_t dst_size = dst_data.size();
                dst_data.resize(dst_size + WORD_SIZE);
                word.toUnaligned(&dst_data[dst_size]);
                src += WORD_SIZE;
            }
            else
            {
                size_t offset_from_begin = src - begin;
                while (offset_from_begin >= src_offsets[offsets_pos])
                    ++offsets_pos;
                auto expected_end = src + WORD_SIZE;
                while (true)
                {
                    const UInt8 * row_end = begin + src_offsets[offsets_pos];
                    assert(row_end >= src);
                    auto end = std::min(expected_end, row_end);
                    while (src < end)
                    {
                        toCaseImplTiDB<
                            not_case_lower_bound,
                            not_case_upper_bound,
                            ascii_upper_bound,
                            flip_case_mask,
                            to_case>(src, row_end, offsets_pos, dst_data, dst_offsets, is_diff_offsets);
                    }
                    if (src >= expected_end)
                        break;
                    ++offsets_pos;
                }
            }
        }

        if (src < src_end)
        {
            size_t offset_from_begin = src - begin;
            while (offset_from_begin >= src_offsets[offsets_pos])
                ++offsets_pos;

            while (src < src_end)
            {
                const UInt8 * row_end = begin + src_offsets[offsets_pos];
                assert(row_end >= src);
                while (src < row_end)
                {
                    toCaseImplTiDB<
                        not_case_lower_bound,
                        not_case_upper_bound,
                        ascii_upper_bound,
                        flip_case_mask,
                        to_case>(src, row_end, offsets_pos, dst_data, dst_offsets, is_diff_offsets);
                }
                ++offsets_pos;
            }
        }

        if unlikely (is_diff_offsets)
        {
            Int64 diff = 0;
            for (size_t i = 0; i < dst_offsets.size(); ++i)
            {
                /// diff is the cumulative offset difference from 0 to the i position
                diff += (Int64)dst_offsets[i] - (Int64)src_offsets[i];
                dst_offsets[i] = src_offsets[i] + diff;
            }
        }
    })
} // namespace

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int),
          void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::array(
    ConstPtr<UInt8> src,
    const ConstPtr<UInt8> src_end,
    Ptr<UInt8> dst)
{
    lowerUpperUTF8ArrayImpl<
        not_case_lower_bound,
        not_case_upper_bound,
        ascii_upper_bound,
        flip_case_mask,
        to_case,
        cyrillic_to_case>(src, src_end, dst);
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int)>
void TiDBLowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case>::vector(
    const ColumnString::Chars_t & data,
    const IColumn::Offsets & offsets,
    ColumnString::Chars_t & res_data,
    IColumn::Offsets & res_offsets)
{
    lowerUpperUTF8ArrayImplTiDB<not_case_lower_bound, not_case_upper_bound, ascii_upper_bound, flip_case_mask, to_case>(
        data,
        offsets,
        res_data,
        res_offsets);
}

template <char not_case_lower_bound,
          char not_case_upper_bound,
          int to_case(int)>
void TiDBLowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case>::vectorFixed(
    const ColumnString::Chars_t & /*data*/,
    size_t /*n*/,
    ColumnString::Chars_t & /*res_data*/)
{
    throw Exception("Cannot apply function TiDBLowerUpperUTF8 to fixed string.", ErrorCodes::ILLEGAL_COLUMN);
}

/** If the string is encoded in UTF-8, then it selects a substring of code points in it.
  * Otherwise, the behavior is undefined.
  */
struct SubstringUTF8Impl
{
    template <bool implicit_length, bool is_positive_start>
    static void vectorConstConst(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        size_t original_start_abs,
        size_t length,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            doSubstringUTF8<implicit_length, is_positive_start>(i, data, offsets, original_start_abs, length, res_data, res_offsets, prev_offset, res_offset);
        }
    }

    template <bool implicit_length, typename StartFf, typename LengthFf>
    static void vectorVectorVector(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        StartFf && start_func,
        LengthFf && length_func,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto [is_positive, original_start_abs] = start_func(i);
            size_t length = 0;
            if constexpr (!implicit_length)
                length = length_func(i);

            if (is_positive)
            {
                doSubstringUTF8<implicit_length, true>(i, data, offsets, original_start_abs, length, res_data, res_offsets, prev_offset, res_offset);
            }
            else
            {
                doSubstringUTF8<implicit_length, false>(i, data, offsets, original_start_abs, length, res_data, res_offsets, prev_offset, res_offset);
            }
        }
    }

private:
    template <bool implicit_length, bool is_positive_start>
    static void doSubstringUTF8(
        size_t column_index,
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        size_t original_start_abs,
        size_t length,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets,
        ColumnString::Offset & prev_offset,
        ColumnString::Offset & res_offset)
    {
        ColumnString::Offset j = prev_offset;
        ColumnString::Offset pos = 1;
        ColumnString::Offset bytes_start = 0;
        ColumnString::Offset bytes_length = 0;
        size_t start = 0;
        if constexpr (is_positive_start)
            start = original_start_abs;
        else
        {
            // set the start as string_length - abs(original_start) + 1
            std::vector<ColumnString::Offset> start_offsets;
            ColumnString::Offset current = prev_offset;
            while (current < offsets[column_index] - 1)
            {
                start_offsets.push_back(current);
                current += UTF8::seqLength(data[current]);
            }
            if (original_start_abs > start_offsets.size())
            {
                // return empty string
                res_data.resize(res_data.size() + 1);
                res_data[res_offset] = 0;
                ++res_offset;
                res_offsets[column_index] = res_offset;
                return;
            }
            start = start_offsets.size() - original_start_abs + 1;
            pos = start;
            j = start_offsets[start - 1];
        }
        while (j < offsets[column_index] - 1)
        {
            if (pos == start)
                bytes_start = j - prev_offset + 1;

            j += UTF8::seqLength(data[j]);

            if constexpr (implicit_length)
            {
                // implicit_length means get the substring from start to the end of the string
                bytes_length = j - prev_offset + 1 - bytes_start;
            }
            else
            {
                if (pos >= start)
                {
                    if (pos - start < length)
                        bytes_length = j - prev_offset + 1 - bytes_start;
                    else
                        break;
                }
            }

            ++pos;
        }

        if (bytes_start == 0)
        {
            res_data.resize(res_data.size() + 1);
            res_data[res_offset] = 0;
            ++res_offset;
        }
        else
        {
            size_t bytes_to_copy = std::min(offsets[column_index] - prev_offset - bytes_start, bytes_length);
            res_data.resize(res_data.size() + bytes_to_copy + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[prev_offset + bytes_start - 1], bytes_to_copy);
            res_offset += bytes_to_copy + 1;
            res_data[res_offset - 1] = 0;
        }
        res_offsets[column_index] = res_offset;
        prev_offset = offsets[column_index];
    }
};


/** If the string is encoded in UTF-8, then it selects a right of code points in it.
  * Otherwise, the behavior is undefined.
  */
struct RightUTF8Impl
{
public:
    template <typename FF>
    static void constVector(
        const size_t const_length_size,
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        FF && get_length_func,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(const_length_size * data.size());
        res_offsets.resize(const_length_size);

        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < const_length_size; ++i)
        {
            size_t length = get_length_func(i);
            res_offset += (0 == length
                               ? appendEmptyString(res_data, res_offset)
                               : doRightUTF8(data, 0, offsets[0], length, res_data, res_offset));
            res_offsets[i] = res_offset;
        }
    }

    // length should not be zero.
    static void vectorConst(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        size_t length,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res_offset += doRightUTF8(data, prev_offset, offsets[i], length, res_data, res_offset);
            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }

    template <typename FF>
    static void vectorVector(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        FF && get_length_func,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t length = get_length_func(i);
            res_offset += (0 == length
                               ? appendEmptyString(res_data, res_offset)
                               : doRightUTF8(data, prev_offset, offsets[i], length, res_data, res_offset));
            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }

private:
    // length should not be zero.
    // copy bytes from data to res_data and return bytes_to_copy.
    static size_t doRightUTF8(
        const ColumnString::Chars_t & data,
        const ColumnString::Offset & start_offset,
        const ColumnString::Offset & end_offset,
        size_t length,
        ColumnString::Chars_t & res_data,
        const ColumnString::Offset & res_offset)
    {
        std::vector<ColumnString::Offset> start_offsets;
        ColumnString::Offset current = start_offset;
        // TODO: break this loop in advance
        // NOTE: data[end_offset -1] = 0, so ignore it
        auto end_flag = end_offset - 1;
        while (current < end_flag)
        {
            start_offsets.push_back(current);
            current += UTF8::seqLength(data[current]);
        }
        if (start_offsets.empty())
        {
            // null
            return appendEmptyString(res_data, res_offset);
        }
        else
        {
            // not null
            // if(string_length > length, string_length - length, 0)
            auto start_index = start_offsets.size() > length ? start_offsets.size() - length : 0;
            // copy data from start to end of this string
            size_t bytes_to_copy = end_offset - start_offsets[start_index];
            res_data.resize(res_data.size() + bytes_to_copy);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[start_offsets[start_index]], bytes_to_copy);
            return bytes_to_copy;
        }
    }

    static size_t appendEmptyString(ColumnString::Chars_t & res_data, const ColumnString::Offset & res_offset)
    {
        res_data.resize(res_data.size() + 1);
        res_data[res_offset] = 0;
        return 1;
    }
};


template <typename Impl, typename Name, typename ResultType>
class FunctionStringOrArrayToT : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionStringOrArrayToT>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString()
            && !checkDataType<DataTypeArray>(&*arguments[0]))
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());
            Impl::vector(col->getChars(), col->getOffsets(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (Impl::is_fixed_to_constant)
            {
                ResultType res = 0;
                Impl::vectorFixedToConstant(col->getChars(), col->getN(), res);

                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(col->size(), toField(res));
            }
            else
            {
                auto col_res = ColumnVector<ResultType>::create();

                typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
                vec_res.resize(col->size());
                Impl::vectorFixedToVector(col->getChars(), col->getN(), vec_res);

                block.getByPosition(result).column = std::move(col_res);
            }
        }
        else if (const auto * col = checkAndGetColumn<ColumnArray>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());
            Impl::array(col->getOffsets(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", block.getByPosition(arguments[0]).column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Also works with arrays.
class FunctionReverse : public IFunction
{
public:
    static constexpr auto name = "reverse";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionReverse>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isInjective(const Block &) const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString()
            && !checkDataType<DataTypeArray>(&*arguments[0]))
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            ReverseImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(col->getN());
            ReverseImpl::vectorFixed(col->getChars(), col->getN(), col_res->getChars());
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (checkColumn<ColumnArray>(column.get()))
        {
            DefaultExecutable(std::make_shared<FunctionArrayReverse>()).execute(block, arguments, result);
        }
        else
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", block.getByPosition(arguments[0]).column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

extern UInt64 GetJsonLength(std::string_view sv);

class FunctionJsonLength : public IFunction
{
public:
    static constexpr auto name = "jsonLength";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonLength>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnUInt64::create();
            typename ColumnUInt64::Container & vec_col_res = col_res->getData();
            {
                const auto & data = col->getChars();
                const auto & offsets = col->getOffsets();
                const size_t size = offsets.size();
                vec_col_res.resize(size);

                ColumnString::Offset prev_offset = 0;
                for (size_t i = 0; i < size; ++i)
                {
                    std::string_view sv(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1);
                    vec_col_res[i] = GetJsonLength(sv);
                    prev_offset = offsets[i];
                }
            }
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(fmt::format("Illegal column {} of argument of function {}", column->getName(), getName()), ErrorCodes::ILLEGAL_COLUMN);
    }
};

template <typename Name, bool is_injective>
class ConcatImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit ConcatImpl(const Context & context)
        : context(context)
    {}
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<ConcatImpl>(context);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const Block &) const override
    {
        return is_injective;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!is_injective && !arguments.empty() && checkDataType<DataTypeArray>(arguments[0].get()))
            return FunctionArrayConcat(context).getReturnTypeImpl(arguments);

        if (arguments.size() < 2)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto * arg = arguments[arg_idx].get();
            if (!arg->isString())
                throw Exception(
                    fmt::format("Illegal type {} of argument {} of function {}", arg->getName(), arg_idx + 1, getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (!is_injective && !arguments.empty() && checkDataType<DataTypeArray>(block.getByPosition(arguments[0]).type.get()))
            return FunctionArrayConcat(context).executeImpl(block, arguments, result);

        if (arguments.size() == 2)
            executeBinary(block, arguments, result);
        else
            executeNAry(block, arguments, result);
    }

private:
    const Context & context;

    void executeBinary(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const IColumn * c1 = block.getByPosition(arguments[1]).column.get();

        const auto * c0_string = checkAndGetColumn<ColumnString>(c0);
        const auto * c1_string = checkAndGetColumn<ColumnString>(c1);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);
        const ColumnConst * c1_const_string = checkAndGetColumnConst<ColumnString>(c1);

        auto c_res = ColumnString::create();

        if (c0_string && c1_string)
            concat(StringSource(*c0_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else if (c0_string && c1_const_string)
            concat(StringSource(*c0_string), ConstSource<StringSource>(*c1_const_string), StringSink(*c_res, c0->size()));
        else if (c0_const_string && c1_string)
            concat(ConstSource<StringSource>(*c0_const_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else
        {
            /// Fallback: use generic implementation for not very important cases.
            executeNAry(block, arguments, result);
            return;
        }

        block.getByPosition(result).column = std::move(c_res);
    }

    void executeNAry(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        size_t num_sources = arguments.size();
        StringSources sources(num_sources);

        for (size_t i = 0; i < num_sources; ++i)
            sources[i] = createDynamicStringSource(*block.getByPosition(arguments[i]).column);

        auto c_res = ColumnString::create();
        concat(sources, StringSink(*c_res, block.rows()));
        block.getByPosition(result).column = std::move(c_res);
    }
};

/** TiDB Function CONCAT(str1,str2,...)
  * Returns the string that results from concatenating the arguments. May have one or more arguments.
  * CONCAT() returns NULL if any argument is NULL.
*/
class FunctionTiDBConcat : public IFunction
{
private:
    const Context & context;

    struct NameTiDBConcat
    {
        static constexpr auto name = "tidbConcat";
    };

public:
    static constexpr auto name = NameTiDBConcat::name;
    explicit FunctionTiDBConcat(const Context & context)
        : context(context)
    {}
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionTiDBConcat>(context);
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForNulls() const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 1.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto & arg = arguments[arg_idx].get();
            if (!arg->isString())
                throw Exception(
                    fmt::format("Illegal type {} of argument {} of function {}", arg->getName(), (arg_idx + 1), getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (arguments.size() == 1)
        {
            const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
            block.getByPosition(result).column = c0->cloneResized(c0->size());
        }
        else
            return ConcatImpl<NameTiDBConcat, false>(context).executeImpl(block, arguments, result);
    }
};

/** TiDB Function CONCAT_WS(separator,str1,str2,...)
  * CONCAT_WS() stands for Concatenate With Separator and is a special form of CONCAT().
  * The first argument is the separator for the rest of the arguments.
  * If the separator is NULL, the result is NULL.
  * CONCAT_WS() does not skip empty strings. However, it does skip any NULL values after the separator argument.
*/
class FunctionTiDBConcatWithSeparator : public IFunction
{
public:
    static constexpr auto name = "tidbConcatWS";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBConcatWithSeparator>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be at least 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            if (!arguments[arg_idx]->onlyNull())
            {
                const auto * arg = removeNullable(arguments[arg_idx]).get();
                if (!arg->isString())
                    throw Exception(
                        fmt::format("Illegal type {} of argument {} of function {}", arg->getName(), arg_idx + 1, getName()),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        return arguments[0]->onlyNull()
            ? makeNullable(std::make_shared<DataTypeNothing>())
            : makeNullable(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        // if separator is only null, return only null const
        auto & separator_column = block.getByPosition(arguments[0]);
        if (separator_column.type->onlyNull())
        {
            DataTypePtr data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
            block.getByPosition(result).column = data_type->createColumnConst(separator_column.column->size(), Null());
            return;
        }

        Block nested_block = createBlockWithNestedColumns(block, arguments, result);
        ColumnNumbers not_only_null_arguments;
        StringSources sources;

        not_only_null_arguments.push_back(arguments[0]);
        sources.push_back(createDynamicStringSource(*nested_block.getByPosition(not_only_null_arguments[0]).column));
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            auto column_number = arguments[i];
            if (!block.getByPosition(column_number).type->onlyNull())
            {
                not_only_null_arguments.push_back(column_number);
                sources.push_back(createDynamicStringSource(*nested_block.getByPosition(column_number).column));
            }
        }

        size_t rows = block.rows();
        auto result_null_map = ColumnUInt8::create(rows);
        auto res = ColumnString::create();
        StringSink sink(*res, rows);

        for (size_t row = 0; row < rows; ++row)
        {
            if (block.getByPosition(not_only_null_arguments[0]).column->isNullAt(row))
            {
                result_null_map->getData()[row] = true;
            }
            else
            {
                result_null_map->getData()[row] = false;

                bool has_not_null = false;
                for (size_t col = 1; col < not_only_null_arguments.size(); ++col)
                {
                    if (!block.getByPosition(not_only_null_arguments[col]).column->isNullAt(row))
                    {
                        if (has_not_null)
                            writeSlice(sources[0]->getWhole(), sink);
                        else
                            has_not_null = true;
                        writeSlice(sources[col]->getWhole(), sink);
                    }
                }
            }
            for (auto & source : sources)
                source->next();
            sink.next();
        }

        block.getByPosition(result).column = ColumnNullable::create(std::move(res), std::move(result_null_map));
    }
};

class FunctionSubstring : public IFunction
{
public:
    static constexpr auto name = "substring";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSubstring>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 2 or 3", getName(), number_of_arguments),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!arguments[0]->isStringOrFixedString())
            throw Exception(fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isNumber())
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", arguments[1]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (number_of_arguments == 3 && !arguments[2]->isNumber())
            throw Exception(
                fmt::format("Illegal type {} of third argument of function {}", arguments[2]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    template <typename Source>
    void executeForSource(
        const ColumnPtr & column_start,
        const ColumnPtr & column_length,
        const ColumnConst * column_start_const,
        const ColumnConst * column_length_const,
        Int64 start_value,
        Int64 length_value,
        Block & block,
        size_t result,
        Source && source) const
    {
        auto col_res = ColumnString::create();

        if (!column_length)
        {
            if (column_start_const)
            {
                if (start_value > 0)
                    sliceFromLeftConstantOffsetUnbounded(source, StringSink(*col_res, block.rows()), start_value - 1);
                else if (start_value < 0)
                    sliceFromRightConstantOffsetUnbounded(source, StringSink(*col_res, block.rows()), -start_value);
                else
                    throw Exception("Indices in strings are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
                sliceDynamicOffsetUnbounded(source, StringSink(*col_res, block.rows()), *column_start);
        }
        else
        {
            if (column_start_const && column_length_const)
            {
                if (start_value > 0)
                    sliceFromLeftConstantOffsetBounded(source, StringSink(*col_res, block.rows()), start_value - 1, length_value);
                else if (start_value < 0)
                    sliceFromRightConstantOffsetBounded(source, StringSink(*col_res, block.rows()), -start_value, length_value);
                else
                    throw Exception("Indices in strings are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
                sliceDynamicOffsetBounded(source, StringSink(*col_res, block.rows()), *column_start, *column_length);
        }

        block.getByPosition(result).column = std::move(col_res);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        size_t number_of_arguments = arguments.size();

        ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        ColumnPtr column_start = block.getByPosition(arguments[1]).column;
        ColumnPtr column_length;

        if (number_of_arguments == 3)
            column_length = block.getByPosition(arguments[2]).column;

        const auto * column_start_const = checkAndGetColumn<ColumnConst>(column_start.get());
        const ColumnConst * column_length_const = nullptr;

        if (number_of_arguments == 3)
            column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());

        Int64 start_value = 0;
        Int64 length_value = 0;

        if (column_start_const)
        {
            start_value = column_start_const->getInt(0);
        }
        if (column_length_const)
        {
            length_value = column_length_const->getInt(0);
            if (length_value < 0)
                throw Exception("Third argument provided for function substring could not be negative.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        if (const auto * col = checkAndGetColumn<ColumnString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value, length_value, block, result, StringSource(*col));
        else if (const auto * col = checkAndGetColumn<ColumnFixedString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value, length_value, block, result, FixedStringSource(*col));
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value, length_value, block, result, ConstSource<StringSource>(*col));
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnFixedString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value, length_value, block, result, ConstSource<FixedStringSource>(*col));
        else
            throw Exception(
                fmt::format("Illegal column {} of first argument of function {}", block.getByPosition(arguments[0]).column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionSubstringUTF8 : public IFunction
{
public:
    static constexpr auto name = "substringUTF8";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSubstringUTF8>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t arguments_size = arguments.size();
        if (arguments_size != 2 && arguments_size != 3)
            throw Exception(
                fmt::format(
                    "Function {} requires from 2 or 3 parameters: string, start, [length]. Passed {}.",
                    getName(),
                    arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isNumber() || (arguments_size == 3 && !arguments[2]->isNumber()))
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", (arguments[1]->isNumber() ? arguments[2]->getName() : arguments[1]->getName()), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr & column_string = block.getByPosition(arguments[0]).column;

        const ColumnPtr & column_start = block.getByPosition(arguments[1]).column;

        bool implicit_length = (arguments.size() == 2);

        bool is_start_type_valid = getNumberType(block.getByPosition(arguments[1]).type, [&](const auto & start_type, bool) {
            using StartType = std::decay_t<decltype(start_type)>;
            // Int64 / UInt64
            using StartFieldType = typename StartType::FieldType;

            // vector const const
            if (!column_string->isColumnConst() && column_start->isColumnConst() && (implicit_length || block.getByPosition(arguments[2]).column->isColumnConst()))
            {
                auto [is_positive, start_abs] = getValueFromStartField<StartFieldType>((*block.getByPosition(arguments[1]).column)[0]);
                UInt64 length = 0;
                if (!implicit_length)
                {
                    bool is_length_type_valid = getNumberType(block.getByPosition(arguments[2]).type, [&](const auto & length_type, bool) {
                        using LengthType = std::decay_t<decltype(length_type)>;
                        // Int64 / UInt64
                        using LengthFieldType = typename LengthType::FieldType;
                        length = getValueFromLengthField<LengthFieldType>((*block.getByPosition(arguments[2]).column)[0]);
                        return true;
                    });

                    if (!is_length_type_valid)
                        throw Exception(fmt::format("3nd argument of function {} must have UInt/Int type.", getName()));
                }

                // for const zero start or const zero length, return const blank string.
                if (start_abs == 0 || (!implicit_length && length == 0))
                {
                    block.getByPosition(result).column = DataTypeString().createColumnConst(column_string->size(), toField(String("")));
                    return true;
                }

                const auto * col = checkAndGetColumn<ColumnString>(column_string.get());
                assert(col);
                auto col_res = ColumnString::create();
                getVectorConstConstFunc(implicit_length, is_positive)(col->getChars(), col->getOffsets(), start_abs, length, col_res->getChars(), col_res->getOffsets());
                block.getByPosition(result).column = std::move(col_res);
            }
            else // all other cases are converted to vector vector vector
            {
                std::function<std::pair<bool, size_t>(size_t)> get_start_func;
                if (column_start->isColumnConst())
                {
                    // func always return const value
                    auto start_const = getValueFromStartField<StartFieldType>((*column_start)[0]);
                    get_start_func = [start_const](size_t) {
                        return start_const;
                    };
                }
                else
                {
                    get_start_func = [&column_start](size_t i) {
                        return getValueFromStartField<StartFieldType>((*column_start)[i]);
                    };
                }

                // if implicit_length, get_length_func be nil is ok.
                std::function<size_t(size_t)> get_length_func;
                if (!implicit_length)
                {
                    const ColumnPtr & column_length = block.getByPosition(arguments[2]).column;
                    bool is_length_type_valid = getNumberType(block.getByPosition(arguments[2]).type, [&](const auto & length_type, bool) {
                        using LengthType = std::decay_t<decltype(length_type)>;
                        // Int64 / UInt64
                        using LengthFieldType = typename LengthType::FieldType;
                        if (column_length->isColumnConst())
                        {
                            // func always return const value
                            auto length_const = getValueFromLengthField<LengthFieldType>((*column_length)[0]);
                            get_length_func = [length_const](size_t) {
                                return length_const;
                            };
                        }
                        else
                        {
                            get_length_func = [column_length](size_t i) {
                                return getValueFromLengthField<LengthFieldType>((*column_length)[i]);
                            };
                        }
                        return true;
                    });

                    if (!is_length_type_valid)
                        throw Exception(fmt::format("3nd argument of function {} must have UInt/Int type.", getName()));
                }

                // convert to vector if string is const.
                ColumnPtr full_column_string = column_string->isColumnConst() ? column_string->convertToFullColumnIfConst() : column_string;
                const auto * col = checkAndGetColumn<ColumnString>(full_column_string.get());
                assert(col);
                auto col_res = ColumnString::create();
                if (implicit_length)
                {
                    SubstringUTF8Impl::vectorVectorVector<true>(col->getChars(), col->getOffsets(), get_start_func, get_length_func, col_res->getChars(), col_res->getOffsets());
                }
                else
                {
                    SubstringUTF8Impl::vectorVectorVector<false>(col->getChars(), col->getOffsets(), get_start_func, get_length_func, col_res->getChars(), col_res->getOffsets());
                }
                block.getByPosition(result).column = std::move(col_res);
            }

            return true;
        });

        if (!is_start_type_valid)
            throw Exception(fmt::format("2nd argument of function {} must have UInt/Int type.", getName()));
    }

private:
    using VectorConstConstFunc = std::function<void(
        const ColumnString::Chars_t &,
        const ColumnString::Offsets &,
        size_t,
        size_t,
        ColumnString::Chars_t &,
        ColumnString::Offsets &)>;

    static VectorConstConstFunc getVectorConstConstFunc(bool implicit_length, bool is_positive_start)
    {
        if (implicit_length)
        {
            return is_positive_start ? SubstringUTF8Impl::vectorConstConst<true, true> : SubstringUTF8Impl::vectorConstConst<true, false>;
        }
        else
        {
            return is_positive_start ? SubstringUTF8Impl::vectorConstConst<false, true> : SubstringUTF8Impl::vectorConstConst<false, false>;
        }
    }

    template <typename Integer>
    static size_t getValueFromLengthField(const Field & length_field)
    {
        if constexpr (std::is_same_v<Integer, Int64>)
        {
            Int64 signed_length = length_field.get<Int64>();
            return signed_length < 0 ? 0 : signed_length;
        }
        else
        {
            static_assert(std::is_same_v<Integer, UInt64>);
            return length_field.get<UInt64>();
        }
    }

    // return {is_positive, abs}
    template <typename Integer>
    static std::pair<bool, size_t> getValueFromStartField(const Field & start_field)
    {
        if constexpr (std::is_same_v<Integer, Int64>)
        {
            Int64 signed_length = start_field.get<Int64>();

            if (signed_length < 0)
            {
                return {false, static_cast<size_t>(-signed_length)};
            }
            else
            {
                return {true, static_cast<size_t>(signed_length)};
            }
        }
        else
        {
            static_assert(std::is_same_v<Integer, UInt64>);
            return {true, start_field.get<UInt64>()};
        }
    }

    template <typename F>
    static bool getNumberType(DataTypePtr type, F && f)
    {
        return castTypeToEither<
            DataTypeInt64,
            DataTypeUInt64>(type.get(), std::forward<F>(f));
    }
};


class FunctionRightUTF8 : public IFunction
{
public:
    static constexpr auto name = "rightUTF8";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRightUTF8>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of first argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!arguments[1]->isNumber())
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", arguments[1]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        const ColumnPtr column_length = block.getByPosition(arguments[1]).column;

        bool is_length_type_valid = getLengthType(block.getByPosition(arguments[1]).type, [&](const auto & length_type, bool) {
            using LengthType = std::decay_t<decltype(length_type)>;
            // Int64 / UInt64
            using LengthFieldType = typename LengthType::FieldType;

            auto col_res = ColumnString::create();
            if (const auto * col_string = checkAndGetColumn<ColumnString>(column_string.get()))
            {
                if (column_length->isColumnConst())
                {
                    // vector const
                    size_t length = getValueFromLengthField<LengthFieldType>((*column_length)[0]);

                    // for const 0, return const blank string.
                    if (0 == length)
                    {
                        block.getByPosition(result).column = DataTypeString().createColumnConst(column_string->size(), toField(String("")));
                        return true;
                    }

                    RightUTF8Impl::vectorConst(col_string->getChars(), col_string->getOffsets(), length, col_res->getChars(), col_res->getOffsets());
                }
                else
                {
                    // vector vector
                    auto get_length_func = [&column_length](size_t i) {
                        return getValueFromLengthField<LengthFieldType>((*column_length)[i]);
                    };
                    RightUTF8Impl::vectorVector(col_string->getChars(), col_string->getOffsets(), get_length_func, col_res->getChars(), col_res->getOffsets());
                }
            }
            else if (const ColumnConst * col_const_string = checkAndGetColumnConst<ColumnString>(column_string.get()))
            {
                // const vector
                const auto * col_string_from_const = checkAndGetColumn<ColumnString>(col_const_string->getDataColumnPtr().get());
                assert(col_string_from_const);
                // When useDefaultImplementationForConstants is true, string and length are not both constants
                assert(!column_length->isColumnConst());
                auto get_length_func = [&column_length](size_t i) {
                    return getValueFromLengthField<LengthFieldType>((*column_length)[i]);
                };
                RightUTF8Impl::constVector(column_length->size(), col_string_from_const->getChars(), col_string_from_const->getOffsets(), get_length_func, col_res->getChars(), col_res->getOffsets());
            }
            else
            {
                // Impossible to reach here
                return false;
            }
            block.getByPosition(result).column = std::move(col_res);
            return true;
        });

        if (!is_length_type_valid)
            throw Exception(fmt::format("2nd argument of function {} must have UInt/Int type.", getName()));
    }

private:
    template <typename F>
    static bool
    getLengthType(DataTypePtr type, F && f)
    {
        return castTypeToEither<
            DataTypeInt64,
            DataTypeUInt64>(type.get(), std::forward<F>(f));
    }

    template <typename Integer>
    static size_t getValueFromLengthField(const Field & length_field)
    {
        if constexpr (std::is_same_v<Integer, Int64>)
        {
            Int64 signed_length = length_field.get<Int64>();
            return signed_length < 0 ? 0 : signed_length;
        }
        else
        {
            static_assert(std::is_same_v<Integer, UInt64>);
            return length_field.get<UInt64>();
        }
    }
};


class FunctionAppendTrailingCharIfAbsent : public IFunction
{
public:
    static constexpr auto name = "appendTrailingCharIfAbsent";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionAppendTrailingCharIfAbsent>();
    }

    String getName() const override
    {
        return name;
    }


private:
    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[1]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        const auto & column = block.getByPosition(arguments[0]).column;
        const auto & column_char = block.getByPosition(arguments[1]).column;

        if (!checkColumnConst<ColumnString>(column_char.get()))
            throw Exception(fmt::format("Second argument of function {} must be a constant string", getName()), ErrorCodes::ILLEGAL_COLUMN);

        auto trailing_char_str = static_cast<const ColumnConst &>(*column_char).getValue<String>();

        if (trailing_char_str.size() != 1)
            throw Exception(fmt::format("Second argument of function {} must be a one-character string", getName()), ErrorCodes::BAD_ARGUMENTS);

        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            const auto & src_data = col->getChars();
            const auto & src_offsets = col->getOffsets();

            auto & dst_data = col_res->getChars();
            auto & dst_offsets = col_res->getOffsets();

            const auto size = src_offsets.size();
            dst_data.resize(src_data.size() + size);
            dst_offsets.resize(size);

            ColumnString::Offset src_offset{};
            ColumnString::Offset dst_offset{};

            for (const auto i : ext::range(0, size))
            {
                const auto src_length = src_offsets[i] - src_offset;
                memcpySmallAllowReadWriteOverflow15(&dst_data[dst_offset], &src_data[src_offset], src_length);
                src_offset = src_offsets[i];
                dst_offset += src_length;

                if (src_length > 1 && dst_data[dst_offset - 2] != static_cast<unsigned char>(trailing_char_str.front()))
                {
                    dst_data[dst_offset - 1] = trailing_char_str.front();
                    dst_data[dst_offset] = 0;
                    ++dst_offset;
                }

                dst_offsets[i] = dst_offset;
            }

            dst_data.resize_assume_reserved(dst_offset);
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                fmt::format("Illegal column {} of argument of function {}", block.getByPosition(arguments[0]).column->getName(), getName()),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <typename Name, bool ltrim, bool rtrim>
class TrimImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit TrimImpl() = default;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<TrimImpl>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 1 or 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto * arg = arguments[arg_idx].get();
            if (!arg->isString())
                throw Exception(
                    fmt::format("Illegal type {} of argument {} of function {}", arg->getName(), arg_idx + 1, getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (arguments.size() == 1)
            executeTrim(block, arguments, result);
        else if (arguments.size() == 2)
            executeTrimWs(block, arguments, result);
        else
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should beat least 1.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

private:
    void executeTrim(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const auto * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);

        auto c_res = ColumnString::create();

        if (c0_string)
            trim<ltrim, rtrim, StringSource, StringSink>(StringSource(*c0_string), StringSink(*c_res, c0->size()));
        else if (c0_const_string)
            trim<ltrim, rtrim, ConstSource<StringSource>, StringSink>(ConstSource<StringSource>(*c0_const_string), StringSink(*c_res, c0->size()));
        else
            throw Exception(fmt::format("Argument of function {} must be string", getName()), ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(c_res);
    }

    void executeTrimWs(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const IColumn * c1 = block.getByPosition(arguments[1]).column.get();

        const auto * c0_string = checkAndGetColumn<ColumnString>(c0);
        const auto * c1_string = checkAndGetColumn<ColumnString>(c1);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);
        const ColumnConst * c1_const_string = checkAndGetColumnConst<ColumnString>(c1);

        auto c_res = ColumnString::create();

        if (c0_string && c1_string)
            trim<ltrim, rtrim, StringSource, StringSource, StringSink>(StringSource(*c0_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else if (c0_string && c1_const_string)
            trim<ltrim, rtrim, StringSource, ConstSource<StringSource>, StringSink>(StringSource(*c0_string), ConstSource<StringSource>(*c1_const_string), StringSink(*c_res, c0->size()));
        else if (c0_const_string && c1_string)
            trim<ltrim, rtrim, ConstSource<StringSource>, StringSource, StringSink>(ConstSource<StringSource>(*c0_const_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else if (c0_const_string && c1_const_string)
            trim<ltrim, rtrim, ConstSource<StringSource>, ConstSource<StringSource>, StringSink>(ConstSource<StringSource>(*c0_const_string), ConstSource<StringSource>(*c1_const_string), StringSink(*c_res, c0->size()));
        else
            throw Exception(fmt::format("Argument of function {} must be string", getName()), ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(c_res);
    }
};


template <typename Name, bool ltrim, bool rtrim>
class TrimUTF8Impl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit TrimUTF8Impl() = default;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<TrimUTF8Impl>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 1 or 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto * arg = arguments[arg_idx].get();
            if (!arg->isString())
                throw Exception(
                    fmt::format("Illegal type {} of argument {} of function {}", arg->getName(), arg_idx + 1, getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (arguments.size() == 1)
            executeTrim(block, arguments, result);
        else if (arguments.size() == 2)
            executeTrimWs(block, arguments, result);
        else
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should beat least 1.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

private:
    void executeTrim(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const auto * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);

        auto c_res = ColumnString::create();

        if (c0_string)
            vector(c0_string->getChars(), c0_string->getOffsets(), c_res->getChars(), c_res->getOffsets());
        else if (c0_const_string)
        {
            const auto * c0_c_string = checkAndGetColumn<ColumnString>(c0_const_string->getDataColumnPtr().get());
            vector(c0_c_string->getChars(), c0_c_string->getOffsets(), c0_const_string->size(), c_res->getChars(), c_res->getOffsets());
        }
        else
            throw Exception(fmt::format("Argument of function {} must be string", getName()), ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(c_res);
    }

    void executeTrimWs(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const IColumn * c1 = block.getByPosition(arguments[1]).column.get();

        const auto * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);
        const ColumnConst * c1_const_string = checkAndGetColumnConst<ColumnString>(c1);
        const auto * column_trim_string = checkAndGetColumn<ColumnString>(c1_const_string->getDataColumnPtr().get());

        auto c_res = ColumnString::create();

        if (c0_string)
            vectorWS(c0_string->getChars(), c0_string->getOffsets(), column_trim_string->getChars(), column_trim_string->getOffsets(), c_res->getChars(), c_res->getOffsets());
        else if (c0_const_string)
        {
            const auto * c0_c_string = checkAndGetColumn<ColumnString>(c0_const_string->getDataColumnPtr().get());
            vectorWS(c0_c_string->getChars(), c0_c_string->getOffsets(), c0_const_string->size(), column_trim_string->getChars(), column_trim_string->getOffsets(), c_res->getChars(), c_res->getOffsets());
        }
        else
            throw Exception(fmt::format("Argument of function {} must be string", getName()), ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(c_res);
    }

    static void vectorWS(const ColumnString::Chars_t & data,
                         const ColumnString::Offsets & offsets,
                         const ColumnString::Chars_t & trim_data,
                         const ColumnString::Offsets & trim_offsets,
                         ColumnString::Chars_t & res_data,
                         ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len = UTF8::countCodePoints(&data[prev_offset], offsets[i] - prev_offset - 1);
            ColumnString::Offset trim_len = UTF8::countCodePoints(&trim_data[0], trim_offsets[0] - 1);

            ColumnString::Offset per_offset = 0;
            ColumnString::Offset per_end_offset = offsets[i] - 1 - prev_offset;

            size_t start = 0, end = 0;

            if (ltrim)
            {
                for (start = 0; start < len; ++start)
                {
                    size_t bytes, trim_bytes;

                    if (data[prev_offset + per_offset] < 0xBF)
                        bytes = 1;
                    else if (data[prev_offset + per_offset] < 0xE0)
                        bytes = 2;
                    else if (data[prev_offset + per_offset] < 0xF0)
                        bytes = 3;
                    else
                        bytes = 1;

                    ColumnString::Offset per_trim_offset = 0;
                    size_t trim_start;
                    for (trim_start = 0; trim_start < trim_len; ++trim_start)
                    {
                        if (trim_data[per_trim_offset] < 0xBF)
                            trim_bytes = 1;
                        else if (trim_data[per_trim_offset] < 0xE0)
                            trim_bytes = 2;
                        else if (trim_data[per_trim_offset] < 0xF0)
                            trim_bytes = 3;
                        else
                            trim_bytes = 1;

                        if (bytes == trim_bytes && memcmp(&trim_data[per_trim_offset], &data[prev_offset + per_offset], bytes) == 0)
                        {
                            break;
                        }
                        else
                        {
                            per_trim_offset += trim_bytes;
                        }
                    }
                    if (trim_start == trim_len)
                    {
                        /// not in the exclude set
                        break;
                    }
                    else
                    {
                        per_offset += bytes;
                    }
                }
            }

            if (rtrim)
            {
                for (end = len - 1; end >= start; --end)
                {
                    size_t trim_bytes = 0;

                    ColumnString::Offset per_trim_offset = 0;
                    size_t trim_start;
                    for (trim_start = 0; trim_start < trim_len; ++trim_start)
                    {
                        if (trim_data[per_trim_offset] < 0xBF)
                            trim_bytes = 1;
                        else if (trim_data[per_trim_offset] < 0xE0)
                            trim_bytes = 2;
                        else if (trim_data[per_trim_offset] < 0xF0)
                            trim_bytes = 3;
                        else
                            trim_bytes = 1;

                        if (memcmp(&trim_data[per_trim_offset], &data[prev_offset + per_end_offset - trim_bytes], trim_bytes) == 0)
                        {
                            break;
                        }
                        else
                        {
                            per_trim_offset += trim_bytes;
                        }
                    }
                    if (trim_start == trim_len)
                    {
                        /// not in the exclude set
                        break;
                    }
                    else
                    {
                        per_end_offset -= trim_bytes;
                    }
                }
            }

            if (per_end_offset > per_offset)
            {
                memcpy(&res_data[res_offset], &data[prev_offset + per_offset], per_end_offset - per_offset);
                res_offset += per_end_offset - per_offset;
            }
            res_data[res_offset] = 0;
            ++res_offset;

            prev_offset = offsets[i];
            res_offsets[i] = res_offset;
        }
    }

    static void vectorWS(const ColumnString::Chars_t & data,
                         const ColumnString::Offsets & offsets,
                         size_t size,
                         const ColumnString::Chars_t & trim_data,
                         const ColumnString::Offsets & trim_offsets,
                         ColumnString::Chars_t & res_data,
                         ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size() * size);
        res_offsets.resize(size);

        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len = UTF8::countCodePoints(&data[0], offsets[0] - 1);
            ColumnString::Offset trim_len = UTF8::countCodePoints(&trim_data[0], trim_offsets[0] - 1);

            ColumnString::Offset per_offset = 0;
            ColumnString::Offset per_end_offset = offsets[0] - 1;

            size_t start = 0, end = 0;

            if (ltrim)
            {
                for (start = 0; start < len; ++start)
                {
                    size_t bytes, trim_bytes;

                    if (data[per_offset] < 0xBF)
                        bytes = 1;
                    else if (data[per_offset] < 0xE0)
                        bytes = 2;
                    else if (data[per_offset] < 0xF0)
                        bytes = 3;
                    else
                        bytes = 1;

                    ColumnString::Offset per_trim_offset = 0;
                    size_t trim_start;
                    for (trim_start = 0; trim_start < trim_len; ++trim_start)
                    {
                        if (trim_data[per_trim_offset] < 0xBF)
                            trim_bytes = 1;
                        else if (trim_data[per_trim_offset] < 0xE0)
                            trim_bytes = 2;
                        else if (trim_data[per_trim_offset] < 0xF0)
                            trim_bytes = 3;
                        else
                            trim_bytes = 1;

                        if (bytes == trim_bytes && memcmp(&trim_data[per_trim_offset], &data[per_offset], bytes) == 0)
                        {
                            break;
                        }
                        else
                        {
                            per_trim_offset += trim_bytes;
                        }
                    }
                    if (trim_start == trim_len)
                    {
                        /// not in the exclude set
                        break;
                    }
                    else
                    {
                        per_offset += bytes;
                    }
                }
            }

            if (rtrim)
            {
                for (end = len - 1; end >= start; --end)
                {
                    size_t trim_bytes = 0;

                    ColumnString::Offset per_trim_offset = 0;
                    size_t trim_start;
                    for (trim_start = 0; trim_start < trim_len; ++trim_start)
                    {
                        if (trim_data[per_trim_offset] < 0xBF)
                            trim_bytes = 1;
                        else if (trim_data[per_trim_offset] < 0xE0)
                            trim_bytes = 2;
                        else if (trim_data[per_trim_offset] < 0xF0)
                            trim_bytes = 3;
                        else
                            trim_bytes = 1;

                        if (memcmp(&trim_data[per_trim_offset], &data[per_end_offset - trim_bytes], trim_bytes) == 0)
                        {
                            break;
                        }
                        else
                        {
                            per_trim_offset += trim_bytes;
                        }
                    }
                    if (trim_start == trim_len)
                    {
                        /// not in the exclude set
                        break;
                    }
                    else
                    {
                        per_end_offset -= trim_bytes;
                    }
                }
            }

            if (per_end_offset > per_offset)
            {
                memcpy(&res_data[res_offset], &data[per_offset], per_end_offset - per_offset);
                res_offset += per_end_offset - per_offset;
            }
            res_data[res_offset] = 0;
            ++res_offset;

            res_offsets[i] = res_offset;
        }
    }

    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len;
            len = UTF8::countCodePoints(&data[prev_offset], offsets[i] - prev_offset - 1);

            ColumnString::Offset per_offset = 0;
            ColumnString::Offset per_end_offset;
            per_end_offset = offsets[i] - 1 - prev_offset;

            size_t start = 0, end = 0;

            if (ltrim)
            {
                for (start = 0; start < len; ++start)
                {
                    size_t bytes;

                    if (data[prev_offset + per_offset] < 0xBF)
                        bytes = 1;
                    else if (data[prev_offset + per_offset] < 0xE0)
                        bytes = 2;
                    else if (data[prev_offset + per_offset] < 0xF0)
                        bytes = 3;
                    else
                        bytes = 1;

                    if (bytes != 1 || memcmp(" ", &data[prev_offset + per_offset], bytes) != 0)
                    {
                        break;
                    }

                    per_offset += bytes;
                }
            }

            if (rtrim)
            {
                for (end = len - 1; end >= start; --end)
                {
                    if (memcmp(" ", &data[prev_offset + per_end_offset - 1], 1) != 0)
                    {
                        break;
                    }
                    else
                    {
                        per_end_offset -= 1;
                    }
                }
            }

            if (per_end_offset > per_offset)
            {
                memcpy(&res_data[res_offset], &data[prev_offset + per_offset], per_end_offset - per_offset);
                res_offset += per_end_offset - per_offset;
            }
            res_data[res_offset] = 0;
            ++res_offset;

            prev_offset = offsets[i];

            res_offsets[i] = res_offset;
        }
    }

    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       size_t size, /// num of rows
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size() * size);
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len;
            len = UTF8::countCodePoints(&data[prev_offset], offsets[0] - 1);

            ColumnString::Offset per_offset = 0;
            ColumnString::Offset per_end_offset;
            per_end_offset = offsets[0] - 1;

            size_t start = 0, end = 0;

            if (ltrim)
            {
                for (start = 0; start < len; ++start)
                {
                    size_t bytes;

                    if (data[prev_offset + per_offset] < 0xBF)
                        bytes = 1;
                    else if (data[prev_offset + per_offset] < 0xE0)
                        bytes = 2;
                    else if (data[prev_offset + per_offset] < 0xF0)
                        bytes = 3;
                    else
                        bytes = 1;

                    if (bytes != 1 || memcmp(" ", &data[prev_offset + per_offset], bytes) != 0)
                    {
                        break;
                    }

                    per_offset += bytes;
                }
            }

            if (rtrim)
            {
                for (end = len - 1; end >= start; --end)
                {
                    if (memcmp(" ", &data[prev_offset + per_end_offset - 1], 1) != 0)
                    {
                        break;
                    }
                    else
                    {
                        per_end_offset -= 1;
                    }
                }
            }

            if (per_end_offset > per_offset)
            {
                memcpy(&res_data[res_offset], &data[prev_offset + per_offset], per_end_offset - per_offset);
                res_offset += per_end_offset - per_offset;
            }
            res_data[res_offset] = 0;
            ++res_offset;

            prev_offset = 0;

            res_offsets[i] = res_offset;
        }
    }
};

template <typename Name, bool ltrim, bool rtrim>
class FunctionTiDBTrim : public IFunction
{
public:
    static constexpr auto name = Name::name;
    FunctionTiDBTrim() = default;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionTiDBTrim>();
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 3)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 1, 2 or 3.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto * arg = arguments[arg_idx].get();
            if (arg_idx < 2 && !arg->isString())
                throw Exception(
                    fmt::format("Illegal type {} of argument {} of function {}", arg->getName(), arg_idx + 1, getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            else if (arg_idx == 2 && !arg->isInteger())
                throw Exception(
                    fmt::format("Illegal type {} of argument 3 of function {}", arg->getName(), getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        switch (arguments.size())
        {
        case 1:
            executeTrim(block, arguments, result);
            break;
        case 2:
            executeTrim2Args(ltrim, rtrim, block, arguments, result);
            break;
        case 3:
            executeTrim3Args(block, arguments, result);
            break;
        default:
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should beat least 1.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
    }

private:
    void executeTrim(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        ColumnPtr & column_data = block.getByPosition(arguments[0]).column;
        auto res_col = ColumnString::create();

        const auto * data_col = checkAndGetColumn<ColumnString>(column_data.get());

        static constexpr std::string_view default_rem = " ";
        static const auto * remstr_ptr = reinterpret_cast<const UInt8 *>(default_rem.data());
        vectorConst(ltrim, rtrim, data_col->getChars(), data_col->getOffsets(), remstr_ptr, default_rem.size() + 1, res_col->getChars(), res_col->getOffsets());

        block.getByPosition(result).column = std::move(res_col);
    }

    void executeTrim2Args(bool is_ltrim, bool is_rtrim, Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        ColumnPtr & column_data = block.getByPosition(arguments[0]).column;
        ColumnPtr & column_remstr = block.getByPosition(arguments[1]).column;

        bool data_const = column_data->isColumnConst();
        bool remstr_const = column_remstr->isColumnConst();

        auto res_col = ColumnString::create();

        if (data_const && !remstr_const)
        {
            const ColumnConst * data_col = checkAndGetColumnConst<ColumnString>(column_data.get());
            const auto * remstr_col = checkAndGetColumn<ColumnString>(column_remstr.get());

            const auto data = data_col->getValue<String>();
            const auto * data_ptr = reinterpret_cast<const UInt8 *>(data.c_str());
            constVector(is_ltrim, is_rtrim, data_ptr, data.size() + 1, remstr_col->getChars(), remstr_col->getOffsets(), res_col->getChars(), res_col->getOffsets());
        }
        else if (remstr_const && !data_const)
        {
            const ColumnConst * remstr_col = checkAndGetColumnConst<ColumnString>(column_remstr.get());
            const auto * data_col = checkAndGetColumn<ColumnString>(column_data.get());

            const auto remstr = remstr_col->getValue<String>();
            const auto * remstr_ptr = reinterpret_cast<const UInt8 *>(remstr.c_str());
            vectorConst(is_ltrim, is_rtrim, data_col->getChars(), data_col->getOffsets(), remstr_ptr, remstr.size() + 1, res_col->getChars(), res_col->getOffsets());
        }
        else
        {
            const auto * data_col = checkAndGetColumn<ColumnString>(column_data.get());
            const auto * remstr_col = checkAndGetColumn<ColumnString>(column_remstr.get());

            vectorVector(is_ltrim, is_rtrim, data_col->getChars(), data_col->getOffsets(), remstr_col->getChars(), remstr_col->getOffsets(), res_col->getChars(), res_col->getOffsets());
        }

        block.getByPosition(result).column = std::move(res_col);
    }

    void executeTrim3Args(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        ColumnPtr & column_direction = block.getByPosition(arguments[2]).column;
        if (!column_direction->isColumnConst())
            throw Exception(fmt::format("3nd argument of function {} must be constant.", getName()));
        const auto * direction_col = checkAndGetColumn<ColumnConst>(column_direction.get());

        static constexpr Int64 trim_both_default = 0; // trims from both direction by default
        static constexpr Int64 trim_both = 1; // trims from both direction with explicit notation
        static constexpr Int64 trim_leading = 2; // trims from left
        static constexpr Int64 trim_trailing = 3; // trims from right
        Int64 direction = direction_col->getInt(0);
        switch (direction)
        {
        case trim_both_default:
        case trim_both:
            executeTrim2Args(true, true, block, ColumnNumbers(arguments.begin(), arguments.end() - 1), result);
            break;
        case trim_leading:
            executeTrim2Args(true, false, block, ColumnNumbers(arguments.begin(), arguments.end() - 1), result);
            break;
        case trim_trailing:
            executeTrim2Args(false, true, block, ColumnNumbers(arguments.begin(), arguments.end() - 1), result);
            break;
        }
    }

    static void trim(
        const UInt8 * data_begin,
        const size_t data_size,
        const UInt8 * remstr,
        const size_t remstr_size,
        const bool is_ltrim,
        const bool is_rtrim,
        ColumnString::Chars_t & res_data,
        ColumnString::Offset & res_offset)
    {
        const UInt8 * left = data_begin;
        const UInt8 * right = data_begin + data_size - 1;
        const Int64 remstr_real_size = remstr_size - 1;

        if (remstr_real_size > 0 && is_ltrim)
        {
            for (; right - left >= remstr_real_size; left += remstr_real_size)
            {
                if (memcmp(left, remstr, remstr_real_size) != 0)
                {
                    break;
                }
            }
        }
        if (remstr_real_size > 0 && is_rtrim)
        {
            for (; right - left >= remstr_real_size; right -= remstr_real_size)
            {
                if (memcmp(right - remstr_real_size, remstr, remstr_real_size) != 0)
                {
                    break;
                }
            }
        }
        if (right - left >= 0)
        {
            copyDataToResult(res_data, res_offset, left, right);
        }
    }

    // trim(column), trim(const from column)
    static void vectorConst(
        bool is_ltrim,
        bool is_rtrim,
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const UInt8 * remstr,
        const size_t remstr_size,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto data_offset = StringUtil::offsetAt(offsets, i);
            auto data_size = StringUtil::sizeAt(offsets, i);
            trim(&data[data_offset], data_size, remstr, remstr_size, is_ltrim, is_rtrim, res_data, res_offset);
            res_offsets[i] = res_offset;
        }
    }

    // trim(column from const)
    static void constVector(
        bool is_ltrim,
        bool is_rtrim,
        const UInt8 * data,
        const size_t data_size,
        const ColumnString::Chars_t & remstr,
        const ColumnString::Offsets & remstr_offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(remstr.size());
        size_t size = remstr_offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto rem_offset = StringUtil::offsetAt(remstr_offsets, i);
            auto remstr_size = StringUtil::sizeAt(remstr_offsets, i);
            trim(data, data_size, &remstr[rem_offset], remstr_size, is_ltrim, is_rtrim, res_data, res_offset);
            res_offsets[i] = res_offset;
        }
    }

    // trim(column from column)
    static void vectorVector(
        bool is_ltrim,
        bool is_rtrim,
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const ColumnString::Chars_t & remstr,
        const ColumnString::Offsets & remstr_offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto data_offset = StringUtil::offsetAt(offsets, i);
            auto data_size = StringUtil::sizeAt(offsets, i);
            auto rem_offset = StringUtil::offsetAt(remstr_offsets, i);
            auto remstr_size = StringUtil::sizeAt(remstr_offsets, i);
            trim(&data[data_offset], data_size, &remstr[rem_offset], remstr_size, is_ltrim, is_rtrim, res_data, res_offset);
            res_offsets[i] = res_offset;
        }
    }

    static void copyDataToResult(
        ColumnString::Chars_t & res_data,
        ColumnString::Offset & res_offset,
        const UInt8 * begin,
        const UInt8 * end)
    {
        res_data.resize(res_data.size() + (end - begin + 1));
        memcpy(&res_data[res_offset], begin, end - begin);
        res_data[res_offset + (end - begin)] = '\0';
        res_offset += end - begin + 1;
    }
};

class TidbPadImpl
{
public:
    template <typename IntType, bool IsUTF8, bool IsLeft>
    static void tidbExecutePadImpl(Block & block, const ColumnNumbers & arguments, const size_t result, const String & func_name)
    {
        bool has_nullable = false;
        bool has_null_constant = false;
        for (const auto & arg : arguments)
        {
            const auto & ele = block.getByPosition(arg);
            has_nullable |= ele.type->isNullable();
            has_null_constant |= ele.type->onlyNull();
        }

        if (has_null_constant)
        {
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
            return;
        }

        ColumnPtr column_string_ptr = block.getByPosition(arguments[0]).column;
        ColumnPtr column_length_ptr = block.getByPosition(arguments[1]).column;
        ColumnPtr column_padding_ptr = block.getByPosition(arguments[2]).column;
        const bool is_string_const = column_string_ptr->isColumnConst();
        const bool is_length_const = column_length_ptr->isColumnConst();
        const bool is_padding_const = column_padding_ptr->isColumnConst();

        size_t size = block.rows();
        auto result_null_map = ColumnUInt8::create(size, 0);
        ColumnUInt8::Container & vec_result_null_map = result_null_map->getData();

        if (has_nullable)
        {
            for (size_t i = 0; i < size; ++i)
            {
                vec_result_null_map[i] = (column_string_ptr->isNullAt(i) || column_length_ptr->isNullAt(i) || column_padding_ptr->isNullAt(i));
            }
            Block tmp_block = createBlockWithNestedColumns(block, arguments, result);
            column_string_ptr = tmp_block.getByPosition(arguments[0]).column;
            column_length_ptr = tmp_block.getByPosition(arguments[1]).column;
            column_padding_ptr = tmp_block.getByPosition(arguments[2]).column;
        }

        // Compute byte length of result so we can reserve enough memory.
        // It's just a hint, because length UTF8 chars vary from 1 to 4.
        size_t res_byte_len = 0;
        const ColumnVector<IntType> * column_length = nullptr;
        IntType target_len = 0;
        if (is_length_const)
        {
            const ColumnConst * tmp_column = checkAndGetColumnConst<ColumnVector<IntType>>(column_length_ptr.get());
            target_len = tmp_column->getInt(0);
            res_byte_len = target_len * size + size;
        }
        else
        {
            column_length = checkAndGetColumn<ColumnVector<IntType>>(column_length_ptr.get());
            if (column_length == nullptr)
            {
                throw Exception(fmt::format("the second argument type of {} is invalid", func_name), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
            for (size_t i = 0; i < size; i++)
            {
                if (vec_result_null_map[i])
                {
                    continue;
                }
                auto len = static_cast<int32_t>(column_length->getInt(i));
                if (len <= 0)
                {
                    len = 0;
                }
                res_byte_len += len;
            }
            res_byte_len += size;
        }

        auto column_result = ColumnString::create();
        ColumnString::Offsets & result_offsets = column_result->getOffsets();
        ColumnString::Chars_t & result_data = column_result->getChars();
        result_offsets.resize(size);
        result_data.reserve(res_byte_len);

        const ColumnString * column_padding = nullptr;
        const ColumnString::Offsets * padding_offsets = nullptr;
        const ColumnString::Chars_t * padding_data = nullptr;
        size_t padding_size = 0;
        const UInt8 * padding = nullptr;
        // prepare objects related to padding_str to avoid duplicated code.
        if (is_padding_const)
        {
            const ColumnConst * column_padding = checkAndGetColumnConst<ColumnString>(column_padding_ptr.get());
            StringRef padding_str = column_padding->getDataAt(0);
            padding_size = padding_str.size + 1;
            padding = reinterpret_cast<const UInt8 *>(padding_str.data);
        }
        else
        {
            column_padding = checkAndGetColumn<ColumnString>(column_padding_ptr.get());
            padding_offsets = &(column_padding->getOffsets());
            padding_data = &(column_padding->getChars());
        }

        if (is_string_const)
        {
            const ColumnConst * column_string = checkAndGetColumnConst<ColumnString>(column_string_ptr.get());
            Field res_field;
            column_string->get(0, res_field);
            String str_val = res_field.get<String>();

            if (is_padding_const && is_length_const)
            {
                constantConstant<IntType, IsUTF8, IsLeft, true>(str_val, column_length, target_len, padding, padding_size, vec_result_null_map, size, result_data, result_offsets);
            }
            else if (is_padding_const && !is_length_const)
            {
                constantConstant<IntType, IsUTF8, IsLeft, false>(str_val, column_length, target_len, padding, padding_size, vec_result_null_map, size, result_data, result_offsets);
            }
            else if (!is_padding_const && is_length_const)
            {
                constantStringVector<IntType, IsUTF8, IsLeft, true>(str_val, column_length, target_len, padding_data, padding_offsets, vec_result_null_map, size, result_data, result_offsets);
            }
            else if (!is_padding_const && !is_length_const)
            {
                constantStringVector<IntType, IsUTF8, IsLeft, false>(str_val, column_length, target_len, padding_data, padding_offsets, vec_result_null_map, size, result_data, result_offsets);
            }
        }
        else
        {
            const auto * column_string = checkAndGetColumn<ColumnString>(column_string_ptr.get());
            const ColumnString::Offsets & string_offsets = column_string->getOffsets();
            const ColumnString::Chars_t & string_data = column_string->getChars();

            if (is_padding_const && is_length_const)
            {
                stringVectorConstant<IntType, IsUTF8, IsLeft, true>(string_data, string_offsets, column_length, target_len, padding, padding_size, vec_result_null_map, size, result_data, result_offsets);
            }
            else if (is_padding_const && !is_length_const)
            {
                stringVectorConstant<IntType, IsUTF8, IsLeft, false>(string_data, string_offsets, column_length, target_len, padding, padding_size, vec_result_null_map, size, result_data, result_offsets);
            }
            else if (!is_padding_const && is_length_const)
            {
                stringVectorStringVector<IntType, IsUTF8, IsLeft, true>(string_data, string_offsets, column_length, target_len, padding_data, padding_offsets, vec_result_null_map, size, result_data, result_offsets);
            }
            else if (!is_padding_const && !is_length_const)
            {
                stringVectorStringVector<IntType, IsUTF8, IsLeft, false>(string_data, string_offsets, column_length, target_len, padding_data, padding_offsets, vec_result_null_map, size, result_data, result_offsets);
            }
        }
        block.getByPosition(result).column = ColumnNullable::create(std::move(column_result), std::move(result_null_map));
    }

    template <typename IntType, bool IsUTF8, bool IsLeft, bool IsLengthConst>
    static void stringVectorStringVector(const ColumnString::Chars_t & string_data, const ColumnString::Offsets & string_offsets, const ColumnVector<IntType> * column_length [[maybe_unused]], IntType target_len, const ColumnString::Chars_t * padding_data, const ColumnString::Offsets * padding_offsets, ColumnUInt8::Container & vec_result_null_map, size_t size, ColumnString::Chars_t & result_data, ColumnString::Offsets & result_offsets)
    {
        ColumnString::Offset string_prev_offset = 0;
        ColumnString::Offset padding_prev_offset = 0;
        ColumnString::Offset res_prev_offset = 0;

        // pad(col_str, length, col_pad)
        for (size_t i = 0; i < size; i++)
        {
            if (!vec_result_null_map[i])
            {
                if constexpr (!IsLengthConst)
                {
                    target_len = column_length->getElement(i);
                }
                if constexpr (IsUTF8)
                {
                    vec_result_null_map[i] = tidbPadOneRowUTF8<IsLeft>(&string_data[string_prev_offset], string_offsets[i] - string_prev_offset, static_cast<int32_t>(target_len), &(*padding_data)[padding_prev_offset], (*padding_offsets)[i] - padding_prev_offset, result_data, res_prev_offset);
                }
                else
                {
                    vec_result_null_map[i] = tidbPadOneRow<IsLeft>(&string_data[string_prev_offset], string_offsets[i] - string_prev_offset, static_cast<int32_t>(target_len), &(*padding_data)[padding_prev_offset], (*padding_offsets)[i] - padding_prev_offset, result_data, res_prev_offset);
                }
            }
            else
            {
                result_data.resize(result_data.size() + 1);
                result_data[res_prev_offset] = '\0';
                res_prev_offset++;
            }

            string_prev_offset = string_offsets[i];
            padding_prev_offset = (*padding_offsets)[i];
            result_offsets[i] = res_prev_offset;
        }
    }

    template <typename IntType, bool IsUTF8, bool IsLeft, bool IsLengthConst>
    static void stringVectorConstant(const ColumnString::Chars_t & string_data, const ColumnString::Offsets & string_offsets, const ColumnVector<IntType> * column_length [[maybe_unused]], IntType target_len, const UInt8 * padding, size_t padding_size, ColumnUInt8::Container & vec_result_null_map, size_t size, ColumnString::Chars_t & result_data, ColumnString::Offsets & result_offsets)
    {
        ColumnString::Offset string_prev_offset = 0;
        ColumnString::Offset res_prev_offset = 0;

        // pad(col_str, length, const_pad)
        for (size_t i = 0; i < size; i++)
        {
            if (!vec_result_null_map[i])
            {
                if constexpr (!IsLengthConst)
                {
                    target_len = column_length->getElement(i);
                }
                if constexpr (IsUTF8)
                {
                    vec_result_null_map[i] = tidbPadOneRowUTF8<IsLeft>(&string_data[string_prev_offset], string_offsets[i] - string_prev_offset, static_cast<int32_t>(target_len), padding, padding_size, result_data, res_prev_offset);
                }
                else
                {
                    vec_result_null_map[i] = tidbPadOneRow<IsLeft>(&string_data[string_prev_offset], string_offsets[i] - string_prev_offset, static_cast<int32_t>(target_len), padding, padding_size, result_data, res_prev_offset);
                }
            }
            else
            {
                result_data.resize(result_data.size() + 1);
                result_data[res_prev_offset] = '\0';
                res_prev_offset++;
            }

            string_prev_offset = string_offsets[i];
            result_offsets[i] = res_prev_offset;
        }
    }

    template <typename IntType, bool IsUTF8, bool IsLeft, bool IsLengthConst>
    static void constantStringVector(const String & str_val, const ColumnVector<IntType> * column_length [[maybe_unused]], IntType target_len, const ColumnString::Chars_t * padding_data, const ColumnString::Offsets * padding_offsets, ColumnUInt8::Container & vec_result_null_map, size_t size, ColumnString::Chars_t & result_data, ColumnString::Offsets & result_offsets)
    {
        ColumnString::Offset padding_prev_offset = 0;
        ColumnString::Offset res_prev_offset = 0;

        // pad(const_str, length, col_pad)
        for (size_t i = 0; i < size; i++)
        {
            if (!vec_result_null_map[i])
            {
                if constexpr (!IsLengthConst)
                {
                    target_len = column_length->getElement(i);
                }
                if constexpr (IsUTF8)
                {
                    vec_result_null_map[i] = tidbPadOneRowUTF8<IsLeft>(reinterpret_cast<const UInt8 *>(str_val.c_str()), str_val.size() + 1, static_cast<int32_t>(target_len), &(*padding_data)[padding_prev_offset], (*padding_offsets)[i] - padding_prev_offset, result_data, res_prev_offset);
                }
                else
                {
                    vec_result_null_map[i] = tidbPadOneRow<IsLeft>(reinterpret_cast<const UInt8 *>(str_val.c_str()), str_val.size() + 1, static_cast<int32_t>(target_len), &(*padding_data)[padding_prev_offset], (*padding_offsets)[i] - padding_prev_offset, result_data, res_prev_offset);
                }
            }
            else
            {
                result_data.resize(result_data.size() + 1);
                result_data[res_prev_offset] = '\0';
                res_prev_offset++;
            }

            padding_prev_offset = (*padding_offsets)[i];
            result_offsets[i] = res_prev_offset;
        }
    }

    template <typename IntType, bool IsUTF8, bool IsLeft, bool IsLengthConst>
    static void constantConstant(const String & str_val, const ColumnVector<IntType> * column_length [[maybe_unused]], IntType target_len, const UInt8 * padding, size_t padding_size, ColumnUInt8::Container & vec_result_null_map, size_t size, ColumnString::Chars_t & result_data, ColumnString::Offsets & result_offsets)
    {
        ColumnString::Offset res_prev_offset = 0;

        // pad(const_str, length, const_pad)
        for (size_t i = 0; i < size; i++)
        {
            if (!vec_result_null_map[i])
            {
                if constexpr (!IsLengthConst)
                {
                    target_len = column_length->getElement(i);
                }
                if constexpr (IsUTF8)
                {
                    vec_result_null_map[i] = tidbPadOneRowUTF8<IsLeft>(reinterpret_cast<const UInt8 *>(str_val.c_str()), str_val.size() + 1, static_cast<int32_t>(target_len), padding, padding_size, result_data, res_prev_offset);
                }
                else
                {
                    vec_result_null_map[i] = tidbPadOneRow<IsLeft>(reinterpret_cast<const UInt8 *>(str_val.c_str()), str_val.size() + 1, static_cast<int32_t>(target_len), padding, padding_size, result_data, res_prev_offset);
                }
            }
            else
            {
                result_data.resize(result_data.size() + 1);
                result_data[res_prev_offset] = '\0';
                res_prev_offset++;
            }

            result_offsets[i] = res_prev_offset;
        }
    }

    // Do padding for one row.
    // data_size/padding_size includes the tailing '\0'.
    // Return true if result is null.
    template <bool IsLeft>
    static bool tidbPadOneRowUTF8(const UInt8 * data, size_t data_size, int32_t target_len, const UInt8 * padding, size_t padding_size, ColumnString::Chars_t & res, ColumnString::Offset & res_offset)
    {
        ColumnString::Offset data_len = UTF8::countCodePoints(data, data_size - 1);
        ColumnString::Offset pad_len = UTF8::countCodePoints(padding, padding_size - 1);

        if (target_len < 0 || (data_len < static_cast<ColumnString::Offset>(target_len) && pad_len == 0))
        {
            return true;
        }

        auto tmp_target_len = static_cast<ColumnString::Offset>(target_len);
        ColumnString::Offset per_pad_offset = 0;
        ColumnString::Offset pad_bytes = 0;
        ColumnString::Offset left = 0;
        if (data_len < tmp_target_len)
        {
            left = tmp_target_len - data_len;
            if constexpr (IsLeft)
            {
                while (left > 0 && pad_len != 0)
                {
                    pad_bytes = UTF8::seqLength(padding[per_pad_offset]);
                    copyResult(res, res_offset, padding, per_pad_offset, pad_bytes);
                    res_offset += pad_bytes;
                    per_pad_offset = (per_pad_offset + pad_bytes) % (padding_size - 1);
                    --left;
                }
                // The tailing '\0' will be handled later.
                copyResult(res, res_offset, data, 0, data_size - 1);
                res_offset += data_size - 1;
            }
            else
            {
                copyResult(res, res_offset, data, 0, data_size - 1);
                res_offset += data_size - 1;

                while (left > 0 && pad_len != 0)
                {
                    pad_bytes = UTF8::seqLength(padding[per_pad_offset]);
                    copyResult(res, res_offset, padding, per_pad_offset, pad_bytes);
                    res_offset += pad_bytes;
                    per_pad_offset = (per_pad_offset + pad_bytes) % (padding_size - 1);
                    --left;
                }
            }
        }
        else
        {
            while (left < tmp_target_len)
            {
                pad_bytes = UTF8::seqLength(data[per_pad_offset]);

                copyResult(res, res_offset, data, per_pad_offset, pad_bytes);
                res_offset += pad_bytes;
                per_pad_offset += pad_bytes;
                ++left;
            }
        }
        // Add trailing zero.
        res.resize(res.size() + 1);
        res[res_offset] = '\0';
        res_offset++;
        return false;
    }

    // Same with tidbPadOneRowUTF8, but handling in byte instead of char.
    template <bool IsLeft>
    static bool tidbPadOneRow(const UInt8 * data, size_t data_size, int32_t target_len, const UInt8 * padding, size_t padding_size, ColumnString::Chars_t & res, ColumnString::Offset & res_offset)
    {
        ColumnString::Offset data_len = data_size - 1;
        ColumnString::Offset pad_len = padding_size - 1;

        if (target_len < 0 || (data_len < static_cast<ColumnString::Offset>(target_len) && pad_len == 0))
        {
            return true;
        }

        auto tmp_target_len = static_cast<ColumnString::Offset>(target_len);
        if (data_len < tmp_target_len)
        {
            ColumnString::Offset left = tmp_target_len - data_len;
            ColumnString::Offset per_pad_offset = 0;
            if (IsLeft)
            {
                while (left >= pad_len && pad_len != 0)
                {
                    copyResult(res, res_offset, padding, 0, pad_len);
                    res_offset += pad_len;
                    left -= pad_len;
                }
                while (left > 0 && pad_len != 0)
                {
                    copyResult(res, res_offset, padding, per_pad_offset, 1);
                    ++per_pad_offset;
                    ++res_offset;
                    --left;
                }
                // The tailing '\0' will be handled later.
                copyResult(res, res_offset, data, 0, data_size - 1);
                res_offset += data_size - 1;
            }
            else
            {
                copyResult(res, res_offset, data, 0, data_size - 1);
                res_offset += data_size - 1;

                while (left >= pad_len && pad_len != 0)
                {
                    copyResult(res, res_offset, padding, 0, pad_len);
                    res_offset += pad_len;
                    left -= pad_len;
                }
                while (left > 0 && pad_len != 0)
                {
                    copyResult(res, res_offset, padding, per_pad_offset, 1);
                    ++per_pad_offset;
                    ++res_offset;
                    --left;
                }
            }
        }
        else
        {
            copyResult(res, res_offset, data, 0, tmp_target_len);
            res_offset += tmp_target_len;
        }
        // Add trailing zero.
        res.resize(res.size() + 1);
        res[res_offset] = '\0';
        res_offset++;
        return false;
    }

    static void copyResult(ColumnString::Chars_t & result_data, size_t dst_offset, const UInt8 * padding, size_t padding_offset, size_t pad_bytes)
    {
        result_data.resize(result_data.size() + pad_bytes);
        memcpy(&result_data[dst_offset], &padding[padding_offset], pad_bytes);
    }
};

template <typename Name, bool is_left>
class PadImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit PadImpl() = default;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<PadImpl>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 3;
    }

    // pad(str, len, padding) return NULL if len(str) < len and len(padding) == 0
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override

    {
        if (!removeNullable(arguments[0])->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!removeNullable(arguments[1])->isInteger())
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", arguments[1]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!removeNullable(arguments[2])->isString())
            throw Exception(
                fmt::format("Illegal type {} of third argument of function {}", arguments[2]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        tidbExecutePad(block, arguments, result);
    }

private:
    void executePad(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        ColumnPtr column_length = block.getByPosition(arguments[1]).column;
        ColumnPtr column_padding = block.getByPosition(arguments[2]).column;

        const auto * column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());
        const ColumnConst * column_padding_const = checkAndGetColumnConst<ColumnString>(column_padding.get());

        Int64 length_value = 0;

        if (column_length_const)
        {
            length_value = column_length_const->getInt(0);
            if (length_value < 0)
                throw Exception(
                    fmt::format("Second argument provided for function {} could not be negative.", getName()),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }
        if (column_padding_const == nullptr)
        {
            throw Exception(fmt::format("Third argument provided for function {} should be literal string.", getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        auto c_res = ColumnString::create();

        if (const auto * col = checkAndGetColumn<ColumnString>(column_string.get()))
            pad<is_left, StringSource, ConstSource<StringSource>, StringSink>(
                StringSource(*col),
                ConstSource<StringSource>(*column_padding_const),
                StringSink(*c_res, col->size()),
                length_value);
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnString>(column_string.get()))
            pad<is_left, ConstSource<StringSource>, ConstSource<StringSource>, StringSink>(
                ConstSource<StringSource>(*col),
                ConstSource<StringSource>(*column_padding_const),
                StringSink(*c_res, col->size()),
                length_value);

        block.getByPosition(result).column = std::move(c_res);
    }

    void tidbExecutePad(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        TypeIndex type_index = removeNullable(block.getByPosition(arguments[1]).type)->getTypeId();
        switch (type_index)
        {
        case TypeIndex::UInt8:
            TidbPadImpl::tidbExecutePadImpl<UInt8, false, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::UInt16:
            TidbPadImpl::tidbExecutePadImpl<UInt16, false, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::UInt32:
            TidbPadImpl::tidbExecutePadImpl<UInt32, false, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::UInt64:
            TidbPadImpl::tidbExecutePadImpl<UInt64, false, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::Int8:
            TidbPadImpl::tidbExecutePadImpl<Int8, false, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::Int16:
            TidbPadImpl::tidbExecutePadImpl<Int16, false, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::Int32:
            TidbPadImpl::tidbExecutePadImpl<Int32, false, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::Int64:
            TidbPadImpl::tidbExecutePadImpl<Int64, false, is_left>(block, arguments, result, getName());
            break;
        default:
            throw Exception(fmt::format("the second argument type of {} is invalid, expect integer, got {}", getName(), type_index), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        };
    }
};

template <typename Name, bool is_left>
class PadUTF8Impl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit PadUTF8Impl() = default;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<PadUTF8Impl>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 3;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!removeNullable(arguments[0])->isString())
            throw Exception(
                fmt::format("Illegal type {} of argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!removeNullable(arguments[1])->isInteger())
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", arguments[1]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!removeNullable(arguments[2])->isString())
            throw Exception(
                fmt::format("Illegal type {} of third argument of function {}", arguments[2]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return makeNullable(std::make_shared<DataTypeString>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        tidbExecutePadUTF8(block, arguments, result);
    }

private:
    void executePadUTF8(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        ColumnPtr column_length = block.getByPosition(arguments[1]).column;
        ColumnPtr column_padding = block.getByPosition(arguments[2]).column;

        const auto * column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());
        const ColumnConst * column_padding_const = checkAndGetColumnConst<ColumnString>(column_padding.get());

        Int64 length_value = 0;

        if (column_length_const)
        {
            length_value = column_length_const->getInt(0);
            if (length_value < 0)
                throw Exception(
                    fmt::format("Second argument provided for function {} could not be negative.", getName()),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }
        if (column_padding_const == nullptr)
        {
            throw Exception(fmt::format("Third argument provided for function {} should be literal string.", getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        auto c_res = ColumnString::create();
        const auto * column_padding_string = checkAndGetColumn<ColumnString>(column_padding_const->getDataColumnPtr().get());
        if (const auto * col = checkAndGetColumn<ColumnString>(column_string.get()))
            vector(col->getChars(), col->getOffsets(), length_value, column_padding_string->getChars(), column_padding_string->getOffsets(), c_res->getChars(), c_res->getOffsets());
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnString>(column_string.get()))
        {
            const auto * col_string = checkAndGetColumn<ColumnString>(col->getDataColumnPtr().get());
            vectorConst(col_string->getChars(),
                        col_string->getOffsets(),
                        col->size(),
                        length_value,
                        column_padding_string->getChars(),
                        column_padding_string->getOffsets(),
                        c_res->getChars(),
                        c_res->getOffsets());
        }

        block.getByPosition(result).column = std::move(c_res);
    }

    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       size_t length,
                       const ColumnString::Chars_t & pad_data,
                       const ColumnString::Offsets & pad_offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        size_t size = offsets.size();
        res_data.reserve(length * 3 * size + size);
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len = UTF8::countCodePoints(&data[prev_offset], offsets[i] - prev_offset - 1);
            ColumnString::Offset pad_len = UTF8::countCodePoints(&pad_data[0], pad_offsets[0] - 1);

            /// if the origin len of input less than the length parameter
            if (len < length)
            {
                size_t left = length - len;
                ColumnString::Offset per_pad_offset = 0;
                if (is_left)
                {
                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    memcpy(&res_data[res_offset], &data[prev_offset], offsets[i] - prev_offset);
                    res_offset += offsets[i] - prev_offset;
                }
                else
                {
                    memcpy(&res_data[res_offset], &data[prev_offset], offsets[i] - prev_offset - 1);
                    res_offset += offsets[i] - prev_offset - 1;

                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    ++res_offset;
                }
            }
            else
            {
                ColumnString::Offset j = prev_offset;

                size_t left = length;
                while (left > 0)
                {
                    /// get length parameter characters
                    ColumnString::Offset pad_bytes;

                    if (data[j] < 0xBF)
                        pad_bytes = 1;
                    else if (data[j] < 0xE0)
                        pad_bytes = 2;
                    else if (data[j] < 0xF0)
                        pad_bytes = 3;
                    else
                        pad_bytes = 1;

                    memcpy(&res_data[res_offset], &data[j], pad_bytes);
                    j += pad_bytes;
                    res_offset += pad_bytes;
                    --left;
                }

                /// including the tailing '\0'
                res_data[res_offset] = 0x0;
                ++res_offset;
            }

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }

    static void vector(const ColumnString::Chars_t & data,
                       size_t fixed_len,
                       size_t size,
                       size_t length,
                       const ColumnString::Chars_t & pad_data,
                       const ColumnString::Offsets & pad_offsets,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(3 * length * size);
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t byte_len = strlen(reinterpret_cast<const char *>(&(data[prev_offset])));
            ColumnString::Offset len = UTF8::countCodePoints(&data[prev_offset], byte_len);
            ColumnString::Offset pad_len = UTF8::countCodePoints(&pad_data[0], pad_offsets[0] - 1);

            /// if the origin len of input less than the length parameter
            if (len < length)
            {
                size_t left = length - len;
                ColumnString::Offset per_pad_offset = 0;
                if (is_left)
                {
                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    memcpy(&res_data[res_offset], &data[prev_offset], byte_len);
                    res_offset += byte_len;

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    res_offset += 1;
                }
                else
                {
                    memcpy(&res_data[res_offset], &data[prev_offset], byte_len);
                    res_offset += byte_len;

                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    ++res_offset;
                }
            }
            else
            {
                ColumnString::Offset j = prev_offset;

                size_t left = length;

                /// get length parameter characters
                while (left > 0)
                {
                    ColumnString::Offset pad_bytes;

                    if (data[j] < 0xBF)
                        pad_bytes = 1;
                    else if (data[j] < 0xE0)
                        pad_bytes = 2;
                    else if (data[j] < 0xF0)
                        pad_bytes = 3;
                    else
                        pad_bytes = 1;

                    memcpy(&res_data[res_offset], &data[j], pad_bytes);
                    j += pad_bytes;
                    res_offset += pad_bytes;
                    --left;
                }

                /// including the tailing '\0'
                res_data[res_offset] = 0x0;
                ++res_offset;
            }

            res_offsets[i] = res_offset;
            prev_offset += fixed_len;
        }
    }

    static void vectorConst(const ColumnString::Chars_t & data,
                            const ColumnString::Offsets & offsets,
                            size_t size, /// number of rows of const column
                            size_t length,
                            const ColumnString::Chars_t & pad_data,
                            const ColumnString::Offsets & pad_offsets,
                            ColumnString::Chars_t & res_data,
                            ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(3 * length * size);
        res_offsets.resize(size);

        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset len = UTF8::countCodePoints(&data[0], offsets[0] - 1);
            ColumnString::Offset pad_len = UTF8::countCodePoints(&pad_data[0], pad_offsets[0] - 1);

            /// if the origin len of input less than the length parameter
            if (len < length)
            {
                size_t left = length - len;
                ColumnString::Offset per_pad_offset = 0;
                if (is_left)
                {
                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    memcpy(&res_data[res_offset], &data[0], offsets[0]);
                    res_offset += offsets[0];
                }
                else
                {
                    memcpy(&res_data[res_offset], &data[0], offsets[0] - 1);
                    res_offset += offsets[0] - 1;

                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    ++res_offset;
                }
            }
            else
            {
                ColumnString::Offset j = 0;

                size_t left = length;
                while (left > 0)
                {
                    /// get length parameter characters
                    ColumnString::Offset pad_bytes;

                    if (data[j] < 0xBF)
                        pad_bytes = 1;
                    else if (data[j] < 0xE0)
                        pad_bytes = 2;
                    else if (data[j] < 0xF0)
                        pad_bytes = 3;
                    else
                        pad_bytes = 1;

                    memcpy(&res_data[res_offset], &data[j], pad_bytes);
                    j += pad_bytes;
                    res_offset += pad_bytes;
                    --left;
                }

                /// including the tailing '\0'
                res_data[res_offset] = 0x0;
                ++res_offset;
            }

            res_offsets[i] = res_offset;
        }
    }

    static void vectorConst(const ColumnString::Chars_t & data,
                            size_t, /// length of fixed colomn
                            size_t size, /// number of row
                            size_t length,
                            const ColumnString::Chars_t & pad_data,
                            const ColumnString::Offsets & pad_offsets,
                            ColumnString::Chars_t & res_data,
                            ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(3 * length * size);
        res_offsets.resize(size);

        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t byte_len = strlen(reinterpret_cast<const char *>(&(data[0])));
            ColumnString::Offset len = UTF8::countCodePoints(&data[0], byte_len);
            ColumnString::Offset pad_len = UTF8::countCodePoints(&pad_data[0], pad_offsets[0] - 1);

            /// if the origin len of input less than the length parameter
            if (len < length)
            {
                size_t left = length - len;
                ColumnString::Offset per_pad_offset = 0;
                if (is_left)
                {
                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    memcpy(&res_data[res_offset], &data[0], byte_len);
                    res_offset += byte_len;

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    res_offset += 1;
                }
                else
                {
                    memcpy(&res_data[res_offset], &data[0], byte_len);
                    res_offset += byte_len;

                    while (left > 0 && pad_len != 0)
                    {
                        /// insert into one utf8 character
                        ColumnString::Offset pad_bytes;

                        if (pad_data[per_pad_offset] < 0xBF)
                            pad_bytes = 1;
                        else if (pad_data[per_pad_offset] < 0xE0)
                            pad_bytes = 2;
                        else if (pad_data[per_pad_offset] < 0xF0)
                            pad_bytes = 3;
                        else
                            pad_bytes = 1;

                        memcpy(&res_data[res_offset], &pad_data[per_pad_offset], pad_bytes);
                        res_offset += pad_bytes;
                        --left;
                        per_pad_offset = (per_pad_offset + pad_bytes) % (pad_offsets[0] - 1);
                    }

                    /// including the tailing '\0'
                    res_data[res_offset] = 0x0;
                    ++res_offset;
                }
            }
            else
            {
                ColumnString::Offset j = 0;

                size_t left = length;

                /// get length parameter characters
                while (left > 0)
                {
                    ColumnString::Offset pad_bytes;

                    if (data[j] < 0xBF)
                        pad_bytes = 1;
                    else if (data[j] < 0xE0)
                        pad_bytes = 2;
                    else if (data[j] < 0xF0)
                        pad_bytes = 3;
                    else
                        pad_bytes = 1;

                    memcpy(&res_data[res_offset], &data[j], pad_bytes);
                    j += pad_bytes;
                    res_offset += pad_bytes;
                    --left;
                }

                /// including the tailing '\0'
                res_data[res_offset] = 0x0;
                ++res_offset;
            }

            res_offsets[i] = res_offset;
        }
    }

    void tidbExecutePadUTF8(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        TypeIndex type_index = removeNullable(block.getByPosition(arguments[1]).type)->getTypeId();
        switch (type_index)
        {
        case TypeIndex::UInt8:
            TidbPadImpl::tidbExecutePadImpl<UInt8, true, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::UInt16:
            TidbPadImpl::tidbExecutePadImpl<UInt16, true, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::UInt32:
            TidbPadImpl::tidbExecutePadImpl<UInt32, true, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::UInt64:
            TidbPadImpl::tidbExecutePadImpl<UInt64, true, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::Int8:
            TidbPadImpl::tidbExecutePadImpl<Int8, true, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::Int16:
            TidbPadImpl::tidbExecutePadImpl<Int16, true, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::Int32:
            TidbPadImpl::tidbExecutePadImpl<Int32, true, is_left>(block, arguments, result, getName());
            break;
        case TypeIndex::Int64:
            TidbPadImpl::tidbExecutePadImpl<Int64, true, is_left>(block, arguments, result, getName());
            break;
        default:
            throw Exception(fmt::format("the second argument type of {} is invalid, expect integer, got {}", getName(), type_index), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        };
    }
};

class FunctionASCII : public IFunction
{
public:
    static constexpr auto name = "ascii";
    FunctionASCII() = default;

    static FunctionPtr create(const Context & /*context*/)
    {
        return std::make_shared<FunctionASCII>();
    }

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (arguments.size() != 1)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 1.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IColumn * c0_col = block.getByPosition(arguments[0]).column.get();
        const auto * c0_string = checkAndGetColumn<ColumnString>(c0_col);
        if unlikely (c0_string == nullptr)
            throw Exception(
                fmt::format("Illegal argument of function {}", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto val_num = static_cast<ssize_t>(c0_col->size());
        auto col_res = ColumnInt64::create();
        ColumnInt64::Container & data = col_res->getData();
        data.resize(val_num);

        const auto & chars = c0_string->getChars();
        const auto & offsets = c0_string->getOffsets();

        for (ssize_t i = 0; i < val_num; i++)
            data[i] = chars[offsets[i - 1]];

        block.getByPosition(result).column = std::move(col_res);
    }
};

class FunctionLength : public IFunction
{
public:
    static constexpr auto name = "length";
    FunctionLength() = default;

    static FunctionPtr create(const Context & /*context*/)
    {
        return std::make_shared<FunctionLength>();
    }

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if unlikely (arguments.size() != 1)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 1.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IColumn * c0_col = block.getByPosition(arguments[0]).column.get();
        const auto * c0_string = checkAndGetColumn<ColumnString>(c0_col);
        if unlikely (c0_string == nullptr)
            throw Exception(
                fmt::format("Illegal argument of function {}", getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto val_num = static_cast<ssize_t>(c0_col->size());
        auto col_res = ColumnInt64::create();
        ColumnInt64::Container & data = col_res->getData();
        data.resize(val_num);

        const auto & offsets = c0_string->getOffsets();

        for (ssize_t i = 0; i < val_num; i++)
            data[i] = offsets[i] - offsets[i - 1] - 1;

        block.getByPosition(result).column = std::move(col_res);
    }

private:
};

class FunctionPosition : public IFunction
{
public:
    static constexpr auto name = "position";
    FunctionPosition() = default;

    static FunctionPtr create(const Context & /*context*/)
    {
        return std::make_shared<FunctionPosition>();
    }

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                fmt::format("Number of arguments for function {} doesn't match: passed {}, should be 2.", getName(), arguments.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const IColumn * c0_col = block.getByPosition(arguments[0]).column.get();
        const auto * c0_const = checkAndGetColumn<ColumnConst>(c0_col);
        const auto * c0_string = checkAndGetColumn<ColumnString>(c0_col);
        Field c0_field;

        const IColumn * c1_col = block.getByPosition(arguments[1]).column.get();
        const auto * c1_const = checkAndGetColumn<ColumnConst>(c1_col);
        const auto * c1_string = checkAndGetColumn<ColumnString>(c1_col);
        Field c1_field;

        if ((c0_const == nullptr && c0_string == nullptr) || (c1_const == nullptr && c1_string == nullptr))
            throw Exception(fmt::format("Illegal argument of function {}", getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (c0_col->size() != c1_col->size())
            throw Exception(fmt::format("Function {} column number is inconformity", getName()), ErrorCodes::LOGICAL_ERROR);

        auto col_res = ColumnInt64::create();
        int val_num = c0_col->size();
        col_res->reserve(val_num);

        for (int i = 0; i < val_num; i++)
        {
            c0_col->get(i, c0_field);
            c1_col->get(i, c1_field);

            String c0_str = c0_field.get<String>();
            String c1_str = c1_field.get<String>();

            // return -1 when c1_str not contains the c0_str
            Int64 idx = c1_str.find(c0_str);
            col_res->insert(getPositionUTF8(c1_str, idx));
        }

        block.getByPosition(result).column = std::move(col_res);
    }

private:
    static Int64 getPositionUTF8(const String & c1_str, Int64 idx)
    {
        if (idx == -1)
            return 0;

        const auto * data = reinterpret_cast<const UInt8 *>(c1_str.data());
        return static_cast<size_t>(UTF8::countCodePoints(data, idx) + 1);
    }
};

class FunctionSubStringIndex : public IFunction
{
public:
    static constexpr auto name = "substringIndex";
    FunctionSubStringIndex() = default;

    static FunctionPtr create(const Context & /*context*/)
    {
        return std::make_shared<FunctionSubStringIndex>();
    }

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                fmt::format("Illegal type {} of first argument of function {}", arguments[0]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!arguments[1]->isString())
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", arguments[1]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!arguments[2]->isInteger())
            throw Exception(
                fmt::format("Illegal type {} of third argument of function {}", arguments[2]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (executeSubStringIndex<UInt8>(block, arguments, result)
            || executeSubStringIndex<UInt16>(block, arguments, result)
            || executeSubStringIndex<UInt32>(block, arguments, result)
            || executeSubStringIndex<UInt64>(block, arguments, result)
            || executeSubStringIndex<Int8>(block, arguments, result)
            || executeSubStringIndex<Int16>(block, arguments, result)
            || executeSubStringIndex<Int32>(block, arguments, result)
            || executeSubStringIndex<Int64>(block, arguments, result))
        {
            return;
        }
        else
        {
            throw Exception(fmt::format("Illegal argument of function {}", getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

private:
    template <typename IntType>
    bool executeSubStringIndex(
        Block & block,
        const ColumnNumbers & arguments,
        const size_t result) const
    {
        ColumnPtr & column_str = block.getByPosition(arguments[0]).column;
        ColumnPtr & column_delim = block.getByPosition(arguments[1]).column;
        ColumnPtr & column_count = block.getByPosition(arguments[2]).column;
        ColumnWithTypeAndName & column_result = block.getByPosition(result);

        bool delim_const = column_delim->isColumnConst();
        bool count_const = column_count->isColumnConst();

        // TODO: differentiate vector and const
        column_str = column_str->isColumnConst() ? column_str->convertToFullColumnIfConst() : column_str;
        if (delim_const && count_const)
        {
            const auto * str_col = checkAndGetColumn<ColumnString>(column_str.get());
            const ColumnConst * delim_col = checkAndGetColumnConst<ColumnString>(column_delim.get());
            const ColumnConst * count_col = checkAndGetColumnConst<ColumnVector<IntType>>(column_count.get());
            if (str_col == nullptr || delim_col == nullptr || count_col == nullptr)
            {
                return false;
            }
            auto col_res = ColumnString::create();
            auto count = count_col->getValue<IntType>();
            vectorConstConst(
                str_col->getChars(),
                str_col->getOffsets(),
                delim_col->getValue<String>(),
                accurate::lessOp(INT64_MAX, count) ? INT64_MAX : count,
                col_res->getChars(),
                col_res->getOffsets());
            column_result.column = std::move(col_res);
        }
        else
        {
            column_delim = column_delim->isColumnConst() ? column_delim->convertToFullColumnIfConst() : column_delim;
            column_count = column_count->isColumnConst() ? column_count->convertToFullColumnIfConst() : column_count;
            const auto * str_col = checkAndGetColumn<ColumnString>(column_str.get());
            const auto * delim_col = checkAndGetColumn<ColumnString>(column_delim.get());
            const auto * count_col = checkAndGetColumn<ColumnVector<IntType>>(column_count.get());
            if (str_col == nullptr || delim_col == nullptr || count_col == nullptr)
            {
                return false;
            }
            auto col_res = ColumnString::create();
            vectorVectorVector(
                str_col->getChars(),
                str_col->getOffsets(),
                delim_col->getChars(),
                delim_col->getOffsets(),
                count_col->getData(),
                col_res->getChars(),
                col_res->getOffsets());
            column_result.column = std::move(col_res);
        }

        return true;
    }

    static void vectorConstConst(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const std::string & delim,
        const Int64 needCount,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_offsets.resize(offsets.size());
        if (delim.empty() || needCount == 0)
        {
            // All result is ""
            res_data.resize(offsets.size());
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                res_data[i] = '\0';
                res_offsets[i] = i + 1;
            }
            return;
        }

        ColumnString::Offset res_offset = 0;
        Volnitsky searcher(delim.c_str(), delim.size(), 0);
        res_data.reserve(data.size());
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            auto data_offset = StringUtil::offsetAt(offsets, i);
            auto data_size = StringUtil::sizeAt(offsets, i) - 1;

            subStringIndex(&data[data_offset], data_size, &searcher, delim.size(), needCount, res_data, res_offset);
            res_offsets[i] = res_offset;
        }
    }

    template <typename IntType>
    static void vectorVectorVector(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const ColumnString::Chars_t & delim_data,
        const ColumnString::Offsets & delim_offsets,
        const PaddedPODArray<IntType> & needCount,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        res_offsets.resize(offsets.size());
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            auto data_offset = StringUtil::offsetAt(offsets, i);
            auto data_size = StringUtil::sizeAt(offsets, i) - 1;
            auto delim_offset = StringUtil::offsetAt(delim_offsets, i);
            auto delim_size = StringUtil::sizeAt(delim_offsets, i) - 1; // ignore the trailing zero.
            Int64 count = accurate::lessOp(INT64_MAX, needCount[i]) ? INT64_MAX : needCount[i];

            if (delim_size == 0 || count == 0)
            {
                res_data.resize(res_data.size() + 1);
                res_data[res_offset] = '\0';
                ++res_offset;
                res_offsets[i] = res_offset;
                continue;
            }
            Volnitsky searcher(reinterpret_cast<const char *>(&delim_data[delim_offset]), delim_size, data_size);
            subStringIndex(&data[data_offset], data_size, &searcher, delim_size, count, res_data, res_offset);
            res_offsets[i] = res_offset;
        }
    }

    static void subStringIndex(
        const UInt8 * data_begin,
        size_t data_size,
        Volnitsky * delim_searcher,
        size_t delim_size,
        Int64 count,
        ColumnString::Chars_t & res_data,
        ColumnString::Offset & res_offset)
    {
        const UInt8 * begin = data_begin;
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data_size;
        assert(delim_size != 0);
        assert(count != 0);
        if (count > 0)
        {
            // Fast exit when count * delim_size > data_size
            if (static_cast<Int64>(data_size / delim_size) < count)
            {
                copyDataToResult(res_data, res_offset, begin, end);
                return;
            }
            while (pos < end)
            {
                const UInt8 * match = delim_searcher->search(pos, end - pos);
                --count;
                if (match == end || count == 0)
                {
                    copyDataToResult(res_data, res_offset, begin, match);
                    return;
                }
                pos = match + delim_size;
            }
            copyDataToResult(res_data, res_offset, begin, end);
        }
        else
        {
            std::vector<const UInt8 *> delim_pos;
            // Fast exit when count * delim_size > data_size, or count == INT64_MIN
            if (count == std::numeric_limits<Int64>::min() || static_cast<Int64>(data_size / delim_size) < -count)
            {
                copyDataToResult(res_data, res_offset, begin, end);
                return;
            }
            count = -count;
            // When count is negative, we need split string by delim.
            while (pos < end)
            {
                const UInt8 * match = delim_searcher->search(pos, end - pos);
                if (match == end)
                {
                    break;
                }
                delim_pos.push_back(match);
                pos = match + delim_size;
            }

            if (count <= static_cast<Int64>(delim_pos.size()))
            {
                auto delim_count = delim_pos.size();
                const UInt8 * match = delim_pos[delim_count - count];
                begin = match + delim_size;
            }
            copyDataToResult(res_data, res_offset, begin, end);
        }
    }

    static void copyDataToResult(
        ColumnString::Chars_t & res_data,
        ColumnString::Offset & res_offset,
        const UInt8 * begin,
        const UInt8 * end)
    {
        res_data.resize(res_data.size() + (end - begin + 1));
        memcpy(&res_data[res_offset], begin, end - begin);
        res_data[res_offset + (end - begin)] = '\0';
        res_offset += end - begin + 1;
    }
};

template <typename Name, typename Format>
class FormatImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit FormatImpl(const Context & context_)
        : context(context_)
    {}

    static FunctionPtr create(const Context & context_)
    {
        return std::make_shared<FormatImpl>(context_);
    }

    String getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto first_argument = arguments[0];
        if (!first_argument->isNumber() && !first_argument->isDecimal())
            throw Exception(
                fmt::format("Illegal type {} of first argument of function {}", first_argument->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isInteger())
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", arguments[1]->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    /// string format(number/decimal, int/uint)
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto & number_base_type = block.getByPosition(arguments[0]).type;
        const auto & precision_base_type = block.getByPosition(arguments[1]).type;

        auto col_res = ColumnString::create();
        auto val_num = block.getByPosition(arguments[0]).column->size();

        bool is_types_valid = getNumberType(number_base_type, [&](const auto & number_type, bool) {
            using NumberType = std::decay_t<decltype(number_type)>;
            using NumberFieldType = typename NumberType::FieldType;
            using NumberColVec = std::conditional_t<IsDecimal<NumberFieldType>, ColumnDecimal<NumberFieldType>, ColumnVector<NumberFieldType>>;
            const auto * number_raw = block.getByPosition(arguments[0]).column.get();

            TiDBDecimalRoundInfo info{number_type, number_type};
            info.output_prec = info.output_prec < 65 ? info.output_prec + 1 : 65;

            return getPrecisionType(precision_base_type, [&](const auto & precision_type, bool) {
                using PrecisionType = std::decay_t<decltype(precision_type)>;
                using PrecisionFieldType = typename PrecisionType::FieldType;
                using PrecisionColVec = ColumnVector<PrecisionFieldType>;
                const auto * precision_raw = block.getByPosition(arguments[1]).column.get();

                if (const auto * col0_const = checkAndGetColumnConst<NumberColVec>(number_raw))
                {
                    const NumberFieldType & const_number = col0_const->template getValue<NumberFieldType>();

                    if (const auto * col1_column = checkAndGetColumn<PrecisionColVec>(precision_raw))
                    {
                        const auto & precision_array = col1_column->getData();
                        for (size_t i = 0; i != val_num; ++i)
                        {
                            size_t max_num_decimals = getMaxNumDecimals(precision_array[i]);
                            format(const_number, max_num_decimals, info, col_res->getChars(), col_res->getOffsets());
                        }
                    }
                    else
                        return false;
                }
                else if (const auto * col0_column = checkAndGetColumn<NumberColVec>(number_raw))
                {
                    if (const auto * col1_const = checkAndGetColumnConst<PrecisionColVec>(precision_raw))
                    {
                        size_t max_num_decimals = getMaxNumDecimals(col1_const->template getValue<PrecisionFieldType>());
                        for (const auto & number : col0_column->getData())
                            format(number, max_num_decimals, info, col_res->getChars(), col_res->getOffsets());
                    }
                    else if (const auto * col1_column = checkAndGetColumn<PrecisionColVec>(precision_raw))
                    {
                        const auto & number_array = col0_column->getData();
                        const auto & precision_array = col1_column->getData();
                        for (size_t i = 0; i != val_num; ++i)
                        {
                            size_t max_num_decimals = getMaxNumDecimals(precision_array[i]);
                            format(number_array[i], max_num_decimals, info, col_res->getChars(), col_res->getOffsets());
                        }
                    }
                    else
                        return false;
                }
                else
                    return false;

                block.getByPosition(result).column = std::move(col_res);
                return true;
            });
        });

        if (!is_types_valid)
            throw Exception(
                fmt::format("Illegal types {}, {} arguments of function {}", number_base_type->getName(), precision_base_type->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    const Context & context;

    /// format_max_decimals limits the maximum number of decimal digits for result of
    /// function `format`, this value is same as `FORMAT_MAX_DECIMALS` in MySQL source code.
    static constexpr size_t format_max_decimals = 30;

    template <typename F>
    static bool getNumberType(DataTypePtr type, F && f)
    {
        return castTypeToEither<
            DataTypeDecimal32,
            DataTypeDecimal64,
            DataTypeDecimal128,
            DataTypeDecimal256,
            DataTypeFloat32,
            DataTypeFloat64,
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64>(type.get(), std::forward<F>(f));
    }

    template <typename F>
    static bool getPrecisionType(DataTypePtr type, F && f)
    {
        return castTypeToEither<
            DataTypeInt8,
            DataTypeInt16,
            DataTypeInt32,
            DataTypeInt64,
            DataTypeUInt8,
            DataTypeUInt16,
            DataTypeUInt32,
            DataTypeUInt64>(type.get(), std::forward<F>(f));
    }

    template <typename T>
    static size_t getMaxNumDecimals(T precision)
    {
        static_assert(std::is_integral_v<T>);
        if (accurate::lessOrEqualsOp(precision, 0))
            return 0;
        return std::min(static_cast<size_t>(precision), format_max_decimals);
    }

    template <typename T>
    static auto round(T number, size_t max_num_decimals [[maybe_unused]], const TiDBDecimalRoundInfo & info [[maybe_unused]])
    {
        if constexpr (IsDecimal<T>)
            return TiDBDecimalRound<T, T>::eval(number, max_num_decimals, info);
        else if constexpr (std::is_floating_point_v<T>)
            return TiDBFloatingRound<T, Float64>::eval(number, max_num_decimals);
        else
        {
            static_assert(std::is_integral_v<T>);
            return number;
        }
    }

    template <typename T>
    static std::string number2Str(T number, const TiDBDecimalRoundInfo & info [[maybe_unused]])
    {
        if constexpr (IsDecimal<T>)
            return number.toString(info.output_scale);
        else
        {
            static_assert(std::is_floating_point_v<T> || std::is_integral_v<T>);
            return fmt::format("{}", number);
        }
    }

    static void copyFromBuffer(const std::string & buffer, ColumnString::Chars_t & res_data, ColumnString::Offsets & res_offsets)
    {
        const size_t old_size = res_data.size();
        const size_t size_to_append = buffer.size() + 1;
        const size_t new_size = old_size + size_to_append;

        res_data.resize(new_size);
        memcpy(&res_data[old_size], buffer.c_str(), size_to_append);
        res_offsets.push_back(new_size);
    }

    template <typename T>
    static void format(
        T number,
        size_t max_num_decimals,
        TiDBDecimalRoundInfo & info,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        info.output_scale = std::min(max_num_decimals, static_cast<size_t>(info.input_scale));
        auto round_number = round(number, max_num_decimals, info);
        std::string round_number_str = number2Str(round_number, info);
        std::string buffer = Format::apply(round_number_str, max_num_decimals);
        copyFromBuffer(buffer, res_data, res_offsets);
    }
};

struct FormatWithEnUS
{
    static std::string apply(const std::string & number, size_t precision)
    {
        std::string buffer;
        size_t number_part_start = 0;
        if (number[0] == '-')
        {
            buffer += '-';
            number_part_start = 1;
        }

        auto point_index = number.find('.');
        if (point_index == std::string::npos)
            point_index = number.size();

        /// a comma can be used to group 3 digits in en_US locale, such as 12,345,678.00
        constexpr int digit_grouping_size = 3;
        constexpr char comma = ',';
        auto integer_part_size = point_index - number_part_start;
        const auto remainder = integer_part_size % digit_grouping_size;
        auto integer_part_pos = number.cbegin() + number_part_start;
        if (remainder != 0)
        {
            buffer.append(integer_part_pos, integer_part_pos + remainder);
            buffer += comma;
            integer_part_pos += remainder;
        }
        const auto integer_part_end = number.cbegin() + point_index;
        for (; integer_part_pos != integer_part_end; integer_part_pos += digit_grouping_size)
        {
            buffer.append(integer_part_pos, integer_part_pos + digit_grouping_size);
            buffer += comma;
        }
        buffer.pop_back();

        if (precision > 0)
        {
            buffer += '.';
            if (point_index == number.size()) /// no decimal part
                buffer.append(precision, '0');
            else
            {
                const auto decimal_part_size = number.size() - point_index - 1;
                const auto decimal_part_start = integer_part_end + 1;
                if (decimal_part_size >= precision)
                    buffer.append(decimal_part_start, decimal_part_start + precision);
                else
                    buffer.append(decimal_part_start, number.cend()).append(precision - decimal_part_size, '0');
            }
        }
        return buffer;
    }
};

class FunctionFormatWithLocale : public IFunction
{
public:
    struct NameFormatWithLocale
    {
        static constexpr auto name = "formatWithLocale";
    };
    template <typename Format>
    using FormatImpl_t = FormatImpl<NameFormatWithLocale, Format>;

    static constexpr auto name = NameFormatWithLocale::name;
    explicit FunctionFormatWithLocale(const Context & context_)
        : context(checkDagContextIsValid(context_))
    {}

    static FunctionPtr create(const Context & context_)
    {
        return std::make_shared<FunctionFormatWithLocale>(context_);
    }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto first_argument = removeNullable(arguments[0]);
        if (!first_argument->isNumber() && !first_argument->isDecimal())
            throw Exception(
                fmt::format("Illegal type {} of first argument of function {}", first_argument->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto second_argument = removeNullable(arguments[1]);
        if (!second_argument->isInteger())
            throw Exception(
                fmt::format("Illegal type {} of second argument of function {}", second_argument->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto third_argument = removeNullable(arguments[2]);
        if (!third_argument->isString())
            throw Exception(
                fmt::format("Illegal type {} of third argument of function {}", third_argument->getName(), getName()),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto return_type = std::make_shared<DataTypeString>();
        return (arguments[0]->isNullable() || arguments[1]->isNullable()) ? makeNullable(return_type) : return_type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto * locale_raw = block.getByPosition(arguments[2]).column.get();
        handleLocale(locale_raw);

        /// TODO support switching different locale in a block.
        static DefaultExecutable forward_function{std::make_shared<FormatImpl_t<FormatWithEnUS>>(context)};
        const ColumnNumbers forward_arguments{arguments[0], arguments[1]};
        forward_function.execute(block, forward_arguments, result);
    }

private:
    const Context & context;

    /// Append warning when locale is not 'en_US'.
    /// TODO support other locales after tidb has supported them.
    void handleLocale(const IColumn * locale_raw) const
    {
        static const std::string supported_locale = "en_US";
        using LocaleColVec = ColumnString;
        const auto column_size = locale_raw->size();
        if (const auto * locale_const = checkAndGetColumnConst<LocaleColVec>(locale_raw, true))
        {
            if (locale_const->onlyNull())
            {
                const auto & msg = genWarningMsg("NULL");
                for (size_t i = 0; i != column_size; ++i)
                    context.getDAGContext()->appendWarning(msg);
            }
            else
            {
                const auto value = locale_const->getValue<String>();
                if (!boost::iequals(value, supported_locale))
                {
                    const auto & msg = genWarningMsg(value);
                    for (size_t i = 0; i != column_size; ++i)
                        context.getDAGContext()->appendWarning(msg);
                }
            }
        }
        else
        {
            Field locale_field;
            for (size_t i = 0; i != column_size; ++i)
            {
                locale_raw->get(i, locale_field);
                if (locale_field.isNull())
                    context.getDAGContext()->appendWarning(genWarningMsg("NULL"));
                else
                {
                    String value = locale_field.get<String>();
                    if (!boost::iequals(value, supported_locale))
                        context.getDAGContext()->appendWarning(genWarningMsg(value));
                }
            }
        }
    }

    static std::string genWarningMsg(const std::string & value)
    {
        return fmt::format("Unknown locale: \'{}\'", value);
    }

    static const Context & checkDagContextIsValid(const Context & context_)
    {
        if (!context_.getDAGContext())
            throw Exception("DAGContext should not be nullptr.", ErrorCodes::LOGICAL_ERROR);
        return context_;
    }
};

// clang-format off
struct NameEmpty                 { static constexpr auto name = "empty"; };
struct NameNotEmpty              { static constexpr auto name = "notEmpty"; };
struct NameLength                { static constexpr auto name = "length"; };
struct NameLengthUTF8            { static constexpr auto name = "lengthUTF8"; };
struct NameLowerBinary           { static constexpr auto name = "lowerBinary"; };
struct NameLowerUTF8             { static constexpr auto name = "lowerUTF8"; };
struct NameUpperBinary           { static constexpr auto name = "upperBinary"; };
struct NameUpperUTF8             { static constexpr auto name = "upperUTF8"; };
struct NameReverseUTF8           { static constexpr auto name = "reverseUTF8"; };
struct NameTrim                  { static constexpr auto name = "trim"; };
struct NameLTrim                 { static constexpr auto name = "ltrim"; };
struct NameRTrim                 { static constexpr auto name = "rtrim"; };
struct NameTiDBTrim              { static constexpr auto name = "tidbTrim"; };
struct NameTiDBLTrim             { static constexpr auto name = "tidbLTrim"; };
struct NameTiDBRTrim             { static constexpr auto name = "tidbRTrim"; };
struct NameLPad                  { static constexpr auto name = "lpad"; };
struct NameLPadUTF8              { static constexpr auto name = "lpadUTF8"; };
struct NameRPad                  { static constexpr auto name = "rpad"; };
struct NameRPadUTF8              { static constexpr auto name = "rpadUTF8"; };
struct NameConcat                { static constexpr auto name = "concat"; };
struct NameConcatAssumeInjective { static constexpr auto name = "concatAssumeInjective"; };
struct NameFormat                { static constexpr auto name = "format"; };
// clang-format on

using FunctionEmpty = FunctionStringOrArrayToT<EmptyImpl<false>, NameEmpty, UInt8>;
using FunctionNotEmpty = FunctionStringOrArrayToT<EmptyImpl<true>, NameNotEmpty, UInt8>;
// using FunctionLength = FunctionStringOrArrayToT<LengthImpl, NameLength, UInt64>;
using FunctionLengthUTF8 = FunctionStringOrArrayToT<LengthUTF8Impl, NameLengthUTF8, UInt64>;
using FunctionLowerBinary = FunctionStringToString<TiDBLowerUpperBinaryImpl, NameLowerBinary>;
using FunctionLowerUTF8 = FunctionStringToString<TiDBLowerUpperUTF8Impl<'A', 'Z', CharUtil::unicodeToLower>, NameLowerUTF8>;
using FunctionUpperBinary = FunctionStringToString<TiDBLowerUpperBinaryImpl, NameUpperBinary>;
using FunctionUpperUTF8 = FunctionStringToString<TiDBLowerUpperUTF8Impl<'a', 'z', CharUtil::unicodeToUpper>, NameUpperUTF8>;
using FunctionReverseUTF8 = FunctionStringToString<ReverseUTF8Impl, NameReverseUTF8, true>;
using FunctionTrim = FunctionTiDBTrim<NameTiDBTrim, true, true>;
using FunctionLTrim = FunctionTiDBTrim<NameTiDBLTrim, true, false>;
using FunctionRTrim = FunctionTiDBTrim<NameTiDBRTrim, false, true>;
using FunctionLPad = PadImpl<NameLPad, true>;
using FunctionRPad = PadImpl<NameRPad, false>;
using FunctionLPadUTF8 = PadUTF8Impl<NameLPadUTF8, true>;
using FunctionRPadUTF8 = PadUTF8Impl<NameRPadUTF8, false>;
using FunctionConcat = ConcatImpl<NameConcat, false>;
using FunctionConcatAssumeInjective = ConcatImpl<NameConcatAssumeInjective, true>;
using FunctionFormat = FormatImpl<NameFormat, FormatWithEnUS>;

// export for tests
template struct LowerUpperUTF8Impl<'A', 'Z', Poco::Unicode::toLower, UTF8CyrillicToCase<true>>;
template struct LowerUpperUTF8Impl<'a', 'z', Poco::Unicode::toUpper, UTF8CyrillicToCase<false>>;
template struct LowerUpperImpl<'A', 'Z'>;
template struct LowerUpperImpl<'a', 'z'>;

void registerFunctionsString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmpty>();
    factory.registerFunction<FunctionNotEmpty>();
    factory.registerFunction<FunctionLength>();
    factory.registerFunction<FunctionLengthUTF8>();
    factory.registerFunction<FunctionLowerBinary>();
    factory.registerFunction<FunctionUpperBinary>();
    factory.registerFunction<FunctionLowerUTF8>();
    factory.registerFunction<FunctionUpperUTF8>();
    factory.registerFunction<FunctionReverse>();
    factory.registerFunction<FunctionReverseUTF8>();
    factory.registerFunction<FunctionTrim>("tidbTrim", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionLTrim>("tidbLTrim", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionRTrim>("tidbRTrim", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionLPad>();
    factory.registerFunction<FunctionRPad>();
    factory.registerFunction<FunctionLPadUTF8>();
    factory.registerFunction<FunctionRPadUTF8>();
    factory.registerFunction<FunctionConcat>();
    factory.registerFunction<FunctionConcatAssumeInjective>();
    factory.registerFunction<FunctionTiDBConcat>();
    factory.registerFunction<FunctionTiDBConcatWithSeparator>();
    factory.registerFunction<FunctionSubstring>();
    factory.registerFunction<FunctionSubstringUTF8>();
    factory.registerFunction<FunctionAppendTrailingCharIfAbsent>();
    factory.registerFunction<FunctionJsonLength>();
    factory.registerFunction<FunctionRightUTF8>();
    factory.registerFunction<FunctionASCII>();
    factory.registerFunction<FunctionPosition>();
    factory.registerFunction<FunctionSubStringIndex>();
    factory.registerFunction<FunctionFormat>();
    factory.registerFunction<FunctionFormatWithLocale>();
}
} // namespace DB
