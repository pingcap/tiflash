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

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNothing.h>
#include <Common/Volnitsky.h>
#include <Common/config.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/CollationStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringReplace.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/Regexps.h>
#include <Functions/StringUtil.h>
#include <Functions/re2Util.h>
#include <IO/WriteHelpers.h>
#include <Poco/UTF8String.h>
#include <TiDB/Collation/CollatorUtils.h>
#include <fmt/core.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <memory>

#if USE_RE2_ST
#include <re2_st/re2.h>
#else
#define re2_st re2
#endif


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

/// Is the LIKE expression reduced to finding a substring in a string?
bool likePatternIsStrstr(const String & pattern, String & res)
{
    res = "";

    if (pattern.size() < 2 || pattern.front() != '%' || pattern.back() != '%')
        return false;

    res.reserve(pattern.size() * 2);

    const char * pos = pattern.data();
    const char * end = pos + pattern.size();

    ++pos;
    --end;

    while (pos < end)
    {
        switch (*pos)
        {
        case '%':
        case '_':
            return false;
        case '\\':
            ++pos;
            if (pos == end)
                return false;
            else
                res += *pos;
            break;
        default:
            res += *pos;
            break;
        }
        ++pos;
    }

    return true;
}

// replace the escape_char in orig_string with '\'
// this function does not check the validation of the orig_string
// for example, for string "abcd" and escape char 'd', it will
// return "abc\"
String replaceEscapeChar(String & orig_string, UInt8 escape_char)
{
    std::stringstream ss;
    for (size_t i = 0; i < orig_string.size(); i++)
    {
        auto c = orig_string[i];
        if (static_cast<UInt8>(c) == escape_char)
        {
            if (i + 1 != orig_string.size() && static_cast<UInt8>(orig_string[i + 1]) == escape_char)
            {
                // two successive escape char, which means it is trying to escape itself, just remove one
                i++;
                ss << escape_char;
            }
            else
            {
                // https://github.com/pingcap/tidb/blob/master/util/stringutil/string_util.go#L154
                // if any char following escape char that is not [escape_char,'_','%'], it is invalid escape.
                // mysql will treat escape character as the origin value even
                // the escape sequence is invalid in Go or C.
                // e.g., \m is invalid in Go, but in MySQL we will get "m" for select '\m'.
                // Following case is correct just for escape \, not for others like +.
                // TODO: Add more checks for other escapes.
                if (i + 1 != orig_string.size() && orig_string[i + 1] == CH_ESCAPE_CHAR)
                {
                    continue;
                }
                ss << CH_ESCAPE_CHAR;
            }
        }
        else if (c == CH_ESCAPE_CHAR)
        {
            // need to escape this '\\'
            ss << CH_ESCAPE_CHAR << CH_ESCAPE_CHAR;
        }
        else
        {
            ss << c;
        }
    }
    return ss.str();
}

/** 'like' - if true, treat pattern as SQL LIKE; if false - treat pattern as re2 regexp.
  * NOTE: We want to run regexp search for whole block by one call (as implemented in function 'position')
  *  but for that, regexp engine must support \0 bytes and their interpretation as string boundaries.
  */
template <bool like, bool revert = false, bool for_tidb = false>
struct MatchImpl
{
    using ResultType = UInt8;
    /// need customized escape char when do the string search
    static const bool need_customized_escape_char = like && for_tidb;
    /// support match type when do the string search, used in regexp
    static const bool support_match_type = !like && for_tidb;

    static void vectorConstant(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const std::string & orig_pattern,
        UInt8 escape_char,
        const std::string & match_type,
        const TiDB::TiDBCollatorPtr & collator,
        PaddedPODArray<UInt8> & res)
    {
        /// collation only take affect for like, for regexp collation is not
        /// fully supported.(Only case sensitive/insensitive is supported)
        if (like && collator != nullptr)
        {
            bool use_optimized_path
                = StringPatternMatch<revert>(data, offsets, orig_pattern, escape_char, collator, res);
            if (!use_optimized_path)
            {
                auto matcher = collator->pattern();
                matcher->compile(orig_pattern, escape_char);
                LoopOneColumn(data, offsets, offsets.size(), [&](const std::string_view & view, size_t i) {
                    res[i] = revert ^ matcher->match(view.data(), view.size());
                });
            }
            return;
        }
        String pattern = orig_pattern;
        if (escape_char != CH_ESCAPE_CHAR)
            pattern = replaceEscapeChar(pattern, escape_char);
        String strstr_pattern;
        /// A simple case where the LIKE expression reduces to finding a substring in a string
        if (like && likePatternIsStrstr(pattern, strstr_pattern))
        {
            const UInt8 * begin = &data[0];
            const UInt8 * pos = begin;
            const UInt8 * end = pos + data.size();

            /// The current index in the array of strings.
            size_t i = 0;

            /// TODO You need to make that `searcher` is common to all the calls of the function.
            Volnitsky searcher(strstr_pattern.data(), strstr_pattern.size(), end - pos);

            /// We will search for the next occurrence in all rows at once.
            while (pos < end && end != (pos = searcher.search(pos, end - pos)))
            {
                /// Let's determine which index it refers to.
                while (begin + offsets[i] <= pos)
                {
                    res[i] = revert;
                    ++i;
                }

                /// We check that the entry does not pass through the boundaries of strings.
                if (pos + strstr_pattern.size() < begin + offsets[i])
                    res[i] = !revert;
                else
                    res[i] = revert;

                pos = begin + offsets[i];
                ++i;
            }

            /// Tail, in which there can be no substring.
            memset(&res[i], revert, (res.size() - i) * sizeof(res[0]));
        }
        else
        {
            size_t size = offsets.size();

            int flags = 0;
            if constexpr (for_tidb)
                flags |= OptimizedRegularExpression::RE_NO_OPTIMIZE;
            else
                flags |= OptimizedRegularExpression::RE_DOT_NL;

            /// match_type can overwrite collator
            if (!match_type.empty() || collator != nullptr)
            {
                String mode_modifiers = re2Util::getRE2ModeModifiers(match_type, collator);
                if (!mode_modifiers.empty())
                    pattern = mode_modifiers + pattern;
            }
            const auto & regexp = Regexps::get<like, true>(pattern, flags);

            std::string required_substring;
            bool is_trivial;
            bool required_substring_is_prefix; /// for `anchored` execution of the regexp.

            regexp->getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);

            if (required_substring.empty())
            {
                if (!regexp->getRE2()) /// An empty regexp. Always matches.
                {
                    memset(&res[0], 1, size * sizeof(res[0]));
                }
                else
                {
                    size_t prev_offset = 0;
                    for (size_t i = 0; i < size; ++i)
                    {
                        res[i] = revert
                            ^ regexp->getRE2()->Match(
                                re2_st::StringPiece(
                                    reinterpret_cast<const char *>(&data[prev_offset]),
                                    offsets[i] - prev_offset - 1),
                                0,
                                offsets[i] - prev_offset - 1,
                                re2_st::RE2::UNANCHORED,
                                nullptr,
                                0);

                        prev_offset = offsets[i];
                    }
                }
            }
            else
            {
                /// NOTE This almost matches with the case of LikePatternIsStrstr.

                const UInt8 * begin = &data[0];
                const UInt8 * pos = begin;
                const UInt8 * end = pos + data.size();

                /// The current index in the array of strings.
                size_t i = 0;

                Volnitsky searcher(required_substring.data(), required_substring.size(), end - pos);

                /// We will search for the next occurrence in all rows at once.
                while (pos < end && end != (pos = searcher.search(pos, end - pos)))
                {
                    /// Determine which index it refers to.
                    while (begin + offsets[i] <= pos)
                    {
                        res[i] = revert;
                        ++i;
                    }

                    /// We check that the entry does not pass through the boundaries of strings.
                    if (pos + strstr_pattern.size() < begin + offsets[i])
                    {
                        /// And if it does not, if necessary, we check the regexp.

                        if (is_trivial)
                            res[i] = !revert;
                        else
                        {
                            const char * str_data = reinterpret_cast<const char *>(&data[i != 0 ? offsets[i - 1] : 0]);
                            size_t str_size = (i != 0 ? offsets[i] - offsets[i - 1] : offsets[0]) - 1;

                            /** Even in the case of `required_substring_is_prefix` use UNANCHORED check for regexp,
                              *  so that it can match when `required_substring` occurs into the line several times,
                              *  and at the first occurrence, the regexp is not a match.
                              */

                            if (required_substring_is_prefix)
                                res[i] = revert
                                    ^ regexp->getRE2()->Match(
                                        re2_st::StringPiece(str_data, str_size),
                                        reinterpret_cast<const char *>(pos) - str_data,
                                        str_size,
                                        re2_st::RE2::UNANCHORED,
                                        nullptr,
                                        0);
                            else
                                res[i] = revert
                                    ^ regexp->getRE2()->Match(
                                        re2_st::StringPiece(str_data, str_size),
                                        0,
                                        str_size,
                                        re2_st::RE2::UNANCHORED,
                                        nullptr,
                                        0);
                        }
                    }
                    else
                        res[i] = revert;

                    pos = begin + offsets[i];
                    ++i;
                }

                memset(&res[i], revert, (res.size() - i) * sizeof(res[0]));
            }
        }
    }

    static void constantConstant(
        const std::string & data,
        const std::string & orig_pattern,
        UInt8 escape_char,
        const std::string & match_type,
        const TiDB::TiDBCollatorPtr & collator,
        UInt8 & res)
    {
        if (like && collator != nullptr)
        {
            auto matcher = collator->pattern();
            matcher->compile(orig_pattern, escape_char);
            res = revert ^ matcher->match(data.data(), data.length());
        }
        else
        {
            String pattern = orig_pattern;
            if (escape_char != CH_ESCAPE_CHAR)
                pattern = replaceEscapeChar(pattern, escape_char);
            int flags = 0;
            if constexpr (for_tidb)
                flags |= OptimizedRegularExpression::RE_NO_OPTIMIZE;
            else
                flags |= OptimizedRegularExpression::RE_DOT_NL;

            /// match_type can overwrite collator
            if (!match_type.empty() || collator != nullptr)
            {
                String mode_modifiers = re2Util::getRE2ModeModifiers(match_type, collator);
                if (!mode_modifiers.empty())
                    pattern = mode_modifiers + pattern;
            }
            const auto & regexp = Regexps::get<like, true>(pattern, flags);
            res = revert ^ regexp->match(data);
        }
    }

    static void vectorVector(
        const ColumnString::Chars_t & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars_t & needle_data,
        const ColumnString::Offsets & needle_offsets,
        UInt8 escape_char,
        const std::string & match_type,
        const TiDB::TiDBCollatorPtr & collator,
        PaddedPODArray<UInt8> & res)
    {
        size_t size = haystack_offsets.size();

        ColumnString::Offset prev_haystack_offset = 0;
        ColumnString::Offset prev_needle_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;
            size_t haystack_size = haystack_offsets[i] - prev_haystack_offset - 1;
            // TODO: remove the copy, use raw char array directly
            std::string haystack_str(
                reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]),
                haystack_size);
            std::string needle_str(reinterpret_cast<const char *>(&needle_data[prev_needle_offset]), needle_size);
            constantConstant(haystack_str, needle_str, escape_char, match_type, collator, res[i]);
            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offsets[i];
        }
    }

    /// Search different needles in single haystack.
    static void constantVector(
        const std::string & haystack_data,
        const ColumnString::Chars_t & needle_data,
        const ColumnString::Offsets & needle_offsets,
        UInt8 escape_char,
        const std::string & match_type,
        const TiDB::TiDBCollatorPtr & collator,
        PaddedPODArray<UInt8> & res)
    {
        size_t size = needle_offsets.size();
        res.resize(size);

        ColumnString::Offset prev_needle_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;
            // TODO: remove the copy, use raw char array directly
            std::string needle_str(reinterpret_cast<const char *>(&needle_data[prev_needle_offset]), needle_size);
            constantConstant(haystack_data, needle_str, escape_char, match_type, collator, res[i]);
            prev_needle_offset = needle_offsets[i];
        }
    }
};


struct ExtractImpl
{
    /// need customized escape char when do the string search
    static const bool need_customized_escape_char = false;
    /// support match type when do the string search, used in regexp
    static const bool support_match_type = false;

    static void vector(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const std::string & pattern,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size() / 5);
        res_offsets.resize(offsets.size());

        int flags = OptimizedRegularExpression::RE_DOT_NL;
        const auto & regexp = Regexps::get<false, false>(pattern, flags);

        unsigned capture = regexp->getNumberOfCaptureGroup() > 0 ? 1 : 0;
        OptimizedRegularExpression::MatchVec matches;
        matches.reserve(capture + 1);
        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t cur_offset = offsets[i];

            unsigned count = regexp->match(
                reinterpret_cast<const char *>(&data[prev_offset]),
                cur_offset - prev_offset - 1,
                matches,
                capture + 1);
            if (count > capture && matches[capture].offset != std::string::npos)
            {
                const auto & match = matches[capture];
                res_data.resize(res_offset + match.length + 1);
                memcpySmallAllowReadWriteOverflow15(
                    &res_data[res_offset],
                    &data[prev_offset + match.offset],
                    match.length);
                res_offset += match.length;
            }
            else
            {
                res_data.resize(res_offset + 1);
            }

            res_data[res_offset] = 0;
            ++res_offset;
            res_offsets[i] = res_offset;

            prev_offset = cur_offset;
        }
    }
};

/** Replace one or all occurencies of substring 'needle' to 'replacement'. 'needle' and 'replacement' are constants.
  */
template <bool replace_one = false>
struct ReplaceStringImpl
{
    static constexpr bool support_non_const_needle = true;
    static constexpr bool support_non_const_replacement = true;
    /// need customized escape char during the string search
    static const bool need_customized_escape_char = false;
    /// support match type during the string search, used in regexp
    static const bool support_match_type = false;

    static void vector(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const UInt8 * begin = &data[0];
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        ColumnString::Offset res_offset = 0;
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        if (needle.empty())
        {
            /// Copy all the data without changing.
            res_data.resize(data.size());
            memcpy(&res_data[0], begin, data.size());
            memcpy(&res_offsets[0], &offsets[0], size * sizeof(UInt64));
            return;
        }

        /// The current index in the array of strings.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (pos < end)
        {
            const UInt8 * match = searcher.search(pos, end - pos);

            /// Copy the data without changing
            res_data.resize(res_data.size() + (match - pos));
            memcpy(&res_data[res_offset], pos, match - pos);

            /// Determine which index it belongs to.
            while (i < offsets.size() && begin + offsets[i] <= match)
            {
                res_offsets[i] = res_offset + ((begin + offsets[i]) - pos);
                ++i;
            }
            res_offset += (match - pos);

            /// If you have reached the end, it's time to stop
            if (i == offsets.size())
                break;

            /// Is it true that this line no longer needs to perform transformations.
            bool can_finish_current_string = false;

            /// We check that the entry does not go through the boundaries of strings.
            if (match + needle.size() < begin + offsets[i])
            {
                res_data.resize(res_data.size() + replacement.size());
                memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                res_offset += replacement.size();
                pos = match + needle.size();
                if (replace_one)
                    can_finish_current_string = true;
            }
            else
            {
                pos = match;
                can_finish_current_string = true;
            }

            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + (begin + offsets[i] - pos));
                memcpy(&res_data[res_offset], pos, (begin + offsets[i] - pos));
                res_offset += (begin + offsets[i] - pos);
                res_offsets[i] = res_offset;
                pos = begin + offsets[i];
                ++i;
            }
        }
    }

    static void vectorNonConstNeedle(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const ColumnString::Chars_t & needle_chars,
        const ColumnString::Offsets & needle_offsets,
        const std::string & replacement,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        res_offsets.resize(offsets.size());

        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            auto data_offset = StringUtil::offsetAt(offsets, i);
            auto data_size = StringUtil::sizeAt(offsets, i);

            auto needle_offset = StringUtil::offsetAt(needle_offsets, i);
            auto needle_size = StringUtil::sizeAt(needle_offsets, i) - 1; // ignore the trailing zero

            const UInt8 * begin = &data[data_offset];
            const UInt8 * pos = begin;
            const UInt8 * end = pos + data_size;

            if (needle_size == 0)
            {
                /// Copy the whole data to res without changing
                res_data.resize(res_data.size() + data_size);
                memcpy(&res_data[res_offset], begin, data_size);
                res_offset += data_size;
                res_offsets[i] = res_offset;
                continue;
            }

            Volnitsky searcher(reinterpret_cast<const char *>(&needle_chars[needle_offset]), needle_size, data_size);
            while (pos < end)
            {
                const UInt8 * match = searcher.search(pos, end - pos);

                /// Copy the data without changing.
                res_data.resize(res_data.size() + (match - pos));
                memcpy(&res_data[res_offset], pos, match - pos);
                res_offset += match - pos;

                if (match == end)
                {
                    /// It's time to stop.
                    break;
                }

                res_data.resize(res_data.size() + replacement.size());
                memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                res_offset += replacement.size();
                pos = match + needle_size;

                if (replace_one)
                {
                    /// Copy the rest of data and stop.
                    res_data.resize(res_data.size() + (end - pos));
                    memcpy(&res_data[res_offset], pos, (end - pos));
                    res_offset += (end - pos);
                    break;
                }
            }
            res_offsets[i] = res_offset;
        }
    }

    static void vectorNonConstReplacement(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const std::string & needle,
        const ColumnString::Chars_t & replacement_chars,
        const ColumnString::Offsets & replacement_offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const UInt8 * begin = &data[0];
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        ColumnString::Offset res_offset = 0;
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        if (needle.empty())
        {
            /// Copy all the data without changing.
            res_data.resize(data.size());
            memcpy(&res_data[0], begin, data.size());
            memcpy(&res_offsets[0], &offsets[0], size * sizeof(UInt64));
            return;
        }

        /// The current index in the array of strings.
        size_t i = 0;

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (pos < end)
        {
            const UInt8 * match = searcher.search(pos, end - pos);

            /// Copy the data without changing
            res_data.resize(res_data.size() + (match - pos));
            memcpy(&res_data[res_offset], pos, match - pos);

            /// Determine which index it belongs to.
            while (i < offsets.size() && begin + offsets[i] <= match)
            {
                res_offsets[i] = res_offset + ((begin + offsets[i]) - pos);
                ++i;
            }
            res_offset += (match - pos);

            /// If you have reached the end, it's time to stop
            if (i == offsets.size())
                break;

            /// Is it true that this line no longer needs to perform transformations.
            bool can_finish_current_string = false;

            auto replacement_offset = StringUtil::offsetAt(replacement_offsets, i);
            auto replacement_size = StringUtil::sizeAt(replacement_offsets, i) - 1; // ignore the trailing zero

            /// We check that the entry does not go through the boundaries of strings.
            if (match + needle.size() < begin + offsets[i])
            {
                res_data.resize(res_data.size() + replacement_size);
                memcpy(&res_data[res_offset], &replacement_chars[replacement_offset], replacement_size);
                res_offset += replacement_size;
                pos = match + needle.size();
                if (replace_one)
                    can_finish_current_string = true;
            }
            else
            {
                pos = match;
                can_finish_current_string = true;
            }

            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + (begin + offsets[i] - pos));
                memcpy(&res_data[res_offset], pos, (begin + offsets[i] - pos));
                res_offset += (begin + offsets[i] - pos);
                res_offsets[i] = res_offset;
                pos = begin + offsets[i];
                ++i;
            }
        }
    }

    static void vectorNonConstNeedleReplacement(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const ColumnString::Chars_t & needle_chars,
        const ColumnString::Offsets & needle_offsets,
        const ColumnString::Chars_t & replacement_chars,
        const ColumnString::Offsets & replacement_offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        res_offsets.resize(offsets.size());
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            auto data_offset = StringUtil::offsetAt(offsets, i);
            auto data_size = StringUtil::sizeAt(offsets, i);

            auto needle_offset = StringUtil::offsetAt(needle_offsets, i);
            auto needle_size = StringUtil::sizeAt(needle_offsets, i) - 1; // ignore the trailing zero

            auto replacement_offset = StringUtil::offsetAt(replacement_offsets, i);
            auto replacement_size = StringUtil::sizeAt(replacement_offsets, i) - 1; // ignore the trailing zero

            const UInt8 * begin = &data[data_offset];
            const UInt8 * pos = begin;
            const UInt8 * end = pos + data_size;

            if (needle_size == 0)
            {
                res_data.resize(res_data.size() + data_size);
                memcpy(&res_data[res_offset], begin, data_size);
                res_offset += data_size;
                res_offsets[i] = res_offset;
                continue;
            }

            Volnitsky searcher(reinterpret_cast<const char *>(&needle_chars[needle_offset]), needle_size, data_size);
            while (pos < end)
            {
                const UInt8 * match = searcher.search(pos, end - pos);

                /// Copy the data without changing.
                res_data.resize(res_data.size() + (match - pos));
                memcpy(&res_data[res_offset], pos, match - pos);
                res_offset += match - pos;

                if (match == end)
                {
                    /// It's time to stop.
                    break;
                }

                res_data.resize(res_data.size() + replacement_size);
                memcpy(&res_data[res_offset], &replacement_chars[replacement_offset], replacement_size);
                res_offset += replacement_size;
                pos = match + needle_size;

                if (replace_one)
                {
                    /// Copy the rest of data and stop.
                    res_data.resize(res_data.size() + (end - pos));
                    memcpy(&res_data[res_offset], pos, (end - pos));
                    res_offset += (end - pos);
                    break;
                }
            }
            res_offsets[i] = res_offset;
        }
    }

    /// Note: this function converts fixed-length strings to variable-length strings
    ///       and each variable-length string should ends with zero byte.
    static void vectorFixed(
        const ColumnString::Chars_t & data,
        size_t n,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const UInt8 * begin = &data[0];
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        ColumnString::Offset res_offset = 0;
        size_t count = data.size() / n;
        res_data.reserve(data.size());
        res_offsets.resize(count);

        /// The current index in the string array.
        size_t i = 0;

#define COPY_REST_OF_CURRENT_STRING()                 \
    do                                                \
    {                                                 \
        const size_t len = begin + n * (i + 1) - pos; \
        res_data.resize(res_data.size() + len + 1);   \
        memcpy(&res_data[res_offset], pos, len);      \
        res_offset += len;                            \
        res_data[res_offset++] = 0;                   \
        res_offsets[i] = res_offset;                  \
        pos = begin + n * (i + 1);                    \
        ++i;                                          \
    } while (false)

        if (needle.empty())
        {
            /// Copy all the data without changing.
            while (i < count)
            {
                COPY_REST_OF_CURRENT_STRING();
            }
            return;
        }

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (pos < end)
        {
            const UInt8 * match = searcher.search(pos, end - pos);

            /// Copy skipped strings without any changes but
            /// add zero byte to the end of each string.
            while (i < count && begin + n * (i + 1) <= match)
            {
                COPY_REST_OF_CURRENT_STRING();
            }

            /// If you have reached the end, it's time to stop
            if (i == count)
                break;

            /// Copy unchanged part of current string.
            res_data.resize(res_data.size() + (match - pos));
            memcpy(&res_data[res_offset], pos, match - pos);
            res_offset += (match - pos);

            /// Is it true that this line no longer needs to perform conversions.
            bool can_finish_current_string = false;

            /// We check that the entry does not pass through the boundaries of strings.
            if (match + needle.size() <= begin + n * (i + 1))
            {
                res_data.resize(res_data.size() + replacement.size());
                memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                res_offset += replacement.size();
                pos = match + needle.size();
                if (replace_one || pos == begin + n * (i + 1))
                    can_finish_current_string = true;
            }
            else
            {
                pos = match;
                can_finish_current_string = true;
            }

            if (can_finish_current_string)
            {
                COPY_REST_OF_CURRENT_STRING();
            }
#undef COPY_REST_OF_CURRENT_STRING
        }
    }

    static void vectorFixedNonConstNeedle(
        const ColumnString::Chars_t & data,
        size_t n,
        const ColumnString::Chars_t & needle_chars,
        const ColumnString::Offsets & needle_offsets,
        const std::string & replacement,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        size_t count = data.size() / n;
        res_data.reserve(data.size());
        res_offsets.resize(count);
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < count; ++i)
        {
            const UInt8 * begin = &data[i * n];
            const UInt8 * pos = begin;
            const UInt8 * end = pos + n;

#define COPY_REST_OF_CURRENT_STRING()               \
    do                                              \
    {                                               \
        const size_t len = end - pos;               \
        res_data.resize(res_data.size() + len + 1); \
        memcpy(&res_data[res_offset], pos, len);    \
        res_offset += len;                          \
        res_data[res_offset++] = 0;                 \
        res_offsets[i] = res_offset;                \
        pos = end;                                  \
    } while (false)

            auto needle_offset = StringUtil::offsetAt(needle_offsets, i);
            auto needle_size = StringUtil::sizeAt(needle_offsets, i) - 1; // ignore the trailing zero
            if (needle_size == 0)
            {
                COPY_REST_OF_CURRENT_STRING();
                continue;
            }

            Volnitsky searcher(reinterpret_cast<const char *>(&needle_chars[needle_offset]), needle_size, n);
            while (pos < end)
            {
                const UInt8 * match = searcher.search(pos, end - pos);

                if (match == end)
                {
                    COPY_REST_OF_CURRENT_STRING();
                    break;
                }

                /// Copy the data without changing
                res_data.resize(res_data.size() + (match - pos));
                memcpy(&res_data[res_offset], pos, match - pos);
                res_offset += match - pos;

                res_data.resize(res_data.size() + replacement.size());
                memcpy(&res_data[res_offset], replacement.data(), replacement.size());
                res_offset += replacement.size();
                pos = match + needle_size;

                if (replace_one)
                {
                    COPY_REST_OF_CURRENT_STRING();
                    break;
                }
            }
#undef COPY_REST_OF_CURRENT_STRING
        }
    }

    static void vectorFixedNonConstReplacement(
        const ColumnString::Chars_t & data,
        size_t n,
        const std::string & needle,
        const ColumnString::Chars_t & replacement_chars,
        const ColumnString::Offsets & replacement_offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const UInt8 * begin = &data[0];
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        ColumnString::Offset res_offset = 0;
        size_t count = data.size() / n;
        res_data.reserve(data.size());
        res_offsets.resize(count);

        /// The current index in the string array.
        size_t i = 0;

#define COPY_REST_OF_CURRENT_STRING()                 \
    do                                                \
    {                                                 \
        const size_t len = begin + n * (i + 1) - pos; \
        res_data.resize(res_data.size() + len + 1);   \
        memcpy(&res_data[res_offset], pos, len);      \
        res_offset += len;                            \
        res_data[res_offset++] = 0;                   \
        res_offsets[i] = res_offset;                  \
        pos = begin + n * (i + 1);                    \
        ++i;                                          \
    } while (false)

        if (needle.empty())
        {
            /// Copy all the data without changing.
            while (i < count)
            {
                COPY_REST_OF_CURRENT_STRING();
            }
            return;
        }

        Volnitsky searcher(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all rows at once.
        while (pos < end)
        {
            const UInt8 * match = searcher.search(pos, end - pos);

            /// Copy skipped strings without any changes but
            /// add zero byte to the end of each string.
            while (i < count && begin + n * (i + 1) <= match)
            {
                COPY_REST_OF_CURRENT_STRING();
            }

            /// If you have reached the end, it's time to stop
            if (i == count)
                break;

            /// Copy unchanged part of current string.
            res_data.resize(res_data.size() + (match - pos));
            memcpy(&res_data[res_offset], pos, match - pos);
            res_offset += (match - pos);

            /// Is it true that this line no longer needs to perform conversions.
            bool can_finish_current_string = false;

            /// We check that the entry does not pass through the boundaries of strings.
            if (match + needle.size() <= begin + n * (i + 1))
            {
                auto replacement_offset = StringUtil::offsetAt(replacement_offsets, i);
                auto replacement_size = StringUtil::sizeAt(replacement_offsets, i) - 1; // ignore the trailing zero

                res_data.resize(res_data.size() + replacement_size);
                memcpy(&res_data[res_offset], &replacement_chars[replacement_offset], replacement_size);
                res_offset += replacement_size;
                pos = match + needle.size();
                if (replace_one || pos == begin + n * (i + 1))
                    can_finish_current_string = true;
            }
            else
            {
                pos = match;
                can_finish_current_string = true;
            }

            if (can_finish_current_string)
            {
                COPY_REST_OF_CURRENT_STRING();
            }
#undef COPY_REST_OF_CURRENT_STRING
        }
    }

    static void vectorFixedNonConstNeedleReplacement(
        const ColumnString::Chars_t & data,
        size_t n,
        const ColumnString::Chars_t & needle_chars,
        const ColumnString::Offsets & needle_offsets,
        const ColumnString::Chars_t & replacement_chars,
        const ColumnString::Offsets & replacement_offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        size_t count = data.size() / n;
        res_data.reserve(data.size());
        res_offsets.resize(count);
        ColumnString::Offset res_offset = 0;

        for (size_t i = 0; i < count; ++i)
        {
            const UInt8 * begin = &data[i * n];
            const UInt8 * pos = begin;
            const UInt8 * end = pos + n;

#define COPY_REST_OF_CURRENT_STRING()               \
    do                                              \
    {                                               \
        const size_t len = end - pos;               \
        res_data.resize(res_data.size() + len + 1); \
        memcpy(&res_data[res_offset], pos, len);    \
        res_offset += len;                          \
        res_data[res_offset++] = 0;                 \
        res_offsets[i] = res_offset;                \
        pos = end;                                  \
    } while (false)

            auto needle_offset = StringUtil::offsetAt(needle_offsets, i);
            auto needle_size = StringUtil::sizeAt(needle_offsets, i) - 1; // ignore the trailing zero

            auto replacement_offset = StringUtil::offsetAt(replacement_offsets, i);
            auto replacement_size = StringUtil::sizeAt(replacement_offsets, i) - 1; // ignore the trailing zero

            if (needle_size == 0)
            {
                COPY_REST_OF_CURRENT_STRING();
                continue;
            }

            Volnitsky searcher(reinterpret_cast<const char *>(&needle_chars[needle_offset]), needle_size, n);
            while (pos < end)
            {
                const UInt8 * match = searcher.search(pos, end - pos);

                if (match == end)
                {
                    COPY_REST_OF_CURRENT_STRING();
                    break;
                }

                /// Copy the data without changing
                res_data.resize(res_data.size() + (match - pos));
                memcpy(&res_data[res_offset], pos, match - pos);
                res_offset += match - pos;

                res_data.resize(res_data.size() + replacement_size);
                memcpy(&res_data[res_offset], &replacement_chars[replacement_offset], replacement_size);
                res_offset += replacement_size;
                pos = match + needle_size;

                if (replace_one)
                {
                    COPY_REST_OF_CURRENT_STRING();
                    break;
                }
            }
#undef COPY_REST_OF_CURRENT_STRING
        }
    }

    static void constant(
        const std::string & data,
        const std::string & needle,
        const std::string & replacement,
        std::string & res_data)
    {
        if (needle.empty())
        {
            res_data = data;
            return;
        }
        res_data = "";
        int replace_cnt = 0;
        for (size_t i = 0; i < data.size(); ++i)
        {
            bool match = true;
            if (i + needle.size() > data.size() || (replace_one && replace_cnt > 0))
                match = false;
            for (size_t j = 0; match && j < needle.size(); ++j)
                if (data[i + j] != needle[j])
                    match = false;
            if (match)
            {
                ++replace_cnt;
                res_data += replacement;
                i = i + needle.size() - 1;
            }
            else
                res_data += data[i];
        }
    }
};

struct NameLike
{
    static constexpr auto name = "like";
};
struct NameMatch
{
    static constexpr auto name = "match";
};
struct NameLike3Args
{
    static constexpr auto name = "like3Args";
};
struct NameNotLike
{
    static constexpr auto name = "notLike";
};
struct NameExtract
{
    static constexpr auto name = "extract";
};
struct NameReplaceOne
{
    static constexpr auto name = "replaceOne";
};
struct NameReplaceAll
{
    static constexpr auto name = "replaceAll";
};

using FunctionMatch = FunctionsStringSearch<MatchImpl<false>, NameMatch>;
using FunctionLike = FunctionsStringSearch<MatchImpl<true>, NameLike>;
using FunctionLike3Args = FunctionsStringSearch<MatchImpl<true, false, true>, NameLike3Args>;
using FunctionIlike3Args = FunctionsStringSearch<MatchImpl<true, false, true>, NameIlike3Args>;
using FunctionNotLike = FunctionsStringSearch<MatchImpl<true, true>, NameNotLike>;
using FunctionExtract = FunctionsStringSearchToString<ExtractImpl, NameExtract>;
using FunctionReplaceOne = FunctionStringReplace<ReplaceStringImpl<true>, NameReplaceOne>;
using FunctionReplaceAll = FunctionStringReplace<ReplaceStringImpl<false>, NameReplaceAll>;

void registerFunctionsStringSearch(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplaceOne>();
    factory.registerFunction<FunctionReplaceAll>();
    factory.registerFunction<FunctionMatch>();
    factory.registerFunction<FunctionLike>();
    factory.registerFunction<FunctionLike3Args>();
    factory.registerFunction<FunctionIlike3Args>();
    factory.registerFunction<FunctionNotLike>();
    factory.registerFunction<FunctionExtract>();
}
} // namespace DB
