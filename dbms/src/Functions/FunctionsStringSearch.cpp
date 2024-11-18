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
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/Regexps.h>
#include <Functions/StringUtil.h>
#include <IO/WriteHelpers.h>
#include <Poco/UTF8String.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <memory>
#include <mutex>

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

/** Implementation details for functions of 'position' family depending on ASCII/UTF8 and case sensitiveness.
  */
struct PositionCaseSensitiveASCII
{
    /// For searching single substring inside big-enough contiguous chunk of data. Coluld have slightly expensive initialization.
    using SearcherInBigHaystack = VolnitskyImpl<true, true>;

    /// For searching single substring, that is different each time. This object is created for each row of data. It must have cheap initialization.
    using SearcherInSmallHaystack = LibCASCIICaseSensitiveStringSearcher;

    static SearcherInBigHaystack createSearcherInBigHaystack(const char * needle_data, size_t needle_size, size_t haystack_size_hint)
    {
        return SearcherInBigHaystack(needle_data, needle_size, haystack_size_hint);
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char * needle_data, size_t needle_size)
    {
        return SearcherInSmallHaystack(needle_data, needle_size);
    }

    /// Number of code points between 'begin' and 'end' (this has different behaviour for ASCII and UTF-8).
    static size_t countChars(const char * begin, const char * end)
    {
        return end - begin;
    }

    /// Convert string to lowercase. Only for case-insensitive search.
    /// Implementation is permitted to be inefficient because it is called for single string.
    static void toLowerIfNeed(std::string &)
    {
    }
};

struct PositionCaseInsensitiveASCII
{
    /// `Volnitsky` is not used here, because one person has measured that this is better. It will be good if you question it.
    using SearcherInBigHaystack = ASCIICaseInsensitiveStringSearcher;
    using SearcherInSmallHaystack = LibCASCIICaseInsensitiveStringSearcher;

    static SearcherInBigHaystack createSearcherInBigHaystack(const char * needle_data, size_t needle_size, size_t /*haystack_size_hint*/)
    {
        return SearcherInBigHaystack(needle_data, needle_size);
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char * needle_data, size_t needle_size)
    {
        return SearcherInSmallHaystack(needle_data, needle_size);
    }

    static size_t countChars(const char * begin, const char * end)
    {
        return end - begin;
    }

    static void toLowerIfNeed(std::string & s)
    {
        std::transform(std::begin(s), std::end(s), std::begin(s), tolower);
    }
};

struct PositionCaseSensitiveUTF8
{
    using SearcherInBigHaystack = VolnitskyImpl<true, false>;
    using SearcherInSmallHaystack = LibCASCIICaseSensitiveStringSearcher;

    static SearcherInBigHaystack createSearcherInBigHaystack(const char * needle_data, size_t needle_size, size_t haystack_size_hint)
    {
        return SearcherInBigHaystack(needle_data, needle_size, haystack_size_hint);
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char * needle_data, size_t needle_size)
    {
        return SearcherInSmallHaystack(needle_data, needle_size);
    }

    static size_t countChars(const char * begin, const char * end)
    {
        size_t res = 0;
        for (const char * it = begin; it != end; ++it)
            if (!UTF8::isContinuationOctet(static_cast<UInt8>(*it)))
                ++res;
        return res;
    }

    static void toLowerIfNeed(std::string &)
    {
    }
};

struct PositionCaseInsensitiveUTF8
{
    using SearcherInBigHaystack = VolnitskyImpl<false, false>;
    using SearcherInSmallHaystack = UTF8CaseInsensitiveStringSearcher; /// TODO Very suboptimal.

    static SearcherInBigHaystack createSearcherInBigHaystack(const char * needle_data, size_t needle_size, size_t haystack_size_hint)
    {
        return SearcherInBigHaystack(needle_data, needle_size, haystack_size_hint);
    }

    static SearcherInSmallHaystack createSearcherInSmallHaystack(const char * needle_data, size_t needle_size)
    {
        return SearcherInSmallHaystack(needle_data, needle_size);
    }

    static size_t countChars(const char * begin, const char * end)
    {
        size_t res = 0;
        for (const char * it = begin; it != end; ++it)
            if (!UTF8::isContinuationOctet(static_cast<UInt8>(*it)))
                ++res;
        return res;
    }

    static void toLowerIfNeed(std::string & s)
    {
        Poco::UTF8::toLowerInPlace(s);
    }
};

template <typename Impl>
struct PositionImpl
{
    using ResultType = UInt64;
    /// need customized escape char when do the string search
    static const bool need_customized_escape_char = false;
    /// support match type when do the string search, used in regexp
    static const bool support_match_type = false;

    /// Find one substring in many strings.
    static void vectorConstant(const ColumnString::Chars_t & data,
                               const ColumnString::Offsets & offsets,
                               const std::string & needle,
                               const UInt8 escape_char,
                               const std::string & match_type,
                               const TiDB::TiDBCollatorPtr & collator,
                               PaddedPODArray<UInt64> & res)
    {
        if (escape_char != CH_ESCAPE_CHAR || !match_type.empty() || collator != nullptr)
            throw Exception("PositionImpl don't support customized escape char/match_type argument/tidb collator", ErrorCodes::NOT_IMPLEMENTED);
        const UInt8 * begin = &data[0];
        const UInt8 * pos = begin;
        const UInt8 * end = pos + data.size();

        /// Current index in the array of strings.
        size_t i = 0;

        typename Impl::SearcherInBigHaystack searcher = Impl::createSearcherInBigHaystack(needle.data(), needle.size(), end - pos);

        /// We will search for the next occurrence in all strings at once.
        while (pos < end && end != (pos = searcher.search(pos, end - pos)))
        {
            /// Determine which index it refers to.
            while (begin + offsets[i] <= pos)
            {
                res[i] = 0;
                ++i;
            }

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + needle.size() < begin + offsets[i])
            {
                size_t prev_offset = i != 0 ? offsets[i - 1] : 0;
                res[i] = 1 + Impl::countChars(reinterpret_cast<const char *>(begin + prev_offset), reinterpret_cast<const char *>(pos));
            }
            else
                res[i] = 0;

            pos = begin + offsets[i];
            ++i;
        }

        memset(&res[i], 0, (res.size() - i) * sizeof(res[0]));
    }

    /// Search for substring in string.
    static void constantConstant(std::string data, std::string needle, const UInt8 escape_char, const std::string & match_type, const TiDB::TiDBCollatorPtr & collator, UInt64 & res)
    {
        if (escape_char != CH_ESCAPE_CHAR || !match_type.empty() || collator != nullptr)
            throw Exception("PositionImpl don't support customized escape char/match_type argument/tidb collator", ErrorCodes::NOT_IMPLEMENTED);
        Impl::toLowerIfNeed(data);
        Impl::toLowerIfNeed(needle);

        res = data.find(needle);
        if (res == std::string::npos)
            res = 0;
        else
            res = 1 + Impl::countChars(data.data(), data.data() + res);
    }

    /// Search each time for a different single substring inside each time different string.
    static void vectorVector(const ColumnString::Chars_t & haystack_data,
                             const ColumnString::Offsets & haystack_offsets,
                             const ColumnString::Chars_t & needle_data,
                             const ColumnString::Offsets & needle_offsets,
                             const UInt8 escape_char,
                             const std::string & match_type,
                             const TiDB::TiDBCollatorPtr & collator,
                             PaddedPODArray<UInt64> & res)
    {
        if (escape_char != CH_ESCAPE_CHAR || !match_type.empty() || collator != nullptr)
            throw Exception("PositionImpl don't support customized escape char/match_type argument/tidb collator", ErrorCodes::NOT_IMPLEMENTED);
        ColumnString::Offset prev_haystack_offset = 0;
        ColumnString::Offset prev_needle_offset = 0;

        size_t size = haystack_offsets.size();

        for (size_t i = 0; i < size; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;
            size_t haystack_size = haystack_offsets[i] - prev_haystack_offset - 1;

            if (0 == needle_size)
            {
                /// An empty string is always at the very beginning of `haystack`.
                res[i] = 1;
            }
            else
            {
                /// It is assumed that the StringSearcher is not very difficult to initialize.
                typename Impl::SearcherInSmallHaystack searcher
                    = Impl::createSearcherInSmallHaystack(reinterpret_cast<const char *>(&needle_data[prev_needle_offset]),
                                                          needle_offsets[i] - prev_needle_offset - 1); /// zero byte at the end

                /// searcher returns a pointer to the found substring or to the end of `haystack`.
                size_t pos = searcher.search(&haystack_data[prev_haystack_offset], &haystack_data[haystack_offsets[i] - 1])
                    - &haystack_data[prev_haystack_offset];

                if (pos != haystack_size)
                {
                    res[i] = 1 + Impl::countChars(reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]), reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset + pos]));
                }
                else
                    res[i] = 0;
            }

            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offsets[i];
        }
    }

    /// Find many substrings in one line.
    static void constantVector(const String & haystack,
                               const ColumnString::Chars_t & needle_data,
                               const ColumnString::Offsets & needle_offsets,
                               const UInt8 escape_char,
                               const std::string & match_type,
                               const TiDB::TiDBCollatorPtr & collator,
                               PaddedPODArray<UInt64> & res)
    {
        if (escape_char != CH_ESCAPE_CHAR || !match_type.empty() || collator != nullptr)
            throw Exception("PositionImpl don't support customized escape char/match_type argument/tidb collator", ErrorCodes::NOT_IMPLEMENTED);
        // NOTE You could use haystack indexing. But this is a rare case.

        ColumnString::Offset prev_needle_offset = 0;

        size_t size = needle_offsets.size();

        for (size_t i = 0; i < size; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;

            if (0 == needle_size)
            {
                res[i] = 1;
            }
            else
            {
                typename Impl::SearcherInSmallHaystack searcher = Impl::createSearcherInSmallHaystack(
                    reinterpret_cast<const char *>(&needle_data[prev_needle_offset]),
                    needle_offsets[i] - prev_needle_offset - 1);

                size_t pos = searcher.search(reinterpret_cast<const UInt8 *>(haystack.data()),
                                             reinterpret_cast<const UInt8 *>(haystack.data()) + haystack.size())
                    - reinterpret_cast<const UInt8 *>(haystack.data());

                if (pos != haystack.size())
                {
                    res[i] = 1 + Impl::countChars(haystack.data(), haystack.data() + pos);
                }
                else
                    res[i] = 0;
            }

            prev_needle_offset = needle_offsets[i];
        }
    }
};

static re2_st::RE2::Options getDefaultRe2Options()
{
    re2_st::RE2::Options options(re2_st::RE2::CannedOptions::DefaultOptions);
    options.set_case_sensitive(true);
    options.set_one_line(true);
    options.set_dot_nl(false);
    return options;
}

static String getRE2ModeModifiers(const std::string & match_type, const TiDB::TiDBCollatorPtr collator)
{
    /// for regexp only ci/cs is supported
    re2_st::RE2::Options options = getDefaultRe2Options();
    if (collator != nullptr && collator->isCI())
        options.set_case_sensitive(false);

    /// match_type can overwrite collator
    if (!match_type.empty())
    {
        for (const auto & c : match_type)
        {
            switch (c)
            {
            case 'i':
                /// according to MySQL doc: if either argument is a binary string, the arguments are handled in
                /// case-sensitive fashion as binary strings, even if match_type contains the i character.
                /// However, test in MySQL 8.0.25 shows that i flag still take affect even if the collation is binary,
                if (collator == nullptr || !collator->isBinary())
                    options.set_case_sensitive(false);
                break;
            case 'c':
                options.set_case_sensitive(true);
                break;
            case 's':
                options.set_dot_nl(true);
                break;
            case 'm':
                options.set_one_line(false);
                break;
            default:
                throw Exception("Incorrect arguments to regexp related functions.");
            }
        }
    }
    if (!options.one_line() || options.dot_nl() || !options.case_sensitive())
    {
        String mode_modifiers("(?");
        if (!options.one_line())
            mode_modifiers += "m";
        if (!options.case_sensitive())
            mode_modifiers += "i";
        if (options.dot_nl())
            mode_modifiers += "s";
        mode_modifiers += ")";
        return mode_modifiers;
    }
    else
        return "";
}

/// Is the LIKE expression reduced to finding a substring in a string?
inline bool likePatternIsStrstr(const String & pattern, String & res)
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
            auto matcher = collator->pattern();
            matcher->compile(orig_pattern, escape_char);
            size_t size = offsets.size();
            size_t prev_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                res[i] = revert ^ matcher->match(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1);
                prev_offset = offsets[i];
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
                String mode_modifiers = getRE2ModeModifiers(match_type, collator);
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
                                re2_st::StringPiece(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1),
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
                                    ^ regexp->getRE2()->Match(re2_st::StringPiece(str_data, str_size),
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
                String mode_modifiers = getRE2ModeModifiers(match_type, collator);
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
            std::string haystack_str(reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]), haystack_size);
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

    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       const std::string & pattern,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size() / 5);
        res_offsets.resize(offsets.size());

        int flags = OptimizedRegularExpression::RE_DOT_NL;
        const auto & regexp = Regexps::get<false, false>(pattern, flags);

        unsigned capture = regexp->getNumberOfSubpatterns() > 0 ? 1 : 0;
        OptimizedRegularExpression::MatchVec matches;
        matches.reserve(capture + 1);
        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t cur_offset = offsets[i];

            unsigned count
                = regexp->match(reinterpret_cast<const char *>(&data[prev_offset]), cur_offset - prev_offset - 1, matches, capture + 1);
            if (count > capture && matches[capture].offset != std::string::npos)
            {
                const auto & match = matches[capture];
                res_data.resize(res_offset + match.length + 1);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[prev_offset + match.offset], match.length);
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


/** Replace all matches of regexp 'needle' to string 'replacement'. 'needle' and 'replacement' are constants.
  * 'replacement' could contain substitutions, for example: '\2-\3-\1'
  */
template <bool replace_one = false>
struct ReplaceRegexpImpl
{
    static constexpr bool support_non_const_needle = false;
    static constexpr bool support_non_const_replacement = false;
    /// need customized escape char when do the string search
    static const bool need_customized_escape_char = false;
    /// support match type when do the string search, used in regexp
    static const bool support_match_type = true;

    /// Sequence of instructions, describing how to get resulting string.
    /// Each element is either:
    /// - substitution (in that case first element of pair is their number and second element is empty)
    /// - string that need to be inserted (in that case, first element of pair is that string and second element is -1)
    using Instructions = std::vector<std::pair<int, std::string>>;

    static const size_t max_captures = 10;

    static Instructions createInstructions(const std::string & s, int num_captures)
    {
        Instructions instructions;

        String now;
        for (size_t i = 0; i < s.size(); ++i)
        {
            if (s[i] == '\\' && i + 1 < s.size())
            {
                if (isNumericASCII(s[i + 1])) /// Substitution
                {
                    if (!now.empty())
                    {
                        instructions.emplace_back(-1, now);
                        now = "";
                    }
                    instructions.emplace_back(s[i + 1] - '0', String());
                }
                else
                    now += s[i + 1]; /// Escaping
                ++i;
            }
            else
                now += s[i]; /// Plain character
        }

        if (!now.empty())
        {
            instructions.emplace_back(-1, now);
            now = "";
        }

        for (const auto & it : instructions)
            if (it.first >= num_captures)
                throw Exception("Invalid replace instruction in replacement string. Id: " + toString(it.first) + ", but regexp has only "
                                    + toString(num_captures - 1)
                                    + " subpatterns",
                                ErrorCodes::BAD_ARGUMENTS);

        return instructions;
    }


    static void processString(const re2_st::StringPiece & input,
                              ColumnString::Chars_t & res_data,
                              ColumnString::Offset & res_offset,
                              const Int64 & pos,
                              const Int64 & occ,
                              re2_st::RE2 & searcher,
                              int num_captures,
                              const Instructions & instructions)
    {
        re2_st::StringPiece matches[max_captures];

        size_t start_pos = pos <= 0 ? 0 : pos - 1;
        Int64 match_occ = 0;
        size_t prefix_length = std::min(start_pos, static_cast<size_t>(input.length()));
        if (prefix_length > 0)
        {
            /// Copy prefix
            res_data.resize(res_data.size() + prefix_length);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data(), prefix_length);
            res_offset += prefix_length;
        }
        while (start_pos < static_cast<size_t>(input.length()))
        {
            /// If no more replacements possible for current string
            bool can_finish_current_string = false;

            if (searcher.Match(input, start_pos, input.length(), re2_st::RE2::Anchor::UNANCHORED, matches, num_captures))
            {
                match_occ++;
                /// if occ > 0, it will replace all the match expr, otherwise it only replace the occ-th match
                if (occ == 0 || match_occ == occ)
                {
                    const auto & match = matches[0];
                    size_t bytes_to_copy = (match.data() - input.data()) - start_pos;

                    /// Copy prefix before matched regexp without modification
                    res_data.resize(res_data.size() + bytes_to_copy);
                    memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data() + start_pos, bytes_to_copy);
                    res_offset += bytes_to_copy;
                    start_pos += bytes_to_copy + match.length();

                    /// Do substitution instructions
                    for (const auto & it : instructions)
                    {
                        if (it.first >= 0)
                        {
                            res_data.resize(res_data.size() + matches[it.first].length());
                            memcpy(&res_data[res_offset], matches[it.first].data(), matches[it.first].length());
                            res_offset += matches[it.first].length();
                        }
                        else
                        {
                            res_data.resize(res_data.size() + it.second.size());
                            memcpy(&res_data[res_offset], it.second.data(), it.second.size());
                            res_offset += it.second.size();
                        }
                    }

                    /// when occ > 0, just replace the occ-th match even if replace_one is false
                    if (replace_one || match.length() == 0) /// Stop after match of zero length, to avoid infinite loop.
                        can_finish_current_string = true;
                }
                else
                {
                    const auto & match = matches[0];
                    size_t bytes_to_copy = (match.data() - input.data()) - start_pos + match.length();

                    /// Copy the matched string without modification
                    res_data.resize(res_data.size() + bytes_to_copy);
                    memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data() + start_pos, bytes_to_copy);
                    res_offset += bytes_to_copy;
                    start_pos += bytes_to_copy;
                    if (match.length() == 0)
                        can_finish_current_string = true;
                }
            }
            else
                can_finish_current_string = true;

            /// If ready, append suffix after match to end of string.
            if (can_finish_current_string)
            {
                res_data.resize(res_data.size() + input.length() - start_pos);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], input.data() + start_pos, input.length() - start_pos);
                res_offset += input.length() - start_pos;
                start_pos = input.length();
            }
        }

        res_data.resize(res_data.size() + 1);
        res_data[res_offset] = 0;
        ++res_offset;
    }


    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       const std::string & needle,
                       const std::string & replacement,
                       const Int64 & pos,
                       const Int64 & occ,
                       const std::string & match_type,
                       TiDB::TiDBCollatorPtr collator,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
    {
        ColumnString::Offset res_offset = 0;
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        if (needle.empty())
        {
            /// Copy all the data without changing.
            res_data.resize(data.size());
            const UInt8 * begin = &data[0];
            memcpy(&res_data[0], begin, data.size());
            memcpy(&res_offsets[0], &offsets[0], size * sizeof(UInt64));
            return;
        }

        String updated_needle = needle;
        if (!match_type.empty() || collator != nullptr)
        {
            String mode_modifiers = getRE2ModeModifiers(match_type, collator);
            if (!mode_modifiers.empty())
                updated_needle = mode_modifiers + updated_needle;
        }
        re2_st::RE2 searcher(updated_needle);
        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, static_cast<int>(max_captures));

        Instructions instructions = createInstructions(replacement, num_captures);

        /// Cannot perform search for whole block. Will process each string separately.
        for (size_t i = 0; i < size; ++i)
        {
            int from = i > 0 ? offsets[i - 1] : 0;
            re2_st::StringPiece input(reinterpret_cast<const char *>(&data[0] + from), offsets[i] - from - 1);

            processString(input, res_data, res_offset, pos, occ, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }

    static void vectorFixed(const ColumnString::Chars_t & data,
                            size_t n,
                            const std::string & needle,
                            const std::string & replacement,
                            const Int64 & pos,
                            const Int64 & occ,
                            const std::string & match_type,
                            TiDB::TiDBCollatorPtr collator,
                            ColumnString::Chars_t & res_data,
                            ColumnString::Offsets & res_offsets)
    {
        ColumnString::Offset res_offset = 0;
        size_t size = data.size() / n;
        res_data.reserve(data.size());
        res_offsets.resize(size);

        if (needle.empty())
        {
            /// TODO: copy all the data without changing
            throw Exception("Length of the second argument of function replace must be greater than 0.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        String updated_needle = needle;
        if (!match_type.empty() || collator != nullptr)
        {
            String mode_modifiers = getRE2ModeModifiers(match_type, collator);
            if (!mode_modifiers.empty())
                updated_needle = mode_modifiers + updated_needle;
        }
        re2_st::RE2 searcher(updated_needle);
        int num_captures = std::min(searcher.NumberOfCapturingGroups() + 1, static_cast<int>(max_captures));

        Instructions instructions = createInstructions(replacement, num_captures);

        for (size_t i = 0; i < size; ++i)
        {
            int from = i * n;
            re2_st::StringPiece input(reinterpret_cast<const char *>(&data[0] + from), n);

            processString(input, res_data, res_offset, pos, occ, searcher, num_captures, instructions);
            res_offsets[i] = res_offset;
        }
    }
    static void constant(const String & input, const String & needle, const String & replacement, const Int64 & pos, const Int64 & occ, const String & match_type, TiDB::TiDBCollatorPtr collator, String & output)
    {
        ColumnString::Chars_t input_data;
        input_data.insert(input_data.end(), input.begin(), input.end());
        ColumnString::Offsets input_offsets;
        input_offsets.push_back(input_data.size() + 1);
        ColumnString::Chars_t output_data;
        ColumnString::Offsets output_offsets;
        vector(input_data, input_offsets, needle, replacement, pos, occ, match_type, collator, output_data, output_offsets);
        output = String(reinterpret_cast<const char *>(&output_data[0]), output_offsets[0] - 1);
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

<<<<<<< HEAD
    static void vector(const ColumnString::Chars_t & data,
                       const ColumnString::Offsets & offsets,
                       const std::string & needle,
                       const std::string & replacement,
                       const Int64 & /* pos */,
                       const Int64 & /* occ */,
                       const std::string & /* match_type */,
                       TiDB::TiDBCollatorPtr /* collator */,
                       ColumnString::Chars_t & res_data,
                       ColumnString::Offsets & res_offsets)
=======
    static void vector(
        const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
>>>>>>> 11ce13fffa (fix error when first argument of replace function is const (#9615))
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
<<<<<<< HEAD
    static void vectorFixed(const ColumnString::Chars_t & data,
                            size_t n,
                            const std::string & needle,
                            const std::string & replacement,
                            const Int64 & /* pos */,
                            const Int64 & /* occ */,
                            const std::string & /* match_type */,
                            TiDB::TiDBCollatorPtr /* collator */,
                            ColumnString::Chars_t & res_data,
                            ColumnString::Offsets & res_offsets)
=======
    static void vectorFixed(
        const ColumnString::Chars_t & data,
        size_t n,
        const std::string & needle,
        const std::string & replacement,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
>>>>>>> 11ce13fffa (fix error when first argument of replace function is const (#9615))
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

<<<<<<< HEAD
    static void constant(const std::string & data, const std::string & needle, const std::string & replacement, const Int64 & /* pos */, const Int64 & /* occ */, const std::string & /* match_type */, TiDB::TiDBCollatorPtr /* collator */, std::string & res_data)
=======
    static void constant(
        const std::string & data,
        const std::string & needle,
        const std::string & replacement,
        std::string & res_data)
>>>>>>> 11ce13fffa (fix error when first argument of replace function is const (#9615))
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


template <typename Impl, typename Name>
class FunctionStringReplace : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionStringReplace>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (Impl::support_non_const_needle && Impl::support_non_const_replacement)
        {
            return {3, 4, 5};
        }
        else if constexpr (Impl::support_non_const_needle)
        {
            return {2, 3, 4, 5};
        }
        else if constexpr (Impl::support_non_const_replacement)
        {
            return {1, 3, 4, 5};
        }
        else
        {
            return {1, 2, 3, 4, 5};
        }
    }
    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override { collator = collator_; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[2]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() > 3 && !arguments[3]->isInteger())
            throw Exception("Illegal type " + arguments[2]->getName() + " of forth argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() > 4 && !arguments[4]->isInteger())
            throw Exception("Illegal type " + arguments[2]->getName() + " of fifth argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() > 5 && !arguments[5]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[2]->getName() + " of sixth argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const ColumnPtr & column_src = block.getByPosition(arguments[0]).column;
        const ColumnPtr & column_needle = block.getByPosition(arguments[1]).column;
        const ColumnPtr & column_replacement = block.getByPosition(arguments[2]).column;
        const ColumnPtr column_pos = arguments.size() > 3 ? block.getByPosition(arguments[3]).column : nullptr;
        const ColumnPtr column_occ = arguments.size() > 4 ? block.getByPosition(arguments[4]).column : nullptr;
        const ColumnPtr column_match_type = arguments.size() > 5 ? block.getByPosition(arguments[5]).column : nullptr;

        if ((column_pos != nullptr && !column_pos->isColumnConst())
            || (column_occ != nullptr && !column_occ->isColumnConst())
            || (column_match_type != nullptr && !column_match_type->isColumnConst()))
            throw Exception("4th, 5th, 6th arguments of function " + getName() + " must be constants.");
        Int64 pos = column_pos == nullptr ? 1 : typeid_cast<const ColumnConst *>(column_pos.get())->getInt(0);
        Int64 occ = column_occ == nullptr ? 0 : typeid_cast<const ColumnConst *>(column_occ.get())->getInt(0);
        String match_type = column_match_type == nullptr ? "" : typeid_cast<const ColumnConst *>(column_match_type.get())->getValue<String>();

        ColumnWithTypeAndName & column_result = block.getByPosition(result);

        bool needle_const = column_needle->isColumnConst();
        bool replacement_const = column_replacement->isColumnConst();

        if (needle_const && replacement_const)
        {
            executeImpl(column_src, column_needle, column_replacement, pos, occ, match_type, column_result);
        }
        else if (needle_const)
        {
            executeImplNonConstReplacement(column_src, column_needle, column_replacement, pos, occ, match_type, column_result);
        }
        else if (replacement_const)
        {
            executeImplNonConstNeedle(column_src, column_needle, column_replacement, pos, occ, match_type, column_result);
        }
        else
        {
            executeImplNonConstNeedleReplacement(column_src, column_needle, column_replacement, pos, occ, match_type, column_result);
        }
    }

private:
    void executeImpl(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos,
        Int64 occ,
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        const ColumnConst * c1_const = typeid_cast<const ColumnConst *>(column_needle.get());
        const ColumnConst * c2_const = typeid_cast<const ColumnConst *>(column_replacement.get());
        String needle = c1_const->getValue<String>();
        String replacement = c2_const->getValue<String>();

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector(col->getChars(), col->getOffsets(), needle, replacement, pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
            column_result.column = std::move(col_res);
        }
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vectorFixed(col->getChars(), col->getN(), needle, replacement, pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
            column_result.column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    void executeImplNonConstNeedle(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos [[maybe_unused]],
        Int64 occ [[maybe_unused]],
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        if constexpr (Impl::support_non_const_needle)
        {
            const ColumnString * col_needle = typeid_cast<const ColumnString *>(column_needle.get());
            const ColumnConst * col_replacement_const = typeid_cast<const ColumnConst *>(column_replacement.get());
            String replacement = col_replacement_const->getValue<String>();

            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorNonConstNeedle(col->getChars(), col->getOffsets(), col_needle->getChars(), col_needle->getOffsets(), replacement, pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorFixedNonConstNeedle(col->getChars(), col->getN(), col_needle->getChars(), col_needle->getOffsets(), replacement, pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else
                throw Exception(
                    "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            throw Exception("Argument at index 2 for function replace must be constant", ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    void executeImplNonConstReplacement(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos [[maybe_unused]],
        Int64 occ [[maybe_unused]],
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        if constexpr (Impl::support_non_const_replacement)
        {
            const ColumnConst * col_needle_const = typeid_cast<const ColumnConst *>(column_needle.get());
            String needle = col_needle_const->getValue<String>();
            const ColumnString * col_replacement = typeid_cast<const ColumnString *>(column_replacement.get());

            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorNonConstReplacement(col->getChars(), col->getOffsets(), needle, col_replacement->getChars(), col_replacement->getOffsets(), pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorFixedNonConstReplacement(col->getChars(), col->getN(), needle, col_replacement->getChars(), col_replacement->getOffsets(), pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else
                throw Exception(
                    "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            throw Exception("Argument at index 3 for function replace must be constant", ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    void executeImplNonConstNeedleReplacement(
        const ColumnPtr & column_src,
        const ColumnPtr & column_needle,
        const ColumnPtr & column_replacement,
        Int64 pos [[maybe_unused]],
        Int64 occ [[maybe_unused]],
        const String & match_type,
        ColumnWithTypeAndName & column_result) const
    {
        if constexpr (Impl::support_non_const_needle && Impl::support_non_const_replacement)
        {
            const ColumnString * col_needle = typeid_cast<const ColumnString *>(column_needle.get());
            const ColumnString * col_replacement = typeid_cast<const ColumnString *>(column_replacement.get());

            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorNonConstNeedleReplacement(col->getChars(), col->getOffsets(), col_needle->getChars(), col_needle->getOffsets(), col_replacement->getChars(), col_replacement->getOffsets(), pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column_src.get()))
            {
                auto col_res = ColumnString::create();
                Impl::vectorFixedNonConstNeedleReplacement(col->getChars(), col->getN(), col_needle->getChars(), col_needle->getOffsets(), col_replacement->getChars(), col_replacement->getOffsets(), pos, occ, match_type, collator, col_res->getChars(), col_res->getOffsets());
                column_result.column = std::move(col_res);
            }
            else
                throw Exception(
                    "Illegal column " + column_src->getName() + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            throw Exception("Argument at index 2 and 3 for function replace must be constant", ErrorCodes::ILLEGAL_COLUMN);
        }
    }
    TiDB::TiDBCollatorPtr collator;
};

struct NamePosition
{
    static constexpr auto name = "position";
};
struct NamePositionUTF8
{
    static constexpr auto name = "positionUTF8";
};
struct NamePositionCaseInsensitive
{
    static constexpr auto name = "positionCaseInsensitive";
};
struct NamePositionCaseInsensitiveUTF8
{
    static constexpr auto name = "positionCaseInsensitiveUTF8";
};
struct NameMatch
{
    static constexpr auto name = "match";
};

struct NameTiDBRegexp
{
    static constexpr auto name = "regexp";
};

struct NameLike
{
    static constexpr auto name = "like";
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
struct NameReplaceRegexpOne
{
    static constexpr auto name = "replaceRegexpOne";
};
struct NameReplaceRegexpAll
{
    static constexpr auto name = "replaceRegexpAll";
};

// using FunctionPosition = FunctionsStringSearch<PositionImpl<PositionCaseSensitiveASCII>, NamePosition>;
using FunctionPositionUTF8 = FunctionsStringSearch<PositionImpl<PositionCaseSensitiveUTF8>, NamePositionUTF8>;
using FunctionPositionCaseInsensitive = FunctionsStringSearch<PositionImpl<PositionCaseInsensitiveASCII>, NamePositionCaseInsensitive>;
using FunctionPositionCaseInsensitiveUTF8
    = FunctionsStringSearch<PositionImpl<PositionCaseInsensitiveUTF8>, NamePositionCaseInsensitiveUTF8>;

using FunctionMatch = FunctionsStringSearch<MatchImpl<false>, NameMatch>;
using FunctionTiDBRegexp = FunctionsStringSearch<MatchImpl<false, false, true>, NameTiDBRegexp>;
using FunctionLike = FunctionsStringSearch<MatchImpl<true>, NameLike>;
using FunctionLike3Args = FunctionsStringSearch<MatchImpl<true, false, true>, NameLike3Args>;
using FunctionNotLike = FunctionsStringSearch<MatchImpl<true, true>, NameNotLike>;
using FunctionExtract = FunctionsStringSearchToString<ExtractImpl, NameExtract>;
using FunctionReplaceOne = FunctionStringReplace<ReplaceStringImpl<true>, NameReplaceOne>;
using FunctionReplaceAll = FunctionStringReplace<ReplaceStringImpl<false>, NameReplaceAll>;
using FunctionReplaceRegexpOne = FunctionStringReplace<ReplaceRegexpImpl<true>, NameReplaceRegexpOne>;
using FunctionReplaceRegexpAll = FunctionStringReplace<ReplaceRegexpImpl<false>, NameReplaceRegexpAll>;


void registerFunctionsStringSearch(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplaceOne>();
    factory.registerFunction<FunctionReplaceAll>();
    factory.registerFunction<FunctionReplaceRegexpOne>();
    factory.registerFunction<FunctionReplaceRegexpAll>();
    // factory.registerFunction<FunctionPosition>();
    factory.registerFunction<FunctionPositionUTF8>();
    factory.registerFunction<FunctionPositionCaseInsensitive>();
    factory.registerFunction<FunctionPositionCaseInsensitiveUTF8>();
    factory.registerFunction<FunctionMatch>();
    factory.registerFunction<FunctionTiDBRegexp>();
    factory.registerFunction<FunctionLike>();
    factory.registerFunction<FunctionLike3Args>();
    factory.registerFunction<FunctionNotLike>();
    factory.registerFunction<FunctionExtract>();
}
} // namespace DB
