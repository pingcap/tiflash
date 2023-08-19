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

#include <Common/Exception.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Poco/Exception.h>
#include <common/StringRef.h>
#include <common/defines.h>
#include <common/types.h>

#include <iostream>
#include <optional>

#define MIN_LENGTH_FOR_STRSTR 3
#define MAX_SUBPATTERNS 5


template <bool thread_safe>
void OptimizedRegularExpressionImpl<thread_safe>::analyze(
    const std::string & regexp,
    std::string & required_substring,
    bool & is_trivial,
    bool & required_substring_is_prefix)
{
    /** The expression is trivial if all the metacharacters in it are escaped.
      * The non-alternative string is
      *  a string outside parentheses,
      *  in which all metacharacters are escaped,
      *  and also if there are no '|' outside the brackets,
      *  and also avoid substrings of the form `http://` or `www` and some other
      *   (this is the hack for typical use case in Yandex.Metrica).
      */
    const char * begin = regexp.data();
    const char * pos = begin;
    const char * end = regexp.data() + regexp.size();
    int depth = 0;
    is_trivial = true;
    required_substring_is_prefix = false;
    required_substring.clear();
    bool has_alternative_on_depth_0 = false;

    /// Substring with a position.
    using Substring = std::pair<std::string, size_t>;
    using Substrings = std::vector<Substring>;

    Substrings trivial_substrings(1);
    Substring * last_substring = &trivial_substrings.back();

    bool in_curly_braces = false;
    bool in_square_braces = false;

    while (pos != end)
    {
        switch (*pos)
        {
        case '\0':
            pos = end;
            break;

        case '\\':
        {
            ++pos;
            if (pos == end)
                break;

            switch (*pos)
            {
            case '|':
            case '(':
            case ')':
            case '^':
            case '$':
            case '.':
            case '[':
            case '?':
            case '*':
            case '+':
            case '{':
                if (depth == 0 && !in_curly_braces && !in_square_braces)
                {
                    if (last_substring->first.empty())
                        last_substring->second = pos - begin;
                    last_substring->first.push_back(*pos);
                }
                break;
            default:
                /// all other escape sequences are not supported
                is_trivial = false;
                if (!last_substring->first.empty())
                {
                    trivial_substrings.resize(trivial_substrings.size() + 1);
                    last_substring = &trivial_substrings.back();
                }
                break;
            }

            ++pos;
            break;
        }

        case '|':
            if (depth == 0)
                has_alternative_on_depth_0 = true;
            is_trivial = false;
            if (!in_square_braces && !last_substring->first.empty())
            {
                trivial_substrings.resize(trivial_substrings.size() + 1);
                last_substring = &trivial_substrings.back();
            }
            ++pos;
            break;

        case '(':
            if (!in_square_braces)
            {
                ++depth;
                is_trivial = false;
                if (!last_substring->first.empty())
                {
                    trivial_substrings.resize(trivial_substrings.size() + 1);
                    last_substring = &trivial_substrings.back();
                }
            }
            ++pos;
            break;

        case '[':
            in_square_braces = true;
            ++depth;
            is_trivial = false;
            if (!last_substring->first.empty())
            {
                trivial_substrings.resize(trivial_substrings.size() + 1);
                last_substring = &trivial_substrings.back();
            }
            ++pos;
            break;

        case ']':
            if (!in_square_braces)
                goto ordinary;

            in_square_braces = false;
            --depth;
            is_trivial = false;
            if (!last_substring->first.empty())
            {
                trivial_substrings.resize(trivial_substrings.size() + 1);
                last_substring = &trivial_substrings.back();
            }
            ++pos;
            break;

        case ')':
            if (!in_square_braces)
            {
                --depth;
                is_trivial = false;
                if (!last_substring->first.empty())
                {
                    trivial_substrings.resize(trivial_substrings.size() + 1);
                    last_substring = &trivial_substrings.back();
                }
            }
            ++pos;
            break;

        case '^':
        case '$':
        case '.':
        case '+':
            is_trivial = false;
            if (!last_substring->first.empty() && !in_square_braces)
            {
                trivial_substrings.resize(trivial_substrings.size() + 1);
                last_substring = &trivial_substrings.back();
            }
            ++pos;
            break;

        /// Quantifiers that allow a zero number of occurences.
        case '{':
            in_curly_braces = true;
            [[fallthrough]];
        case '?':
            [[fallthrough]];
        case '*':
            is_trivial = false;
            if (!last_substring->first.empty() && !in_square_braces)
            {
                last_substring->first.resize(last_substring->first.size() - 1);
                trivial_substrings.resize(trivial_substrings.size() + 1);
                last_substring = &trivial_substrings.back();
            }
            ++pos;
            break;

        case '}':
            if (!in_curly_braces)
                goto ordinary;

            in_curly_braces = false;
            ++pos;
            break;

        ordinary: /// Normal, not escaped symbol.
            [[fallthrough]];
        default:
            if (depth == 0 && !in_curly_braces && !in_square_braces)
            {
                if (last_substring->first.empty())
                    last_substring->second = pos - begin;
                last_substring->first.push_back(*pos);
            }
            ++pos;
            break;
        }
    }

    if (last_substring && last_substring->first.empty())
        trivial_substrings.pop_back();

    if (!is_trivial)
    {
        if (!has_alternative_on_depth_0)
        {
            /** We choose the non-alternative substring of the maximum length, among the prefixes,
              *  or a non-alternative substring of maximum length.
              */
            size_t max_length = 0;
            Substrings::const_iterator candidate_it = trivial_substrings.begin();
            for (Substrings::const_iterator it = trivial_substrings.begin(); it != trivial_substrings.end(); ++it)
            {
                if (((it->second == 0 && candidate_it->second != 0)
                     || ((it->second == 0) == (candidate_it->second == 0) && it->first.size() > max_length))
                    /// Tuning for typical usage domain
                    && (it->first.size() > strlen("://") || strncmp(it->first.data(), "://", strlen("://")))
                    && (it->first.size() > strlen("http://") || strncmp(it->first.data(), "http", strlen("http")))
                    && (it->first.size() > strlen("www.") || strncmp(it->first.data(), "www", strlen("www")))
                    && (it->first.size() > strlen("Windows ") || strncmp(it->first.data(), "Windows ", strlen("Windows "))))
                {
                    max_length = it->first.size();
                    candidate_it = it;
                }
            }

            if (max_length >= MIN_LENGTH_FOR_STRSTR)
            {
                required_substring = candidate_it->first;
                required_substring_is_prefix = candidate_it->second == 0;
            }
        }
    }
    else if (!trivial_substrings.empty())
    {
        required_substring = trivial_substrings.front().first;
        required_substring_is_prefix = trivial_substrings.front().second == 0;
    }

    /*    std::cerr
        << "regexp: " << regexp
        << ", is_trivial: " << is_trivial
        << ", required_substring: " << required_substring
        << ", required_substring_is_prefix: " << required_substring_is_prefix
        << std::endl;*/
}


template <bool thread_safe>
OptimizedRegularExpressionImpl<thread_safe>::OptimizedRegularExpressionImpl(const std::string & regexp_, int options)
{
    if (options & RE_NO_OPTIMIZE)
    {
        /// query from TiDB, currently, since analyze does not handle all the cases, skip the optimization
        /// to avoid im-compatible issues
        is_trivial = false;
        required_substring.clear();
        required_substring_is_prefix = false;
    }
    else
    {
        analyze(regexp_, required_substring, is_trivial, required_substring_is_prefix);
    }

    /// Just four following options are supported
    if (options & (~(RE_CASELESS | RE_NO_CAPTURE | RE_DOT_NL | RE_NO_OPTIMIZE)))
        throw Poco::Exception("OptimizedRegularExpression: Unsupported option.");

    is_case_insensitive = options & RE_CASELESS;
    bool is_no_capture = options & RE_NO_CAPTURE;
    bool is_dot_nl = options & RE_DOT_NL;

    number_of_subpatterns = 0;
    if (!is_trivial)
    {
        /// Compile the re2 regular expression.
        typename RegexType::Options options;

        if (is_case_insensitive)
            options.set_case_sensitive(false);

        if (is_dot_nl)
            options.set_dot_nl(true);

        re2 = std::make_unique<RegexType>(regexp_, options);
        if (!re2->ok())
            throw Poco::Exception("OptimizedRegularExpression: cannot compile re2: " + regexp_ + ", error: " + re2->error());

        if (!is_no_capture)
        {
            number_of_subpatterns = re2->NumberOfCapturingGroups();
            if (number_of_subpatterns > MAX_SUBPATTERNS)
                throw Poco::Exception("OptimizedRegularExpression: too many subpatterns in regexp: " + regexp_);
        }
    }
}


template <bool thread_safe>
bool OptimizedRegularExpressionImpl<thread_safe>::match(const char * subject, size_t subject_size) const
{
    if (is_trivial)
    {
        if (is_case_insensitive)
            return nullptr != strcasestr(subject, required_substring.data());
        else
            return nullptr != strstr(subject, required_substring.data());
    }
    else
    {
        if (!required_substring.empty())
        {
            const char * pos;
            if (is_case_insensitive)
                pos = strcasestr(subject, required_substring.data());
            else
                pos = strstr(subject, required_substring.data());

            if (nullptr == pos)
                return false;
        }

        return re2->Match(StringPieceType(subject, subject_size), 0, subject_size, RegexType::UNANCHORED, nullptr, 0);
    }
}


template <bool thread_safe>
bool OptimizedRegularExpressionImpl<thread_safe>::match(const char * subject, size_t subject_size, Match & match) const
{
    if (is_trivial)
    {
        const char * pos;
        if (is_case_insensitive)
            pos = strcasestr(subject, required_substring.data());
        else
            pos = strstr(subject, required_substring.data());

        if (pos == nullptr)
            return false;
        else
        {
            match.offset = pos - subject;
            match.length = required_substring.size();
            return true;
        }
    }
    else
    {
        if (!required_substring.empty())
        {
            const char * pos;
            if (is_case_insensitive)
                pos = strcasestr(subject, required_substring.data());
            else
                pos = strstr(subject, required_substring.data());

            if (nullptr == pos)
                return false;
        }

        StringPieceType piece;

        if (!RegexType::PartialMatch(StringPieceType(subject, subject_size), *re2, &piece))
            return false;
        else
        {
            match.offset = piece.data() - subject;
            match.length = piece.length();
            return true;
        }
    }
}


template <bool thread_safe>
unsigned OptimizedRegularExpressionImpl<thread_safe>::match(const char * subject, size_t subject_size, MatchVec & matches, unsigned limit) const
{
    matches.clear();

    if (limit == 0)
        return 0;

    if (limit > number_of_subpatterns + 1)
        limit = number_of_subpatterns + 1;

    if (is_trivial)
    {
        const char * pos;
        if (is_case_insensitive)
            pos = strcasestr(subject, required_substring.data());
        else
            pos = strstr(subject, required_substring.data());

        if (pos == nullptr)
            return 0;
        else
        {
            Match match;
            match.offset = pos - subject;
            match.length = required_substring.size();
            matches.push_back(match);
            return 1;
        }
    }
    else
    {
        if (!required_substring.empty())
        {
            const char * pos;
            if (is_case_insensitive)
                pos = strcasestr(subject, required_substring.data());
            else
                pos = strstr(subject, required_substring.data());

            if (nullptr == pos)
                return 0;
        }

        StringPieceType pieces[MAX_SUBPATTERNS];

        if (!re2->Match(StringPieceType(subject, subject_size), 0, subject_size, RegexType::UNANCHORED, pieces, limit))
            return 0;
        else
        {
            matches.resize(limit);
            for (size_t i = 0; i < limit; ++i)
            {
                if (pieces[i] != nullptr)
                {
                    matches[i].offset = pieces[i].data() - subject;
                    matches[i].length = pieces[i].length();
                }
                else
                {
                    matches[i].offset = std::string::npos;
                    matches[i].length = 0;
                }
            }
            return limit;
        }
    }
}

template <bool thread_safe>
Int64 OptimizedRegularExpressionImpl<thread_safe>::processInstrEmptyStringExpr(const char * expr, size_t expr_size, size_t pos, Int64 occur)
{
    if (occur != 1)
        return 0;

    StringPieceType expr_sp(expr, expr_size);
    return RegexType::FindAndConsume(&expr_sp, *re2) ? pos : 0;
}

template <bool thread_safe>
std::optional<StringRef> OptimizedRegularExpressionImpl<thread_safe>::processSubstrEmptyStringExpr(const char * expr, size_t expr_size, size_t byte_pos, Int64 occur)
{
    if (occur != 1 || byte_pos != 1)
        return std::nullopt;

    StringPieceType expr_sp(expr, expr_size);
    StringPieceType matched_str;
    if (!RegexType::FindAndConsume(&expr_sp, *re2, &matched_str))
        return std::nullopt;

    return std::optional<StringRef>(StringRef(matched_str.data(), matched_str.size()));
}

static inline void checkInstrArgs(Int64 utf8_total_len, size_t subject_size, Int64 pos, Int64 ret_op)
{
    RUNTIME_CHECK_MSG(!(ret_op != 0 && ret_op != 1), "Incorrect argument to regexp function: return_option must be 1 or 0");
    RUNTIME_CHECK_MSG(!(pos <= 0 || (pos > utf8_total_len && subject_size != 0)), "Index out of bounds in regular function.");
}

static inline void checkSubstrArgs(Int64 utf8_total_len, size_t subject_size, Int64 pos)
{
    RUNTIME_CHECK_MSG(!(pos <= 0 || (pos > utf8_total_len && subject_size != 0)), "Index out of bounds in regular function.");
}

static inline void makeOccurValid(Int64 & occur)
{
    occur = occur < 1 ? 1 : occur;
}

template <bool thread_safe>
Int64 OptimizedRegularExpressionImpl<thread_safe>::instrImpl(const char * subject, size_t subject_size, Int64 byte_pos, Int64 occur, Int64 ret_op)
{
    size_t byte_offset = byte_pos - 1; // This is a offset for bytes, not utf8
    const char * expr = subject + byte_offset; // expr is the string actually passed into regexp to be matched
    size_t expr_size = subject_size - byte_offset;

    StringPieceType expr_sp(expr, expr_size);
    StringPieceType matched_str;

    while (occur > 0)
    {
        if (!RegexType::FindAndConsume(&expr_sp, *re2, &matched_str))
            return 0;

        --occur;
    }

    byte_offset = matched_str.data() - subject;
    return ret_op == 0 ? DB::UTF8::bytePos2Utf8Pos(reinterpret_cast<const UInt8 *>(subject), byte_offset + 1) : DB::UTF8::bytePos2Utf8Pos(reinterpret_cast<const UInt8 *>(subject), byte_offset + matched_str.size() + 1);
}

template <bool thread_safe>
std::optional<StringRef> OptimizedRegularExpressionImpl<thread_safe>::substrImpl(const char * subject, size_t subject_size, Int64 byte_pos, Int64 occur)
{
    size_t byte_offset = byte_pos - 1; // This is a offset for bytes, not utf8
    const char * expr = subject + byte_offset; // expr is the string actually passed into regexp to be matched
    size_t expr_size = subject_size - byte_offset;

    StringPieceType expr_sp(expr, expr_size);
    StringPieceType matched_str;
    while (occur > 0)
    {
        if (!RegexType::FindAndConsume(&expr_sp, *re2, &matched_str))
            return std::nullopt;

        --occur;
    }

    return std::optional<StringRef>(StringRef(matched_str.data(), matched_str.size()));
}

template <bool thread_safe>
Int64 OptimizedRegularExpressionImpl<thread_safe>::instr(const char * subject, size_t subject_size, Int64 pos, Int64 occur, Int64 ret_op)
{
    Int64 utf8_total_len = DB::UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(subject), subject_size);
    ;
    checkInstrArgs(utf8_total_len, subject_size, pos, ret_op);
    makeOccurValid(occur);

    if (unlikely(subject_size == 0))
        return processInstrEmptyStringExpr(subject, subject_size, pos, occur);

    size_t byte_pos = DB::UTF8::utf8Pos2bytePos(reinterpret_cast<const UInt8 *>(subject), pos);
    return instrImpl(subject, subject_size, byte_pos, occur, ret_op);
}

template <bool thread_safe>
std::optional<StringRef> OptimizedRegularExpressionImpl<thread_safe>::substr(const char * subject, size_t subject_size, Int64 pos, Int64 occur)
{
    Int64 utf8_total_len = DB::UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(subject), subject_size);
    checkSubstrArgs(utf8_total_len, subject_size, pos);
    makeOccurValid(occur);

    if (unlikely(subject_size == 0))
        return processSubstrEmptyStringExpr(subject, subject_size, pos, occur);

    size_t byte_pos = DB::UTF8::utf8Pos2bytePos(reinterpret_cast<const UInt8 *>(subject), pos);
    return substrImpl(subject, subject_size, byte_pos, occur);
}

#undef MIN_LENGTH_FOR_STRSTR
#undef MAX_SUBPATTERNS
