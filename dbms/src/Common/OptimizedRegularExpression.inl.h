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
#include <common/StringRef.h>
#include <common/defines.h>
#include <common/types.h>

#include <cstring>
#include <iostream>
#include <optional>

#define MIN_LENGTH_FOR_STRSTR 3
constexpr static int MAX_CAPTURES = 9;

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes
} // namespace DB

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
            auto candidate_it = trivial_substrings.begin();
            for (auto it = trivial_substrings.begin(); it != trivial_substrings.end(); ++it)
            {
                if (((it->second == 0 && candidate_it->second != 0)
                     || ((it->second == 0) == (candidate_it->second == 0) && it->first.size() > max_length))
                    /// Tuning for typical usage domain
                    && (it->first.size() > strlen("://") || strncmp(it->first.data(), "://", strlen("://")) != 0)
                    && (it->first.size() > strlen("http://") || strncmp(it->first.data(), "http", strlen("http")) != 0)
                    && (it->first.size() > strlen("www.") || strncmp(it->first.data(), "www", strlen("www")) != 0)
                    && (it->first.size() > strlen("Windows ")
                        || strncmp(it->first.data(), "Windows ", strlen("Windows ")) != 0))
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
    bool is_dot_nl = options & RE_DOT_NL;

    capture_num = 0;
    if (!is_trivial)
    {
        /// Compile the re2 regular expression.
        typename RegexType::Options reg_options;

        if (is_case_insensitive)
            reg_options.set_case_sensitive(false);

        if (is_dot_nl)
            reg_options.set_dot_nl(true);

        reg_options.set_log_errors(false);

        re2 = std::make_unique<RegexType>(regexp_, reg_options);
        if (!re2->ok())
            throw Poco::Exception(
                fmt::format("OptimizedRegularExpression: cannot compile re2: {}, error: {}", regexp_, re2->error()));

        capture_num = re2->NumberOfCapturingGroups();
        capture_num = capture_num <= MAX_CAPTURES ? capture_num : MAX_CAPTURES;
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
unsigned OptimizedRegularExpressionImpl<thread_safe>::match(
    const char * subject,
    size_t subject_size,
    MatchVec & matches,
    unsigned limit) const
{
    matches.clear();

    if (limit == 0)
        return 0;

    if (limit > capture_num + 1)
        limit = capture_num + 1;

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

        StringPieceType pieces[MAX_CAPTURES];

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
Int64 OptimizedRegularExpressionImpl<thread_safe>::processInstrEmptyStringExpr(
    const char * expr,
    size_t expr_size,
    size_t pos,
    Int64 occur)
{
    if (occur != 1)
        return 0;

    StringPieceType expr_sp(expr, expr_size);
    return RegexType::FindAndConsume(&expr_sp, *re2) ? pos : 0;
}

template <bool thread_safe>
std::optional<StringRef> OptimizedRegularExpressionImpl<thread_safe>::processSubstrEmptyStringExpr(
    const char * expr,
    size_t expr_size,
    size_t byte_pos,
    Int64 occur)
{
    if (occur != 1 || byte_pos != 1)
        return std::nullopt;

    StringPieceType expr_sp(expr, expr_size);
    StringPieceType matched_str;
    if (!RegexType::FindAndConsume(&expr_sp, *re2, &matched_str))
        return std::nullopt;

    return std::optional<StringRef>(StringRef(matched_str.data(), matched_str.size()));
}

namespace FunctionsRegexp
{
inline void checkArgPos(Int64 utf8_total_len, size_t subject_size, Int64 pos)
{
    RUNTIME_CHECK_MSG(
        !(pos <= 0 || (pos > utf8_total_len && subject_size != 0)),
        "Index out of bounds in regular function.");
}

inline void checkArgsInstr(Int64 utf8_total_len, size_t subject_size, Int64 pos, Int64 ret_op)
{
    RUNTIME_CHECK_MSG(
        !(ret_op != 0 && ret_op != 1),
        "Incorrect argument to regexp function: return_option must be 1 or 0");
    checkArgPos(utf8_total_len, subject_size, pos);
}

inline void checkArgsSubstr(Int64 utf8_total_len, size_t subject_size, Int64 pos)
{
    checkArgPos(utf8_total_len, subject_size, pos);
}

inline void checkArgsReplace(Int64 utf8_total_len, size_t subject_size, Int64 pos)
{
    RUNTIME_CHECK_MSG(
        !(pos <= 0 || (pos > utf8_total_len && subject_size != 0) || (pos != 1 && subject_size == 0)),
        "Index out of bounds in regular function.");
}

inline void makeOccurValid(Int64 & occur)
{
    occur = occur < 1 ? 1 : occur;
}

inline void makeReplaceOccurValid(Int64 & occur)
{
    occur = occur < 0 ? 1 : occur;
}
} // namespace FunctionsRegexp

template <bool thread_safe>
Int64 OptimizedRegularExpressionImpl<thread_safe>::instrImpl(
    const char * subject,
    size_t subject_size,
    Int64 byte_pos,
    Int64 occur,
    Int64 ret_op)
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
    return ret_op == 0
        ? DB::UTF8::bytePos2Utf8Pos(reinterpret_cast<const UInt8 *>(subject), byte_offset + 1)
        : DB::UTF8::bytePos2Utf8Pos(reinterpret_cast<const UInt8 *>(subject), byte_offset + matched_str.size() + 1);
}

template <bool thread_safe>
std::optional<StringRef> OptimizedRegularExpressionImpl<thread_safe>::substrImpl(
    const char * subject,
    size_t subject_size,
    Int64 byte_pos,
    Int64 occur)
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
void OptimizedRegularExpressionImpl<thread_safe>::replaceAllImpl(
    const char * subject,
    size_t subject_size,
    DB::ColumnString::Chars_t & res_data,
    DB::ColumnString::Offset & res_offset,
    Int64 byte_pos,
    const Instructions & instructions)
{
    size_t byte_offset = byte_pos - 1; // This is a offset for bytes, not utf8
    StringPieceType expr_sp(subject + byte_offset, subject_size - byte_offset);
    size_t start_pos = 0;
    size_t copy_pos = 0;
    size_t expr_len = expr_sp.size();
    StringPieceType matches[MAX_CAPTURES + 1];

    // Copy characters that before position
    res_data.resize(res_data.size() + byte_offset);
    memcpy(&res_data[res_offset], subject, byte_offset);
    res_offset += byte_offset;

    while (true)
    {
        bool success
            = re2->Match(expr_sp, start_pos, expr_len, re2_st::RE2::Anchor::UNANCHORED, matches, capture_num + 1);
        if (!success)
            break;

        auto skipped_byte_size = static_cast<Int64>(matches[0].data() - (expr_sp.data() + copy_pos));
        res_data.resize(res_data.size() + skipped_byte_size);
        memcpy(&res_data[res_offset], expr_sp.data() + copy_pos, skipped_byte_size); // copy the skipped bytes
        res_offset += skipped_byte_size;
        copy_pos += skipped_byte_size + matches[0].length();
        start_pos = copy_pos;

        replaceMatchedStringWithInstructions(res_data, res_offset, matches, instructions);

        if (matches[0].empty())
            start_pos += DB::UTF8::seqLength(expr_sp[start_pos]); // Avoid infinity loop
    }

    size_t suffix_byte_size = expr_len - copy_pos;
    res_data.resize(res_data.size() + suffix_byte_size + 1);
    memcpy(&res_data[res_offset], expr_sp.data() + copy_pos, suffix_byte_size); // Copy suffix string
    res_offset += suffix_byte_size;
    res_data[res_offset++] = 0;
}

template <bool thread_safe>
void OptimizedRegularExpressionImpl<thread_safe>::replaceOneImpl(
    const char * subject,
    size_t subject_size,
    DB::ColumnString::Chars_t & res_data,
    DB::ColumnString::Offset & res_offset,
    Int64 byte_pos,
    Int64 occur,
    const Instructions & instructions)
{
    size_t byte_offset = byte_pos - 1; // This is a offset for bytes, not utf8
    StringPieceType expr_sp(subject + byte_offset, subject_size - byte_offset);
    size_t start_pos = 0;
    size_t expr_len = expr_sp.size();
    StringPieceType matches[MAX_CAPTURES + 1];

    while (occur > 0)
    {
        bool success
            = re2->Match(expr_sp, start_pos, expr_len, re2_st::RE2::Anchor::UNANCHORED, matches, capture_num + 1);
        if (!success)
        {
            res_data.resize(res_data.size() + subject_size + 1);
            memcpy(&res_data[res_offset], subject, subject_size);
            res_offset += subject_size;
            res_data[res_offset++] = 0;
            return;
        }

        start_pos = matches[0].data() + matches[0].size() - expr_sp.data();
        --occur;
        if (matches[0].empty())
            start_pos += DB::UTF8::seqLength(expr_sp[start_pos]); // Avoid infinity loop
    }

    auto prefix_byte_size = static_cast<Int64>(matches[0].data() - subject);
    res_data.resize(res_data.size() + prefix_byte_size);
    memcpy(&res_data[res_offset], subject, prefix_byte_size); // Copy prefix string
    res_offset += prefix_byte_size;

    replaceMatchedStringWithInstructions(res_data, res_offset, matches, instructions);

    const char * suffix_str = subject + prefix_byte_size + matches[0].size();
    size_t suffix_byte_size = subject_size - prefix_byte_size - matches[0].size();
    res_data.resize(res_data.size() + suffix_byte_size + 1);
    memcpy(&res_data[res_offset], suffix_str, suffix_byte_size); // Copy suffix string
    res_offset += suffix_byte_size;

    res_data[res_offset++] = 0;
}

template <bool thread_safe>
void OptimizedRegularExpressionImpl<thread_safe>::replaceImpl(
    const char * subject,
    size_t subject_size,
    DB::ColumnString::Chars_t & res_data,
    DB::ColumnString::Offset & res_offset,
    Int64 byte_pos,
    Int64 occur,
    const Instructions & instructions)
{
    if (occur == 0)
        return replaceAllImpl(subject, subject_size, res_data, res_offset, byte_pos, instructions);
    else
        return replaceOneImpl(subject, subject_size, res_data, res_offset, byte_pos, occur, instructions);
}

template <bool thread_safe>
Int64 OptimizedRegularExpressionImpl<thread_safe>::instr(
    const char * subject,
    size_t subject_size,
    Int64 pos,
    Int64 occur,
    Int64 ret_op)
{
    Int64 utf8_total_len = DB::UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(subject), subject_size);

    FunctionsRegexp::checkArgsInstr(utf8_total_len, subject_size, pos, ret_op);
    FunctionsRegexp::makeOccurValid(occur);

    if (unlikely(subject_size == 0))
        return processInstrEmptyStringExpr(subject, subject_size, pos, occur);

    size_t byte_pos = DB::UTF8::utf8Pos2bytePos(reinterpret_cast<const UInt8 *>(subject), pos);
    return instrImpl(subject, subject_size, byte_pos, occur, ret_op);
}

template <bool thread_safe>
std::optional<StringRef> OptimizedRegularExpressionImpl<thread_safe>::substr(
    const char * subject,
    size_t subject_size,
    Int64 pos,
    Int64 occur)
{
    Int64 utf8_total_len = DB::UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(subject), subject_size);
    FunctionsRegexp::checkArgsSubstr(utf8_total_len, subject_size, pos);
    FunctionsRegexp::makeOccurValid(occur);

    if (unlikely(subject_size == 0))
        return processSubstrEmptyStringExpr(subject, subject_size, pos, occur);

    size_t byte_pos = DB::UTF8::utf8Pos2bytePos(reinterpret_cast<const UInt8 *>(subject), pos);
    return substrImpl(subject, subject_size, byte_pos, occur);
}

template <bool thread_safe>
void OptimizedRegularExpressionImpl<thread_safe>::replace(
    const char * subject,
    size_t subject_size,
    DB::ColumnString::Chars_t & res_data,
    DB::ColumnString::Offset & res_offset,
    const Instructions & instructions,
    Int64 pos,
    Int64 occur)
{
    Int64 utf8_total_len = DB::UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(subject), subject_size);

    FunctionsRegexp::checkArgsReplace(utf8_total_len, subject_size, pos);
    FunctionsRegexp::makeReplaceOccurValid(occur);

    size_t byte_pos = DB::UTF8::utf8Pos2bytePos(reinterpret_cast<const UInt8 *>(subject), pos);
    replaceImpl(subject, subject_size, res_data, res_offset, byte_pos, occur, instructions);
}

template <bool thread_safe>
Instructions OptimizedRegularExpressionImpl<thread_safe>::getInstructions(const StringRef & repl)
{
    Instructions instructions;
    String literals;

    for (size_t i = 0; i < repl.size; ++i)
    {
        if (repl.data[i] == '\\')
        {
            if (i + 1 < repl.size)
            {
                if (isNumericASCII(repl.data[i + 1])) /// Substitution
                {
                    if (!literals.empty())
                    {
                        instructions.emplace_back(literals);
                        literals = "";
                    }
                    instructions.emplace_back(repl.data[i + 1] - '0');
                }
                else
                    literals += repl.data[i + 1]; /// Escaping
                ++i;
            }
            else
            {
                // This slash is in the end. Ignore it and break the loop.
                break;
            }
        }
        else
            literals += repl.data[i]; /// Plain character
    }

    if (!literals.empty())
        instructions.emplace_back(literals);

    for (const auto & instr : instructions)
        if (instr.substitution_num > static_cast<Int32>(capture_num))
            throw Poco::Exception(
                fmt::format(
                    "Id {} in replacement string is an invalid substitution, regexp has only {} capturing groups",
                    instr.substitution_num,
                    capture_num),
                DB::ErrorCodes::BAD_ARGUMENTS);

    return instructions;
}

template <bool thread_safe>
void OptimizedRegularExpressionImpl<thread_safe>::replaceMatchedStringWithInstructions(
    DB::ColumnString::Chars_t & res_data,
    DB::ColumnString::Offset & res_offset,
    StringPieceType * matches,
    const Instructions & instructions)
{
    // Replace the matched string with instructions
    for (const auto & instr : instructions)
    {
        std::string_view replacement;
        if (instr.substitution_num >= 0)
            replacement
                = std::string_view(matches[instr.substitution_num].data(), matches[instr.substitution_num].size());
        else
            replacement = instr.literal;
        res_data.resize(res_data.size() + replacement.size());
        memcpy(&res_data[res_offset], replacement.data(), replacement.size());
        res_offset += replacement.size();
    }
}

#undef MIN_LENGTH_FOR_STRSTR
#undef MAX_SUBPATTERNS
