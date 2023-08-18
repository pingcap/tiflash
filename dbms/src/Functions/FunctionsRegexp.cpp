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

#include <Columns/ColumnNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRegexp.h>
#include <Functions/Regexps.h>
#include <fmt/core.h>

namespace DB
{
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
    /// - string that need to be inserted (in that case, first element of pair is -1 and second element is that string)
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
                /// if occ == 0, it will replace all the match expr, otherwise it only replace the occ-th match
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
            String mode_modifiers = re2Util::getRE2ModeModifiers(match_type, collator);
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
            String mode_modifiers = re2Util::getRE2ModeModifiers(match_type, collator);
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

using FunctionTiDBRegexp = FunctionStringRegexp<NameTiDBRegexp>;
using FunctionRegexpLike = FunctionStringRegexp<NameRegexpLike>;
using FunctionRegexpInstr = FunctionStringRegexpInstr<NameRegexpInstr>;
using FunctionRegexpSubstr = FunctionStringRegexpSubstr<NameRegexpSubstr>;
using FunctionReplaceRegexpOne = FunctionStringReplace<ReplaceRegexpImpl<true>, NameReplaceRegexpOne>;
using FunctionReplaceRegexpAll = FunctionStringReplace<ReplaceRegexpImpl<false>, NameReplaceRegexpAll>;

void registerFunctionsRegexp(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplaceRegexpOne>();
    factory.registerFunction<FunctionReplaceRegexpAll>();
    factory.registerFunction<FunctionTiDBRegexp>();
    factory.registerFunction<FunctionRegexpLike>();
    factory.registerFunction<FunctionRegexpInstr>();
    factory.registerFunction<FunctionRegexpSubstr>();
}

} // namespace DB
