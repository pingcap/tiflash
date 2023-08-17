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

#include <Columns/ColumnString.h>
#include <Common/config.h>
#include <common/StringRef.h>
#include <common/types.h>
#include <re2/re2.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>
#if USE_RE2_ST
#include <re2_st/re2.h>
#else
#define re2_st re2
#endif


/** Uses two ways to optimize a regular expression:
  * 1. If the regular expression is trivial (reduces to finding a substring in a string),
  *     then replaces the search with strstr or strcasestr.
  * 2. If the regular expression contains a non-alternative substring of sufficient length,
  *     then before testing, strstr or strcasestr of sufficient length is used;
  *     regular expression is only fully checked if a substring is found.
  * 3. In other cases, the re2 engine is used.
  *
  * This makes sense, since strstr and strcasestr in libc for Linux are well optimized.
  *
  * Suitable if the following conditions are simultaneously met:
  * - if in most calls, the regular expression does not match;
  * - if the regular expression is compatible with the re2 engine;
  * - you can use at your own risk, since, probably, not all cases are taken into account.
  *
  * NOTE: Multi-character metasymbols such as \Pl are handled incorrectly.
  */

namespace OptimizedRegularExpressionDetails
{
struct Match
{
    std::string::size_type offset;
    std::string::size_type length;
};
} // namespace OptimizedRegularExpressionDetails

struct Instruction
{
    explicit Instruction(int substitution_num_)
        : substitution_num(substitution_num_)
    {}
    explicit Instruction(String literal_)
        : literal(std::move(literal_))
    {}

    // If not negative, perform substitution of n-th subpattern from the regexp match.
    Int32 substitution_num = -1;

    // Otherwise, paste this literal string verbatim.
    String literal;
};

/// Decomposes the replacement string into a sequence of substitutions and literals.
/// E.g. "abc$1de$2fg$1$2" --> inst("abc"), inst(1), inst("de"), inst(2), inst("fg"), inst(1), inst(2)
using Instructions = std::vector<Instruction>;

template <bool thread_safe>
class OptimizedRegularExpressionImpl
{
public:
    enum Options
    {
        RE_CASELESS = 0x00000001,
        RE_NO_CAPTURE = 0x00000010,
        RE_DOT_NL = 0x00000100,
        RE_NO_OPTIMIZE = 0x00001000
    };

    using Match = OptimizedRegularExpressionDetails::Match;
    using MatchVec = std::vector<Match>;

    using RegexType = std::conditional_t<thread_safe, re2::RE2, re2_st::RE2>;
    using StringPieceType = std::conditional_t<thread_safe, re2::StringPiece, re2_st::StringPiece>;

    explicit OptimizedRegularExpressionImpl(const std::string & regexp_, int options = 0);

    bool match(const std::string & subject) const { return match(subject.data(), subject.size()); }

    bool match(const std::string & subject, Match & match_) const
    {
        return match(subject.data(), subject.size(), match_);
    }

    unsigned match(const std::string & subject, MatchVec & matches) const
    {
        return match(subject.data(), subject.size(), matches);
    }

    unsigned match(const char * subject, size_t subject_size, MatchVec & matches) const
    {
        return match(subject, subject_size, matches, capture_num + 1);
    }

    bool match(const char * subject, size_t subject_size) const;
    bool match(const char * subject, size_t subject_size, Match & match) const;
    unsigned match(const char * subject, size_t subject_size, MatchVec & matches, unsigned limit) const;

    unsigned getNumberOfCaptureGroup() const { return capture_num; }
    Instructions getInstructions(const StringRef & repl);

    /// Get the regexp re2 or nullptr if the pattern is trivial (for output to the log).
    const std::unique_ptr<RegexType> & getRE2() const { return re2; }

    static void analyze(
        const std::string & regexp_,
        std::string & required_substring,
        bool & is_trivial,
        bool & required_substring_is_prefix);

    void getAnalyzeResult(
        std::string & out_required_substring,
        bool & out_is_trivial,
        bool & out_required_substring_is_prefix) const
    {
        out_required_substring = required_substring;
        out_is_trivial = is_trivial;
        out_required_substring_is_prefix = required_substring_is_prefix;
    }

    Int64 instr(const char * subject, size_t subject_size, Int64 pos, Int64 occur, Int64 ret_op);
    std::optional<StringRef> substr(const char * subject, size_t subject_size, Int64 pos, Int64 occur);
    void replace(
        const char * subject,
        size_t subject_size,
        DB::ColumnString::Chars_t & res_data,
        DB::ColumnString::Offset & res_offset,
        const Instructions & instructions,
        Int64 pos,
        Int64 occur);

private:
    Int64 processInstrEmptyStringExpr(const char * expr, size_t expr_size, size_t byte_pos, Int64 occur);
    Int64 instrImpl(const char * subject, size_t subject_size, Int64 byte_pos, Int64 occur, Int64 ret_op);

    std::optional<StringRef> processSubstrEmptyStringExpr(
        const char * expr,
        size_t expr_size,
        size_t byte_pos,
        Int64 occur);
    std::optional<StringRef> substrImpl(const char * subject, size_t subject_size, Int64 byte_pos, Int64 occur);

    void replaceImpl(
        const char * subject,
        size_t subject_size,
        DB::ColumnString::Chars_t & res_data,
        DB::ColumnString::Offset & res_offset,
        Int64 byte_pos,
        Int64 occur,
        const Instructions & instructions);
    void replaceOneImpl(
        const char * subject,
        size_t subject_size,
        DB::ColumnString::Chars_t & res_data,
        DB::ColumnString::Offset & res_offset,
        Int64 byte_pos,
        Int64 occur,
        const Instructions & instructions);
    void replaceAllImpl(
        const char * subject,
        size_t subject_size,
        DB::ColumnString::Chars_t & res_data,
        DB::ColumnString::Offset & res_offset,
        Int64 byte_pos,
        const Instructions & instructions);
    void replaceMatchedStringWithInstructions(
        DB::ColumnString::Chars_t & res_data,
        DB::ColumnString::Offset & res_offset,
        StringPieceType * matches,
        const Instructions & instructions);

    bool is_trivial;
    bool required_substring_is_prefix;
    bool is_case_insensitive;
    std::string required_substring;
    std::unique_ptr<RegexType> re2;
    unsigned capture_num;
};

using OptimizedRegularExpression = OptimizedRegularExpressionImpl<true>;

#include "OptimizedRegularExpression.inl.h"
