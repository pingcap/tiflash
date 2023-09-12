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

#include <Common/UTF8Helpers.h>
#include <TiDB/Collation/CollatorUtils.h>
#include <common/mem_utils_opt.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <vector>

namespace TiDB
{

static constexpr char ANY = '%';
static constexpr char ONE = '_';

/*
    Unicode Code    UTF-8 Code
    0000～007F      0xxxxxxx
    0080～07FF      110xxxxx 10xxxxxx
    0800～FFFF      1110xxxx 10xxxxxx 10xxxxxx
    10000～10FFFF   11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
*/
template <bool utf8>
inline size_t BinCharSizeFromHead(const uint8_t b0)
{
    if constexpr (!utf8)
    {
        return 1;
    }
    return DB::UTF8::seqLength(b0);
}

template <bool utf8>
inline size_t BinCharSizeFromEnd(const char * b_, const char * begin_)
{
    if constexpr (!utf8)
    {
        return 1;
    }

    const auto * b = reinterpret_cast<const uint8_t *>(b_);
    if (*b < 0x80)
    {
        return 1;
    }
    const auto * ori = b;

    const auto * begin = reinterpret_cast<const uint8_t *>(begin_);

    // check range in case that bin str is invalid
    while (begin < b && *b < 0xC0)
    {
        --b;
    }
    return ori - b + 1;
}

template <bool utf8>
struct BinStrPattern
{
    void compile(std::string_view pattern, char escape_)
    {
        {
            match_sub_str.clear();
            match_sub_str.reserve(8);
            match_types.clear();
            match_types.reserve(8);
        }
        escape = escape_;

        auto last_match_start = std::string_view::npos;

        auto && fn_try_add_last_match_str = [&](size_t end_offset) {
            if (last_match_start != std::string_view::npos)
            {
                match_sub_str.emplace_back(&pattern[last_match_start], end_offset - last_match_start);
                match_types.emplace_back(MatchType::Match);
                // reset
                last_match_start = std::string_view::npos;
            }
        };

        for (size_t offset = 0; offset < pattern.size();)
        {
            auto c = pattern[offset];
            auto cur_offset = offset;
            auto size = BinCharSizeFromHead<utf8>(pattern[offset]);
            offset += size; // move next

            if (size == 1)
            {
                if (c == escape)
                {
                    fn_try_add_last_match_str(cur_offset);

                    if (offset < pattern.size())
                    {
                        // start from current offset
                        last_match_start = offset;

                        // use next to match
                        auto new_size = BinCharSizeFromHead<utf8>(pattern[offset]);
                        offset += new_size; // move next
                    }
                    else
                    {
                        // use `escape` to match
                        match_sub_str.emplace_back(&escape, sizeof(escape));
                        match_types.emplace_back(MatchType::Match);
                    }
                }
                else if (c == ANY)
                {
                    fn_try_add_last_match_str(cur_offset);
                    match_types.emplace_back(MatchType::Any);
                }
                else if (c == ONE)
                {
                    fn_try_add_last_match_str(cur_offset);
                    match_types.emplace_back(MatchType::One);
                }
                else
                {
                    // if last match start offset is none, start from current offset.
                    last_match_start = last_match_start == std::string_view::npos ? cur_offset : last_match_start;
                }
            }
            else
            {
                // if last match start offset is none, start from current offset.
                last_match_start = last_match_start == std::string_view::npos ? cur_offset : last_match_start;
            }
        }
        fn_try_add_last_match_str(pattern.size());
    }
    struct MatchDesc
    {
        ssize_t pattern_index_start{}, pattern_index_end{};
        ssize_t match_str_index_start{}, match_str_index_end{};
        ssize_t src_index_start{}, src_index_end{};

        bool isSrcValid() const { return !isSrcEmpty(); }
        bool isSrcEmpty() const { return src_index_start >= src_index_end; }
        size_t srcSize() const { return src_index_end - src_index_start; }
        std::string_view getSrcStrView(const char * src_data, size_t size) const
        {
            return std::string_view{src_data + src_index_start, size};
        }
        void srcMoveByOffset(size_t size) { src_index_start += size; }
        void srcSkipChar(const char * src_data)
        {
            auto size = BinCharSizeFromHead<utf8>(src_data[src_index_start]);
            srcMoveByOffset(size);
        }
        bool patternEmpty() const { return pattern_index_start >= pattern_index_end; }
        void makeSrcInvalid() { src_index_start = src_index_end; }
    };

    // check str equality
    // - make src invalid if remain size if smaller than required
    ALWAYS_INLINE inline bool matchStrEqual(const std::string_view & src, MatchDesc & desc) const
    {
        const auto & match_str = match_sub_str[desc.match_str_index_start];
        if (desc.srcSize() < match_str.size())
        {
            desc.makeSrcInvalid();
            return false;
        }
        if (!mem_utils::IsStrViewEqual(desc.getSrcStrView(src.data(), match_str.size()), match_str))
        {
            return false;
        }
        desc.match_str_index_start++;
        desc.srcMoveByOffset(match_str.size());
        return true;
    }

    // match from start exactly
    // - return true if meet %
    // - return false if failed to match else true
    ALWAYS_INLINE inline bool matchExactly(const std::string_view & src, MatchDesc & cur_match_desc) const
    {
        // match from start
        for (; !cur_match_desc.patternEmpty(); cur_match_desc.pattern_index_start++)
        {
            const auto & type = match_types[cur_match_desc.pattern_index_start];
            if (type == MatchType::Any)
            {
                // break from loop
                break;
            }

            if (type == MatchType::Match)
            {
                if (!matchStrEqual(src, cur_match_desc))
                    return false;
            }
            else
            {
                // src must be not empty
                if (!cur_match_desc.isSrcValid())
                    return false;
                cur_match_desc.srcSkipChar(src.data());
            }
        }
        return true;
    };

    // match from end exactly
    // - return true if meet %
    // - return false if failed to match else true
    ALWAYS_INLINE inline bool matchExactlyReverse(const std::string_view & src, MatchDesc & cur_match_desc) const
    {
        for (; !cur_match_desc.patternEmpty(); --cur_match_desc.pattern_index_end)
        {
            const auto & type = match_types[cur_match_desc.pattern_index_end - 1];
            if (type == MatchType::Any)
            {
                break;
            }

            if (type == MatchType::Match)
            {
                const auto & match_str = match_sub_str[cur_match_desc.match_str_index_end - 1];
                if (cur_match_desc.srcSize() < match_str.size())
                {
                    return false;
                }

                if (!mem_utils::IsStrViewEqual(
                        {src.data() + cur_match_desc.src_index_end - match_str.size(), match_str.size()},
                        match_str))
                {
                    return false;
                }
                cur_match_desc.match_str_index_end--;
                cur_match_desc.src_index_end -= match_str.size();
            }
            else
            {
                // src must be not empty
                if (!cur_match_desc.isSrcValid())
                    return false;

                auto size = BinCharSizeFromEnd<utf8>(
                    &src[cur_match_desc.src_index_end - 1],
                    &src[cur_match_desc.src_index_start]);
                cur_match_desc.src_index_end -= size; // remove from end
            }
        }
        return true;
    };

    // search by pattern `...%..%`
    // - return true if meet %
    // - return false if failed to search
    ALWAYS_INLINE inline bool searchByPattern(const std::string_view & src, MatchDesc & desc) const
    {
        assert(match_types[desc.pattern_index_end - 1] == MatchType::Any);
        assert(!desc.patternEmpty());

        // leading `MatchType::One` can be removed first
        for (; match_types[desc.pattern_index_start] == MatchType::One; desc.pattern_index_start++)
        {
            // src must be not empty
            if (!desc.isSrcValid())
                return false;
            desc.srcSkipChar(src.data());
        }

        if (match_types[desc.pattern_index_start] == MatchType::Any)
        {
            return true;
        }

        // current type is MatchType::Match
        // loop:
        // - search next position of match sub str
        // - if position found, start to match exactly
        //   - if match fail, fallback to loop
        //   - if match success, return match end pos
        // - if position not found, return with fail.
        for (;;)
        {
            const auto & match_str = match_sub_str[desc.match_str_index_start];
            auto src_view = desc.getSrcStrView(src.data(), desc.srcSize());
            auto pos = std::string_view::npos;

            // search sub str
            // - seachers like `ASCIICaseSensitiveStringSearcher` or `Volnitsky` are too heavy for small str
            {
                pos = mem_utils::StrFind(src_view, match_str);
            }

            if (pos == std::string_view::npos)
            {
                return false;
            }
            else
            {
                // move to sub str position
                desc.src_index_start = pos + src_view.data() - src.data();

                MatchDesc new_desc = desc;
                new_desc.srcMoveByOffset(match_str.size()); // start to check rest
                new_desc.match_str_index_start++;
                new_desc.pattern_index_start++;

                if (!matchExactly(src, new_desc))
                {
                    if (!new_desc.isSrcValid())
                        return false;
                    // skip one char and restart to search
                    desc.srcSkipChar(src.data());
                }
                else
                {
                    desc = new_desc;
                    return true;
                }
            }
        }
    };

    ALWAYS_INLINE inline bool match(std::string_view src) const
    {
        MatchDesc cur_match_desc;
        {
            cur_match_desc.pattern_index_end = match_types.size();
            cur_match_desc.match_str_index_end = match_sub_str.size();
            cur_match_desc.src_index_end = src.size();
        }

        // if pattern starts or ends with `MatchType::Match` or `MatchType::One`, match exactly
        {
            // match from start
            if (!matchExactly(src, cur_match_desc))
            {
                return false;
            }
            // match from end
            if (!matchExactlyReverse(src, cur_match_desc))
            {
                return false;
            }
        }

        // if remain pattern is empty, src must be empty
        if (cur_match_desc.patternEmpty())
        {
            return cur_match_desc.isSrcEmpty();
        }

        assert(match_types[cur_match_desc.pattern_index_end - 1] == MatchType::Any);

        // remain pattern should be %..%...%
        // search sub str one by one based on greedy rule
        for (;;)
        {
            assert(match_types[cur_match_desc.pattern_index_start] == MatchType::Any);

            // move to next match type
            cur_match_desc.pattern_index_start++;

            if (cur_match_desc.patternEmpty()) // if % is the last one
                break;

            if (!searchByPattern(src, cur_match_desc))
                return false;
        }
        return true;
    }

    enum class MatchType
    {
        Match,
        One,
        Any,
    };

    std::vector<MatchType> match_types;
    std::vector<std::string_view> match_sub_str;
    char escape{};
};
} // namespace TiDB

namespace DB
{
template <typename Result, bool revert, bool utf8>
inline void BinStringPatternMatch(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const std::string_view & pattern_str,
    uint8_t escape_char,
    Result & c)
{
    TiDB::BinStrPattern<utf8> matcher;
    matcher.compile(pattern_str, escape_char);
    LoopOneColumn(a_data, a_offsets, a_offsets.size(), [&](const std::string_view & view, size_t i) {
        c[i] = revert ^ matcher.match(view);
    });
}

template <bool revert, typename Result>
ALWAYS_INLINE inline bool StringPatternMatchImpl(
    const ColumnString::Chars_t & a_data,
    const ColumnString::Offsets & a_offsets,
    const std::string_view & pattern_str,
    uint8_t escape_char,
    const TiDB::TiDBCollatorPtr & collator,
    Result & c)
{
    bool use_optimized_path = false;

    switch (collator->getCollatorType())
    {
    case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
    case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
    {
        BinStringPatternMatch<Result, revert, true>(a_data, a_offsets, pattern_str, escape_char, c);
        use_optimized_path = true;
        break;
    }
    case TiDB::ITiDBCollator::CollatorType::BINARY:
    case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
    case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
    {
        BinStringPatternMatch<Result, revert, false>(a_data, a_offsets, pattern_str, escape_char, c);
        use_optimized_path = true;
        break;
    }

    default:
        break;
    }
    return use_optimized_path;
}
} // namespace DB