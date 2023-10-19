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
#include <Common/StringUtils/StringUtils.h>
#include <TiDB/Decode/JsonBinary.h>
#include <TiDB/Decode/JsonPathExpr.h>

namespace DB
{
const std::pair<bool, JsonPathArrayIndex> JsonPathStream::InvalidIndexPair{false, 0};
const JsonPathArraySelection AsteriskSelection{JsonPathArraySelectionAsterisk};

bool isEcmascriptIdentifier(const String & str)
{
    if (str.empty())
        return false;

    size_t length = str.length();
    for (size_t i = 0; i < length; ++i)
    {
        char c = str[i];
        if ((i != 0 && isNumericASCII(c)) || isAlphaASCII(c) || c == '_' || c == '$' || static_cast<UInt8>(c) >= 0x80U)
            continue;
        return false;
    }
    return true;
}

// validateIndexRange returns whether `a` could be less or equal than `b`
// if two indexes are all non-negative, or all negative, the comparison follows the number order
// if the sign of them differs, this function will still return true
bool validateIndexRange(JsonPathArrayIndex a, JsonPathArrayIndex b)
{
    if ((a >= 0 && b >= 0) || (a < 0 && b < 0))
        return a <= b;
    return true;
}

JsonPathArrayIndex jsonPathArrayIndexFromStart(Int32 index)
{
    return index;
}

JsonPathArrayIndex jsonPathArrayIndexFromLast(Int32 index)
{
    return -1 - index;
}

JsonPathArrayIndex getArrayIndexFromStart(const JsonBinary & json, JsonPathArrayIndex index)
{
    if (index < 0)
        return static_cast<Int32>(json.getElementCount()) + index;
    return index;
}

std::pair<JsonPathArrayIndex, JsonPathArrayIndex> JsonPathArraySelection::getIndexRange(const JsonBinary & json) const
{
    auto element_count = static_cast<Int32>(json.getElementCount());
    switch (type)
    {
    case JsonPathArraySelectionAsterisk:
        return std::make_pair(0, element_count - 1);
    case JsonPathArraySelectionIndex:
    {
        auto end = getArrayIndexFromStart(json, index);
        auto adjusted_end = end;
        // returns index, min(index, count - 1)
        // so that the caller could only check the start <= end
        if (adjusted_end >= element_count)
        {
            adjusted_end = element_count - 1;
        }
        return std::make_pair(end, adjusted_end);
    }
    case JsonPathArraySelectionRange:
    {
        auto start = getArrayIndexFromStart(json, index_range[0]);
        auto end = getArrayIndexFromStart(json, index_range[1]);
        // returns index, min(index, count - 1)
        // so that the caller could only check the start <= end
        if (end >= element_count)
        {
            end = element_count - 1;
        }
        return std::make_pair(start, end);
    }
    case JsonPathArraySelectionInvalid:
        RUNTIME_CHECK(false);
    }
}

void JsonPathStream::skipWhiteSpace()
{
    for (; pos < str.size; ++pos)
    {
        if (!isWhitespaceASCII(
                str.data[pos])) /// Implementation is different from TiDB, which uses go lib's unicode.IsSpace
            break;
    }
}

char JsonPathStream::read()
{
    return str.data[pos++];
}

char JsonPathStream::peek() const
{
    return str.data[pos];
}

void JsonPathStream::skip(UInt32 i)
{
    pos += i;
}

bool JsonPathStream::exhausted() const
{
    return pos >= str.size;
}

std::pair<bool, JsonPathArrayIndex> JsonPathStream::tryReadIndexNumber()
{
    auto record_pos = pos;
    auto res = readWhile([](char c) { return isNumericASCII(c); });
    if (!res.first)
    {
        pos = record_pos;
        return InvalidIndexPair;
    }

    Int32 index = std::stoll(res.second);
    if (index > std::numeric_limits<Int32>::max())
    {
        pos = record_pos;
        return InvalidIndexPair;
    }

    return std::make_pair(true, index);
}

bool JsonPathStream::tryReadString(const String & expected)
{
    auto record_pos = pos;
    size_t i = 0;
    size_t expected_length = expected.length();

    auto res = readWhile([&i, expected_length](char) {
        ++i;
        return i <= expected_length;
    });

    if (!res.first || res.second != expected)
    {
        pos = record_pos;
        return false;
    }
    return true;
}

std::pair<bool, JsonPathArrayIndex> JsonPathStream::tryParseArrayIndex()
{
    auto record_pos = pos;
    skipWhiteSpace();
    if (exhausted())
        return InvalidIndexPair;

    char c = peek();
    if (isNumericASCII(c))
    {
        auto res = tryReadIndexNumber();
        if (!res.first)
        {
            pos = record_pos;
            return InvalidIndexPair;
        }
        return std::make_pair(true, jsonPathArrayIndexFromStart(res.second));
    }
    else if (c == 'l')
    {
        if (!tryReadString("last"))
        {
            pos = record_pos;
            return InvalidIndexPair;
        }
        skipWhiteSpace();
        if (exhausted())
            return std::make_pair(true, jsonPathArrayIndexFromLast(0));

        if (peek() != '-')
            return std::make_pair(true, jsonPathArrayIndexFromLast(0));

        skip(1);
        skipWhiteSpace();

        auto res = tryReadIndexNumber();
        if (!res.first)
        {
            pos = record_pos;
            return InvalidIndexPair;
        }
        return std::make_pair(true, jsonPathArrayIndexFromLast(res.second));
    }
    return InvalidIndexPair;
}

std::shared_ptr<JsonPathExpr> JsonPathExpr::parseJsonPathExpr(const StringRef & str_ref)
{
    JsonPathStream stream(str_ref);
    stream.skipWhiteSpace();
    if (stream.exhausted() || stream.read() != '$')
        return nullptr;

    stream.skipWhiteSpace();

    /// Not using std::make_shared for private constructor
    auto * json_path_ptr = new JsonPathExpr();
    std::shared_ptr<JsonPathExpr> path_expr(json_path_ptr);
    path_expr->legs.reserve(16);
    bool ok = false;
    while (!stream.exhausted())
    {
        switch (stream.peek())
        {
        case '.':
            ok = parseJsonPathMember(stream, path_expr.get());
            break;
        case '[':
            ok = parseJsonPathArray(stream, path_expr.get());
            break;
        case '*':
            ok = parseJsonPathWildcard(stream, path_expr.get());
            break;
        default:
            ok = false;
        }

        if (!ok)
            return nullptr;
        stream.skipWhiteSpace();
    }

    size_t leg_length = path_expr->legs.size();
    if (!path_expr->legs.empty() && path_expr->legs[leg_length - 1]->type == JsonPathLeg::JsonPathLegDoubleAsterisk)
        return nullptr;

    /// If multiple match could happen, disable LegKey cache index
    if (JsonPathExpr::containsAnyAsterisk(path_expr->flag) || JsonPathExpr::containsAnyRange(path_expr->flag))
    {
        for (auto & leg_ptr : path_expr->legs)
        {
            if (leg_ptr->type == JsonPathLeg::JsonPathLegKey)
                leg_ptr->dot_key.status = JsonPathObjectKeyCacheDisabled;
        }
    }
    return path_expr;
}

bool JsonPathExpr::parseJsonPathArray(JsonPathStream & stream, JsonPathExpr * path_expr)
{
    stream.skip(1);
    stream.skipWhiteSpace();
    if (stream.exhausted())
        return false;

    if (stream.peek() == '*')
    {
        stream.skip(1);
        path_expr->flag |= JsonPathExpressionContainsAsterisk;
        path_expr->legs.push_back(
            std::make_unique<JsonPathLeg>(JsonPathLeg::JsonPathLegArraySelection, AsteriskSelection));
    }
    else
    {
        auto res = stream.tryParseArrayIndex();
        if (!res.first)
            return false;
        auto start = res.second;
        JsonPathArraySelection selection(JsonPathArraySelectionIndex, start);
        // try to read " to " and the end
        if (std::isspace(stream.peek()))
        {
            stream.skipWhiteSpace();
            if (stream.tryReadString("to") && std::isspace(stream.peek()))
            {
                stream.skipWhiteSpace();
                if (stream.exhausted())
                    return false;
                auto parsed_pair = stream.tryParseArrayIndex();
                if (!parsed_pair.first)
                    return false;

                auto end = parsed_pair.second;
                if (!validateIndexRange(start, end))
                    return false;
                path_expr->flag |= JsonPathExpressionContainsRange;
                selection = JsonPathArraySelection(JsonPathArraySelectionRange, start, end);
            }
        }
        path_expr->legs.push_back(std::make_unique<JsonPathLeg>(JsonPathLeg::JsonPathLegArraySelection, selection));
    }
    stream.skipWhiteSpace();
    return !(stream.exhausted() || stream.read() != ']');
}

bool JsonPathExpr::parseJsonPathWildcard(JsonPathStream & stream, JsonPathExpr * path_expr)
{
    stream.skip(1);
    if (stream.exhausted() || stream.read() != '*')
        return false;

    if (stream.exhausted() || stream.peek() == '*')
        return false;

    path_expr->flag |= JsonPathExpressionContainsDoubleAsterisk;
    path_expr->legs.push_back(std::make_unique<JsonPathLeg>(JsonPathLeg::JsonPathLegDoubleAsterisk, AsteriskSelection));
    return true;
}

bool JsonPathExpr::parseJsonPathMember(JsonPathStream & stream, JsonPathExpr * path_expr)
{
    stream.skip(1);
    stream.skipWhiteSpace();
    if (stream.exhausted())
        return false;

    if (stream.peek() == '*')
    {
        stream.skip(1);
        path_expr->flag |= JsonPathExpressionContainsAsterisk;
        path_expr->legs.push_back(std::make_unique<JsonPathLeg>(JsonPathLeg::JsonPathLegKey, "*"));
    }
    else
    {
        String dot_key;
        bool quoted = false;
        if (stream.peek() == '"')
        {
            stream.skip(1);
            auto res = stream.readWhile([&stream](char c) {
                if (c == '\\')
                {
                    stream.skip(1);
                    return true;
                }
                return c != '"';
            });

            if (!res.first)
                return false;
            stream.skip(1);
            dot_key = res.second;
            quoted = true;
        }
        else
        {
            auto res
                = stream.readWhile([](char c) { return !(isWhitespaceASCII(c) || c == '.' || c == '[' || c == '*'); });
            dot_key = res.second;
        }

        dot_key = JsonBinary::unquoteJsonString(dot_key);
        if (!quoted && !isEcmascriptIdentifier(dot_key))
            return false;

        path_expr->legs.push_back(std::make_unique<JsonPathLeg>(JsonPathLeg::JsonPathLegKey, dot_key));
    }
    return true;
}

} // namespace DB
