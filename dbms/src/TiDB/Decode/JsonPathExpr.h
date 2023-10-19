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

#include <Core/Types.h>
#include <common/StringRef.h>
#include <common/memcpy.h>

/**
 * From MySQL 5.7, JSON path expression grammar:
		pathExpression ::= scope (jsonPathLeg)*
		scope ::= [ columnReference ] '$'
		columnReference ::= // omit...
		jsonPathLeg ::= member | arrayLocation | '**'
		member ::= '.' (keyName | '*')
		arrayLocation ::= '[' (non-negative-integer | '*') ']'
		keyName ::= ECMAScript-identifier | ECMAScript-string-literal

    And some implementation limits in MySQL 5.7:
		1) columnReference in scope must be empty now;
        2) double asterisk(**) could not be last leg;

        Examples:
            select json_extract('{"a": "b", "c": [1, "2"]}', '$.a') -> "b"
            select json_extract('{"a": "b", "c": [1, "2"]}', '$.c') -> [1, "2"]
            select json_extract('{"a": "b", "c": [1, "2"]}', '$.a', '$.c') -> ["b", [1, "2"]]
            select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[0]') -> 1
            select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[2]') -> NULL
            select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[*]') -> [1, "2"]
            select json_extract('{"a": "b", "c": [1, "2"]}', '$.*') -> ["b", [1, "2"]]
 */
namespace DB
{
class JsonBinary;

enum JsonPathArraySelectionType
{
    JsonPathArraySelectionAsterisk,
    JsonPathArraySelectionIndex,
    JsonPathArraySelectionRange,
    JsonPathArraySelectionInvalid
};

enum JsonPathObjectKeyStatus
{
    JsonPathObjectKeyCacheDisabled, /// *, ** will disable caching
    JsonPathObjectKeyUncached,
    JsonPathObjectKeyCached,
};

// if index is positive, it represents the [index]
// if index is negative, it represents the [len() + index]
// a normal index "5" will be parsed into "5", and the "last - 5" will be "-6"
using JsonPathArrayIndex = Int32;

struct JsonPathArraySelection
{
    explicit JsonPathArraySelection(JsonPathArraySelectionType type_)
        : JsonPathArraySelection(type_, 0)
    {}
    JsonPathArraySelection(JsonPathArraySelectionType type_, JsonPathArrayIndex index_)
        : type(type_)
        , index(index_)
    {}
    JsonPathArraySelection(JsonPathArraySelectionType type_, JsonPathArrayIndex start_, JsonPathArrayIndex end_)
        : type(type_)
        , index_range{start_, end_}
    {}
    std::pair<JsonPathArrayIndex, JsonPathArrayIndex> getIndexRange(const JsonBinary & json) const;
    JsonPathArraySelectionType type = JsonPathArraySelectionInvalid;
    union
    {
        JsonPathArrayIndex index;
        JsonPathArrayIndex index_range[2];
    };
};

struct JsonPathObjectKey
{
    explicit JsonPathObjectKey(const String & key_)
        : key(key_)
        , status(JsonPathObjectKeyUncached)
        , cached_index(-1)
    {}
    String key;
    JsonPathObjectKeyStatus status;
    Int32 cached_index;
};

struct JsonPathStream
{
    explicit JsonPathStream(const StringRef & str_ref)
        : str(str_ref)
    {}

    void skipWhiteSpace();
    char read(); /// Read and advance
    char peek() const; /// Read only, No advance
    void skip(UInt32 i);
    bool exhausted() const;
    std::pair<bool, JsonPathArrayIndex> tryReadIndexNumber();
    // tryParseArrayIndex tries to read an arrayIndex, which is 'number', 'last' or 'last - number'
    // if failed, the stream will not be pushed forward
    std::pair<bool, JsonPathArrayIndex> tryParseArrayIndex();
    bool tryReadString(const String & expected);
    template <typename FF>
    std::pair<bool, String> readWhile(
        FF && f); /// Since path are usually very short strings, return String instead of StringRef
    static const std::pair<bool, JsonPathArrayIndex> InvalidIndexPair;
    StringRef str;
    size_t pos = 0;
};

template <typename FF>
std::pair<bool, String> JsonPathStream::readWhile(FF && f)
{
    size_t start = pos;
    for (; !exhausted(); skip(1))
    {
        if (!f(peek()))
        {
            return std::make_pair(true, String(str.data + start, pos - start));
        }
    }
    return std::make_pair(false, String(str.data + start, pos - start));
}

using JsonPathLegType = UInt8;
struct JsonPathLeg
{
    /// JsonPathLegKey indicates the path leg with '.key'.
    static constexpr JsonPathLegType JsonPathLegKey = 0x01;
    /// JsonPathLegArraySelection indicates the path leg with form '[index]', '[index to index]'.
    static constexpr JsonPathLegType JsonPathLegArraySelection = 0x02;
    /// JsonPathLegDoubleAsterisk indicates the path leg with form '**'.
    static constexpr JsonPathLegType JsonPathLegDoubleAsterisk = 0x03;

    JsonPathLeg(JsonPathLegType type_, JsonPathArraySelection selection_)
        : type(type_)
        , array_selection(selection_)
        , dot_key("")
    {}

    JsonPathLeg(JsonPathLegType type_, const String & dot_key_)
        : type(type_)
        , array_selection(JsonPathArraySelectionInvalid)
        , dot_key(dot_key_)
    {}

    JsonPathLegType type;
    JsonPathArraySelection
        array_selection; // if typ is JsonPathLegArraySelection, the value should be parsed into here.
    JsonPathObjectKey dot_key; // if typ is JsonPathLegKey, the key should be parsed into here.
};
using JsonPathLegPtr = std::unique_ptr<JsonPathLeg>;
using JsonPathLegRawPtr = JsonPathLeg *;
using JsonPathExpressionFlag = UInt8;
/// JsonPathExpr instance should be un-mutable after it is created
class JsonPathExpr
{
public:
    static constexpr JsonPathExpressionFlag JsonPathExpressionContainsAsterisk = 0x01;
    static constexpr JsonPathExpressionFlag JsonPathExpressionContainsDoubleAsterisk = 0x02;
    static constexpr JsonPathExpressionFlag JsonPathExpressionContainsRange = 0x04;

    static bool containsAnyAsterisk(JsonPathExpressionFlag flag)
    {
        flag &= JsonPathExpressionContainsAsterisk | JsonPathExpressionContainsDoubleAsterisk;
        return flag != 0;
    }

    static bool containsAnyRange(JsonPathExpressionFlag flag)
    {
        flag &= JsonPathExpressionContainsRange;
        return flag != 0;
    }

    static std::shared_ptr<JsonPathExpr> parseJsonPathExpr(const StringRef & str_ref);

    JsonPathExpressionFlag getFlag() const { return flag; }

    const std::vector<JsonPathLegPtr> & getLegs() const { return legs; }

private:
    JsonPathExpr() = default;

    static bool parseJsonPathArray(JsonPathStream & stream, JsonPathExpr * path_expr);
    static bool parseJsonPathWildcard(JsonPathStream & stream, JsonPathExpr * path_expr);
    static bool parseJsonPathMember(JsonPathStream & stream, JsonPathExpr * path_expr);

    std::vector<JsonPathLegPtr> legs;
    JsonPathExpressionFlag flag = 0U;
};
using JsonPathExprPtr = std::shared_ptr<JsonPathExpr>;

} // namespace DB
