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

#include <Functions/FunctionsRegexpSubstr.h>
#include <Functions/tests/regexp_test_util.h>

namespace DB
{
namespace tests
{
class RegexpSubstr : public Regexp
{
};

struct RegexpSubstrCase
{
    RegexpSubstrCase(
        const String & res,
        const String & expr,
        const String & pat,
        Int64 pos = 1,
        Int64 occur = 1,
        const String & mt = "")
        : result(res)
        , expression(expr)
        , pattern(pat)
        , position(pos)
        , occurrence(occur)
        , match_type(mt)
    {}

    RegexpSubstrCase(
        const String & res,
        const std::vector<UInt8> & null_map_,
        const String & expr,
        const String & pat,
        Int64 pos = 1,
        Int64 occur = 1,
        const String & mt = "")
        : result(res)
        , null_map(null_map_)
        , expression(expr)
        , pattern(pat)
        , position(pos)
        , occurrence(occur)
        , match_type(mt)
    {}

    static void setVecsWithoutNullMap(
        int param_num,
        const std::vector<RegexpSubstrCase> test_cases,
        std::vector<String> & results,
        std::vector<String> & exprs,
        std::vector<String> & pats,
        std::vector<Int64> & positions,
        std::vector<Int64> & occurs,
        std::vector<String> & match_types)
    {
        results = getResultVec<String>(test_cases);
        switch (param_num)
        {
        case 5:
            match_types = getMatchTypeVec(test_cases);
        case 4:
            occurs = getOccurVec(test_cases);
        case 3:
            positions = getPosVec(test_cases);
        case 2:
            pats = getPatVec(test_cases);
            exprs = getExprVec(test_cases);
            break;
        default:
            throw DB::Exception("Invalid param_num");
        }
    }

    static void setVecsWithNullMap(
        int param_num,
        const std::vector<RegexpSubstrCase> test_cases,
        std::vector<String> & results,
        std::vector<std::vector<UInt8>> & null_map,
        std::vector<String> & exprs,
        std::vector<String> & pats,
        std::vector<Int64> & positions,
        std::vector<Int64> & occurs,
        std::vector<String> & match_types)
    {
        null_map.clear();
        null_map.resize(REGEXP_SUBSTR_MAX_PARAM_NUM);
        for (const auto & elem : test_cases)
        {
            null_map[EXPR_NULL_MAP_IDX].push_back(elem.null_map[EXPR_NULL_MAP_IDX]);
            null_map[PAT_NULL_MAP_IDX].push_back(elem.null_map[PAT_NULL_MAP_IDX]);
            null_map[POS_NULL_MAP_IDX].push_back(elem.null_map[POS_NULL_MAP_IDX]);
            null_map[OCCUR_NULL_MAP_IDX].push_back(elem.null_map[OCCUR_NULL_MAP_IDX]);
            null_map[MATCH_TYPE_NULL_MAP_IDX].push_back(elem.null_map[MATCH_TYPE_NULL_MAP_IDX]);
        }

        setVecsWithoutNullMap(param_num, test_cases, results, exprs, pats, positions, occurs, match_types);
    }

    const static UInt8 REGEXP_SUBSTR_MAX_PARAM_NUM = 5;
    const static UInt8 EXPR_NULL_MAP_IDX = 0;
    const static UInt8 PAT_NULL_MAP_IDX = 1;
    const static UInt8 POS_NULL_MAP_IDX = 2;
    const static UInt8 OCCUR_NULL_MAP_IDX = 3;
    const static UInt8 MATCH_TYPE_NULL_MAP_IDX = 4;

    String result;
    std::vector<UInt8> null_map;
    String expression;
    String pattern;
    Int64 position;
    Int64 occurrence;
    String match_type;
};

TEST_F(RegexpSubstr, RegexpSubstrTest)
{
    // Test: All columns are const
    {
        for (size_t row_size = 1; row_size < 3; ++row_size)
        {
            ASSERT_COLUMN_EQ(
                createConstColumn<Nullable<String>>(row_size, "123"),
                executeFunction(
                    "regexp_substr",
                    createConstColumn<String>(row_size, "123"),
                    createConstColumn<String>(row_size, "12.")));
            ASSERT_COLUMN_EQ(
                createConstColumn<Nullable<String>>(row_size, {}),
                executeFunction(
                    "regexp_substr",
                    createConstColumn<String>(row_size, "123"),
                    createConstColumn<String>(row_size, "12."),
                    createConstColumn<UInt64>(row_size, 2)));
            ASSERT_COLUMN_EQ(
                createConstColumn<Nullable<String>>(row_size, "12"),
                executeFunction(
                    "regexp_substr",
                    createConstColumn<String>(row_size, "11212"),
                    createConstColumn<String>(row_size, "12"),
                    createConstColumn<UInt8>(row_size, 2),
                    createConstColumn<UInt64>(row_size, 2)));
            ASSERT_COLUMN_EQ(
                createConstColumn<Nullable<String>>(row_size, "ab"),
                executeFunction(
                    "regexp_substr",
                    createConstColumn<String>(row_size, "aabab"),
                    createConstColumn<String>(row_size, "aB"),
                    createConstColumn<UInt16>(row_size, 2),
                    createConstColumn<Int8>(row_size, 2),
                    createConstColumn<String>(row_size, "i")));
        }
    }

    // Test: null const
    {
        for (size_t row_size = 1; row_size < 3; ++row_size)
        {
            ASSERT_COLUMN_EQ(
                createConstColumn<Nullable<String>>(row_size, {}),
                executeFunction(
                    "regexp_substr",
                    createConstColumn<Nullable<String>>(row_size, {}),
                    createConstColumn<String>(row_size, "123")));

            ASSERT_COLUMN_EQ(
                createConstColumn<Nullable<String>>(row_size, {}),
                executeFunction(
                    "regexp_substr",
                    createConstColumn<String>(row_size, "123"),
                    createConstColumn<Nullable<String>>(row_size, {})));

            ASSERT_COLUMN_EQ(
                createConstColumn<Nullable<String>>(row_size, {}),
                executeFunction(
                    "regexp_substr",
                    createConstColumn<String>(row_size, "123"),
                    createConstColumn<String>(row_size, "12."),
                    createConstColumn<Nullable<UInt8>>(row_size, {})));

            ASSERT_COLUMN_EQ(
                createConstColumn<Nullable<String>>(row_size, {}),
                executeFunction(
                    "regexp_substr",
                    createConstColumn<String>(row_size, "123"),
                    createConstColumn<String>(row_size, "12."),
                    createConstColumn<Int8>(row_size, 2),
                    createConstColumn<Nullable<UInt8>>(row_size, {})));

            ASSERT_COLUMN_EQ(
                createConstColumn<Nullable<String>>(row_size, {}),
                executeFunction(
                    "regexp_substr",
                    createConstColumn<String>(row_size, "123"),
                    createConstColumn<String>(row_size, "12."),
                    createConstColumn<Int8>(row_size, 2),
                    createConstColumn<Int8>(row_size, 2),
                    createConstColumn<Nullable<String>>(row_size, {})));
        }
    }

    std::vector<RegexpSubstrCase> test_cases;
    std::vector<String> results;
    std::vector<std::vector<UInt8>> null_maps;
    std::vector<String> exprs;
    std::vector<String> patterns;
    std::vector<Int64> positions;
    std::vector<Int64> occurs;
    std::vector<String> match_types;

    // Test: All columns are pure vector
    {
        // test regexp_substr(vector, vector)
        test_cases
            = {{"tifl", "ttttifl", "tifl"},
               {"tidb", "tidb_tikv", "ti(db|kv)"},
               {"aa", "aaaaaa", "a."},
               {"", "\n", "."},
               {"", "", "^$"},
               {"", "ab\naB", "^ab$"},
               {"跑", "pp跑ppのaaa", "(跑|の|P)"}};
        RegexpSubstrCase::setVecsWithoutNullMap(
            2,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, {0, 0, 0, 1, 0, 1, 0}),
            executeFunction("regexp_substr", createColumn<String>(exprs), createColumn<String>(patterns)));

        // test regexp_substr(vector, vector, vector)
        test_cases
            = {{"tifl", "ttttifl", "tifl", 3},
               {"tikv", "tidb_tikv", "ti(db|kv)", 2},
               {"aa", "aaaaaa", "aa", 3},
               {"", "\n", ".", 1},
               {"", "ab\naB", "^ab$", 1},
               {"跑", "pp跑ppのaaa", "(跑|の|P)", 2}};
        RegexpSubstrCase::setVecsWithoutNullMap(
            3,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, {0, 0, 0, 1, 1, 0}),
            executeFunction(
                "regexp_substr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int32>(positions)));

        // test regexp_substr(vector, vector, vector, vector)
        test_cases
            = {{"tifl", "ttttifl", "tifl", 3, 1},
               {"tikv", "tidb_tikv", "ti(db|kv)", 2, 1},
               {"aa", "aaaaaa", "aa", 3, 2},
               {"", "\n", ".", 1, 1},
               {"", "ab\naB", "^ab$", 1, 1},
               {"の", "pp跑ppのaaa", "(跑|の|P)", 2, 2}};
        RegexpSubstrCase::setVecsWithoutNullMap(
            4,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, {0, 0, 0, 1, 1, 0}),
            executeFunction(
                "regexp_substr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int32>(positions),
                createColumn<Int32>(occurs)));

        // test regexp_substr(vector, vector, vector, vector, vector)
        test_cases
            = {{"tifl", "ttttifl", "tifl", 3, 1, ""},
               {"tikv", "tidb_tikv", "ti(db|kv)", 2, 1, ""},
               {"aa", "aaaaaa", "aa", 3, 2, ""},
               {"\n", "\n", ".", 1, 1, "s"},
               {"aB", "ab\naB", "^ab$", 3, 1, "mi"},
               {"跑", "pp跑ppのaaa", "(跑|の|P)", 2, 2, "i"}};
        RegexpSubstrCase::setVecsWithoutNullMap(
            5,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        results = getResultVec<String>(test_cases);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, {0, 0, 0, 0, 0, 0}),
            executeFunction(
                "regexp_substr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int32>(positions),
                createColumn<Int32>(occurs),
                createColumn<String>(match_types)));

        // test collation
        const auto * utf8mb4_general_ci_collator
            = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
        test_cases
            = {{"tiFl", "ttiFl", "tifl", 1, 1, ""},
               {"", "ttiFl", "tifl", 1, 1, "c"},
               {"tiFl", "ttiFl", "tifl", 1, 1, "i"},
               {"tiFl", "ttiFl", "tifl", 1, 1, "ci"},
               {"", "ttiFl", "tifl", 1, 1, "ic"},
               {"", "ttiFl", "tifl", 1, 1, "iccc"},
               {"", "ttiFl", "tifl", 1, 1, "icic"}};
        RegexpSubstrCase::setVecsWithoutNullMap(
            5,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        results = getResultVec<String>(test_cases);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, {0, 1, 0, 0, 1, 1, 1}),
            executeFunction(
                "regexp_substr",
                {createColumn<String>(exprs),
                 createColumn<String>(patterns),
                 createColumn<Int32>(positions),
                 createColumn<Int32>(occurs),
                 createColumn<String>(match_types)},
                utf8mb4_general_ci_collator));
    }

    // Test: Args include nullable columns
    {
        // test regexp_substr(nullable vector, vector)
        test_cases
            = {{"", {{1, 0, 0, 0, 0}}, "ttttifl", "tifl"}, {"tidb", {{0, 0, 0, 0, 0}}, "tidb_tikv", "ti(db|kv)"}};
        RegexpSubstrCase::setVecsWithNullMap(
            2,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, null_maps[RegexpSubstrCase::EXPR_NULL_MAP_IDX]),
            executeFunction(
                "regexp_substr",
                createNullableVectorColumn<String>(exprs, null_maps[RegexpSubstrCase::EXPR_NULL_MAP_IDX]),
                createColumn<String>(patterns)));

        // test regexp_substr(vector, nullable vector)
        test_cases
            = {{"tifl", {{0, 0, 0, 0, 0}}, "ttttifl", "tifl"}, {"", {{0, 1, 0, 0, 0}}, "tidb_tikv", "ti(db|kv)"}};
        RegexpSubstrCase::setVecsWithNullMap(
            2,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, null_maps[RegexpSubstrCase::PAT_NULL_MAP_IDX]),
            executeFunction(
                "regexp_substr",
                createColumn<String>(exprs),
                createNullableVectorColumn<String>(patterns, null_maps[RegexpSubstrCase::PAT_NULL_MAP_IDX])));

        // test regexp_substr(vector, vector, nullable vector)
        test_cases = {{"tifl", {{0, 0, 0, 0, 0}}, "ttttifl", "tifl", 3}, {"", {{0, 0, 1, 0, 0}}, "ttttifl", "tifl", 3}};
        RegexpSubstrCase::setVecsWithNullMap(
            3,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, null_maps[RegexpSubstrCase::POS_NULL_MAP_IDX]),
            executeFunction(
                "regexp_substr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createNullableVectorColumn<Int64>(positions, null_maps[RegexpSubstrCase::POS_NULL_MAP_IDX])));

        // test regexp_substr(vector, vector, vector, nullable vector)
        test_cases
            = {{"tikv", {{0, 0, 0, 0, 0}}, "tidb_tikv", "ti(db|kv)", 1, 2},
               {"", {{0, 0, 0, 1, 0}}, "tidb_tikv", "ti(db|kv)", 1, 2}};
        RegexpSubstrCase::setVecsWithNullMap(
            4,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, null_maps[RegexpSubstrCase::OCCUR_NULL_MAP_IDX]),
            executeFunction(
                "regexp_substr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int64>(positions),
                createNullableVectorColumn<Int64>(occurs, null_maps[RegexpSubstrCase::OCCUR_NULL_MAP_IDX])));

        // test regexp_substr(vector, vector, vector, vector, nullable vector)
        test_cases = {{"b", {{0, 0, 0, 0, 0}}, "b", "B", 1, 1, "i"}, {"", {{0, 0, 0, 0, 1}}, "b", "B", 1, 1, "i"}};
        RegexpSubstrCase::setVecsWithNullMap(
            5,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, null_maps[RegexpSubstrCase::MATCH_TYPE_NULL_MAP_IDX]),
            executeFunction(
                "regexp_substr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int64>(positions),
                createColumn<Int64>(occurs),
                createNullableVectorColumn<String>(match_types, null_maps[RegexpSubstrCase::MATCH_TYPE_NULL_MAP_IDX])));
    }

    // Test: const, nullable and pure vector columns appear together
    {
        // test regexp_substr(nullable vector, vector, nullable vector, vector, const vector, vector)
        test_cases
            = {{"tidb", {{0, 0, 0, 0, 0}}, "tidb_tikv", "ti(db|Kv)", 1, 1, "i"},
               {"", {{1, 0, 0, 0, 0}}, "tidb_tikv", "ti(db|Kv)", 1, 1, "i"},
               {"", {{0, 0, 1, 0, 0}}, "tidb_tikv", "ti(db|Kv)", 1, 1, "i"},
               {"", {{1, 0, 1, 0, 0}}, "tidb_tikv", "ti(db|Kv)", 1, 1, "i"}};
        RegexpSubstrCase::setVecsWithNullMap(
            5,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, {0, 1, 1, 1}),
            executeFunction(
                "regexp_substr",
                createNullableVectorColumn<String>(exprs, null_maps[RegexpSubstrCase::EXPR_NULL_MAP_IDX]),
                createColumn<String>(patterns),
                createNullableVectorColumn<Int64>(positions, null_maps[RegexpSubstrCase::POS_NULL_MAP_IDX]),
                createColumn<Int64>(occurs),
                createColumn<String>(match_types)));
    }

    // Test: empty column tests
    {
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(0, ""),
            executeFunction(
                "regexp_substr",
                createConstColumn<String>(0, "m"),
                createConstColumn<String>(0, "m"),
                createConstColumn<Int32>(0, 1),
                createConstColumn<Int32>(0, 1),
                createConstColumn<String>(0, "m")));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({}),
            executeFunction(
                "regexp_substr",
                createColumn<String>({}),
                createColumn<String>({}),
                createColumn<Int32>({}),
                createColumn<Int32>({}),
                createColumn<String>({})));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({}),
            executeFunction(
                "regexp_substr",
                createColumn<String>({}),
                createColumn<String>({}),
                createConstColumn<Int32>(0, 1),
                createColumn<Int32>({}),
                createConstColumn<String>(0, "")));
    }

    // Test: Invalid parameter handling
    {
        // test empty pattern
        test_cases = {{"", "ttt", ""}};
        RegexpSubstrCase::setVecsWithoutNullMap(
            2,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        ASSERT_THROW(
            executeFunction(
                "regexp_substr",
                createNullableVectorColumn<String>(exprs, {0}),
                createColumn<String>(patterns)),
            Exception);

        // test invalid match type
        test_cases = {{"", "ttt", "t", 1, 1, "p"}};
        RegexpSubstrCase::setVecsWithoutNullMap(
            5,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            match_types);
        ASSERT_THROW(
            executeFunction(
                "regexp_substr",
                createNullableVectorColumn<String>(exprs, {0}),
                createColumn<String>(patterns),
                createColumn<Int64>(positions),
                createColumn<Int64>(occurs),
                createColumn<String>(match_types)),
            Exception);
    }
}
} // namespace tests
} // namespace DB
