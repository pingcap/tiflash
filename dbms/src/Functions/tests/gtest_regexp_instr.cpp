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

#include <Functions/FunctionsRegexpInstr.h>
#include <Functions/tests/regexp_test_util.h>

namespace DB
{
namespace tests
{
class RegexpInstr : public Regexp
{
};

struct RegexpInstrCase
{
    RegexpInstrCase(
        Int64 res,
        const String & expr,
        const String & pat,
        Int64 pos = 1,
        Int64 occur = 1,
        Int64 ret_op = 0,
        const String & mt = "")
        : result(res)
        , expression(expr)
        , pattern(pat)
        , position(pos)
        , occurrence(occur)
        , return_option(ret_op)
        , match_type(mt)
    {}

    RegexpInstrCase(
        Int64 res,
        const std::vector<UInt8> & null_map_,
        const String & expr,
        const String & pat,
        Int64 pos = 1,
        Int64 occur = 1,
        Int64 ret_op = 0,
        const String & mt = "")
        : result(res)
        , null_map(null_map_)
        , expression(expr)
        , pattern(pat)
        , position(pos)
        , occurrence(occur)
        , return_option(ret_op)
        , match_type(mt)
    {}

    static void setVecsWithoutNullMap(
        int param_num,
        const std::vector<RegexpInstrCase> test_cases,
        std::vector<Int64> & results,
        std::vector<String> & exprs,
        std::vector<String> & pats,
        std::vector<Int64> & positions,
        std::vector<Int64> & occurs,
        std::vector<Int64> & ret_ops,
        std::vector<String> & match_types)
    {
        results = getResultVec<Int64>(test_cases);
        switch (param_num)
        {
        case 6:
            match_types = getMatchTypeVec(test_cases);
        case 5:
            ret_ops = getRetOpVec(test_cases);
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
        const std::vector<RegexpInstrCase> test_cases,
        std::vector<Int64> & results,
        std::vector<std::vector<UInt8>> & null_map,
        std::vector<String> & exprs,
        std::vector<String> & pats,
        std::vector<Int64> & positions,
        std::vector<Int64> & occurs,
        std::vector<Int64> & ret_ops,
        std::vector<String> & match_types)
    {
        null_map.clear();
        null_map.resize(REGEXP_INSTR_MAX_PARAM_NUM);
        for (const auto & elem : test_cases)
        {
            null_map[EXPR_NULL_MAP_IDX].push_back(elem.null_map[EXPR_NULL_MAP_IDX]);
            null_map[PAT_NULL_MAP_IDX].push_back(elem.null_map[PAT_NULL_MAP_IDX]);
            null_map[POS_NULL_MAP_IDX].push_back(elem.null_map[POS_NULL_MAP_IDX]);
            null_map[OCCUR_NULL_MAP_IDX].push_back(elem.null_map[OCCUR_NULL_MAP_IDX]);
            null_map[RET_OP_NULL_MAP_IDX].push_back(elem.null_map[RET_OP_NULL_MAP_IDX]);
            null_map[MATCH_TYPE_NULL_MAP_IDX].push_back(elem.null_map[MATCH_TYPE_NULL_MAP_IDX]);
        }

        setVecsWithoutNullMap(param_num, test_cases, results, exprs, pats, positions, occurs, ret_ops, match_types);
    }

    const static UInt8 REGEXP_INSTR_MAX_PARAM_NUM = 6;
    const static UInt8 EXPR_NULL_MAP_IDX = 0;
    const static UInt8 PAT_NULL_MAP_IDX = 1;
    const static UInt8 POS_NULL_MAP_IDX = 2;
    const static UInt8 OCCUR_NULL_MAP_IDX = 3;
    const static UInt8 RET_OP_NULL_MAP_IDX = 4;
    const static UInt8 MATCH_TYPE_NULL_MAP_IDX = 5;

    Int64 result;
    std::vector<UInt8> null_map;
    String expression;
    String pattern;
    Int64 position;
    Int64 occurrence;
    Int64 return_option;
    String match_type;
};

TEST_F(RegexpInstr, RegexpInstrTest)
{
    // Test: All columns are const
    {
        for (size_t row_size = 1; row_size < 3; ++row_size)
        {
            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(row_size, 1),
                executeFunction(
                    "regexp_instr",
                    createConstColumn<String>(row_size, "123"),
                    createConstColumn<String>(row_size, "12.")));
            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(row_size, 0),
                executeFunction(
                    "regexp_instr",
                    createConstColumn<String>(row_size, "123"),
                    createConstColumn<String>(row_size, "12."),
                    createConstColumn<UInt64>(row_size, 2)));
            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(row_size, 4),
                executeFunction(
                    "regexp_instr",
                    createConstColumn<String>(row_size, "11212"),
                    createConstColumn<String>(row_size, "12"),
                    createConstColumn<UInt8>(row_size, 2),
                    createConstColumn<UInt64>(row_size, 2)));
            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(row_size, 6),
                executeFunction(
                    "regexp_instr",
                    createConstColumn<String>(row_size, "11212"),
                    createConstColumn<String>(row_size, "12"),
                    createConstColumn<UInt64>(row_size, 2),
                    createConstColumn<Int16>(row_size, 2),
                    createConstColumn<Int32>(row_size, 1)));
            ASSERT_COLUMN_EQ(
                createConstColumn<Int64>(row_size, 6),
                executeFunction(
                    "regexp_instr",
                    createConstColumn<String>(row_size, "aabab"),
                    createConstColumn<String>(row_size, "aB"),
                    createConstColumn<UInt16>(row_size, 2),
                    createConstColumn<Int8>(row_size, 2),
                    createConstColumn<UInt32>(row_size, 1),
                    createConstColumn<String>(row_size, "i")));
        }
    }

    // Test: null const
    {
        size_t row_size = 2;
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(row_size, {}),
            executeFunction(
                "regexp_instr",
                createConstColumn<Nullable<String>>(row_size, {}),
                createConstColumn<String>(row_size, "123")));

        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(row_size, {}),
            executeFunction(
                "regexp_instr",
                createConstColumn<String>(row_size, "123"),
                createConstColumn<Nullable<String>>(row_size, {})));

        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(row_size, {}),
            executeFunction(
                "regexp_instr",
                createConstColumn<String>(row_size, "123"),
                createConstColumn<String>(row_size, "12."),
                createConstColumn<Nullable<UInt8>>(row_size, {})));

        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(row_size, {}),
            executeFunction(
                "regexp_instr",
                createConstColumn<String>(row_size, "123"),
                createConstColumn<String>(row_size, "12."),
                createConstColumn<Int8>(row_size, 2),
                createConstColumn<Nullable<UInt8>>(row_size, {})));

        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(row_size, {}),
            executeFunction(
                "regexp_instr",
                createConstColumn<String>(row_size, "123"),
                createConstColumn<String>(row_size, "12."),
                createConstColumn<Int8>(row_size, 2),
                createConstColumn<Int8>(row_size, 2),
                createConstColumn<Nullable<UInt8>>(row_size, {})));

        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<Int64>>(row_size, {}),
            executeFunction(
                "regexp_instr",
                createConstColumn<String>(row_size, "123"),
                createConstColumn<String>(row_size, "12."),
                createConstColumn<Int8>(row_size, 2),
                createConstColumn<Int8>(row_size, 2),
                createConstColumn<Int8>(row_size, 2),
                createConstColumn<Nullable<String>>(row_size, {})));
    }

    std::vector<RegexpInstrCase> test_cases;
    std::vector<Int64> results;
    std::vector<std::vector<UInt8>> null_maps;
    std::vector<String> exprs;
    std::vector<String> patterns;
    std::vector<Int64> positions;
    std::vector<Int64> occurs;
    std::vector<Int64> return_options;
    std::vector<String> match_types;

    // Test: All columns are pure vector
    {
        // test regexp_instr(vector, vector)
        test_cases
            = {{4, "ttttifl", "tifl"},
               {1, "tidb_tikv", "ti(db|kv)"},
               {1, "aaaaaa", "aa"},
               {0, "\n", "."},
               {1, "", "^$"},
               {0, "ab\naB", "^ab$"},
               {3, "pp跑ppのaaa", "(跑|の|P)"}};
        RegexpInstrCase::setVecsWithoutNullMap(
            2,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_COLUMN_EQ(
            createColumn<Int64>(results),
            executeFunction("regexp_instr", createColumn<String>(exprs), createColumn<String>(patterns)));

        // test regexp_instr(vector, vector, vector)
        test_cases
            = {{4, "ttttifl", "tifl", 3},
               {6, "tidb_tikv", "ti(db|kv)", 2},
               {3, "aaaaaa", "aa", 3},
               {0, "\n", ".", 1},
               {3, "", "^$", 3},
               {0, "ab\naB", "^ab$", 1},
               {3, "pp跑ppのaaa", "(跑|の|P)", 2}};
        RegexpInstrCase::setVecsWithoutNullMap(
            3,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_COLUMN_EQ(
            createColumn<Int64>(results),
            executeFunction(
                "regexp_instr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int32>(positions)));

        // test regexp_instr(vector, vector, vector, vector)
        test_cases
            = {{4, "ttttifl", "tifl", 3, 1},
               {6, "tidb_tikv", "ti(db|kv)", 2, 1},
               {5, "aaaaaa", "aa", 3, 2},
               {0, "\n", ".", 1, 1},
               {0, "", "^$", 3, 2},
               {0, "ab\naB", "^ab$", 1, 1},
               {6, "pp跑ppのaaa", "(跑|の|P)", 2, 2},
               {0, "pp跑ppのaaa", "(跑|の|P)", 2, 10}};
        RegexpInstrCase::setVecsWithoutNullMap(
            4,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_COLUMN_EQ(
            createColumn<Int64>(results),
            executeFunction(
                "regexp_instr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int32>(positions),
                createColumn<Int32>(occurs)));

        // test regexp_instr(vector, vector, vector, vector, vector)
        test_cases
            = {{8, "ttttifl", "tifl", 3, 1, 1},
               {10, "tidb_tikv", "ti(db|kv)", 2, 1, 1},
               {7, "aaaaaa", "aa", 3, 2, 1},
               {0, "\n", ".", 1, 1, 1},
               {0, "", "^$", 3, 2, 1},
               {0, "ab\naB", "^ab$", 1, 1, 1},
               {7, "pp跑ppのaaa", "(跑|の|P)", 2, 2, 1}};
        RegexpInstrCase::setVecsWithoutNullMap(
            5,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_COLUMN_EQ(
            createColumn<Int64>(results),
            executeFunction(
                "regexp_instr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int32>(positions),
                createColumn<Int32>(occurs),
                createColumn<Int32>(return_options)));

        // test regexp_instr(vector, vector, vector, vector, vector, vector)
        test_cases
            = {{8, "ttttifl", "tifl", 3, 1, 1, ""},
               {10, "tidb_tikv", "ti(db|kv)", 2, 1, 1, ""},
               {7, "aaaaaa", "aa", 3, 2, 1, ""},
               {2, "\n", ".", 1, 1, 1, "s"},
               {0, "", "^$", 3, 2, 1, ""},
               {6, "ab\naB", "^ab$", 3, 1, 1, "mi"},
               {4, "pp跑ppのaaa", "(跑|の|P)", 2, 2, 1, "i"}};
        RegexpInstrCase::setVecsWithoutNullMap(
            6,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        results = getResultVec<Int64>(test_cases);
        ASSERT_COLUMN_EQ(
            createColumn<Int64>(results),
            executeFunction(
                "regexp_instr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int32>(positions),
                createColumn<Int32>(occurs),
                createColumn<Int32>(return_options),
                createColumn<String>(match_types)));

        // test collation
        const auto * utf8mb4_general_ci_collator
            = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8MB4_GENERAL_CI);
        test_cases
            = {{2, "ttiFl", "tifl", 1, 1, 0, ""},
               {0, "ttiFl", "tifl", 1, 1, 0, "c"},
               {2, "ttiFl", "tifl", 1, 1, 0, "i"},
               {2, "ttiFl", "tifl", 1, 1, 0, "ci"},
               {0, "ttiFl", "tifl", 1, 1, 0, "ic"},
               {0, "ttiFl", "tifl", 1, 1, 0, "iccc"},
               {0, "ttiFl", "tifl", 1, 1, 0, "icic"}};
        RegexpInstrCase::setVecsWithoutNullMap(
            6,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        results = getResultVec<Int64>(test_cases);
        ASSERT_COLUMN_EQ(
            createColumn<Int64>(results),
            executeFunction(
                "regexp_instr",
                {createColumn<String>(exprs),
                 createColumn<String>(patterns),
                 createColumn<Int32>(positions),
                 createColumn<Int32>(occurs),
                 createColumn<Int32>(return_options),
                 createColumn<String>(match_types)},
                utf8mb4_general_ci_collator));
    }

    // Test: Args include nullable columns
    {
        // test regexp_instr(nullable vector, vector)
        test_cases
            = {{0, {{1, 0, 0, 0, 0, 0}}, "ttttifl", "tifl"}, {1, {{0, 0, 0, 0, 0, 0}}, "tidb_tikv", "ti(db|kv)"}};
        RegexpInstrCase::setVecsWithNullMap(
            2,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<Int64>(results, null_maps[RegexpInstrCase::EXPR_NULL_MAP_IDX]),
            executeFunction(
                "regexp_instr",
                createNullableVectorColumn<String>(exprs, null_maps[RegexpInstrCase::EXPR_NULL_MAP_IDX]),
                createColumn<String>(patterns)));

        // test regexp_instr(vector, nullable vector)
        test_cases
            = {{4, {{0, 0, 0, 0, 0, 0}}, "ttttifl", "tifl"}, {0, {{0, 1, 0, 0, 0, 0}}, "tidb_tikv", "ti(db|kv)"}};
        RegexpInstrCase::setVecsWithNullMap(
            2,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<Int64>(results, null_maps[RegexpInstrCase::PAT_NULL_MAP_IDX]),
            executeFunction(
                "regexp_instr",
                createColumn<String>(exprs),
                createNullableVectorColumn<String>(patterns, null_maps[RegexpInstrCase::PAT_NULL_MAP_IDX])));

        // test regexp_instr(vector, vector, nullable vector)
        test_cases = {{4, {{0, 0, 0, 0, 0, 0}}, "ttttifl", "tifl", 3}, {0, {{0, 0, 1, 0, 0, 0}}, "ttttifl", "tifl", 3}};
        RegexpInstrCase::setVecsWithNullMap(
            3,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<Int64>(results, null_maps[RegexpInstrCase::POS_NULL_MAP_IDX]),
            executeFunction(
                "regexp_instr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createNullableVectorColumn<Int64>(positions, null_maps[RegexpInstrCase::POS_NULL_MAP_IDX])));

        // test regexp_instr(vector, vector, vector, nullable vector)
        test_cases
            = {{6, {{0, 0, 0, 0, 0, 0}}, "tidb_tikv", "ti(db|kv)", 1, 2},
               {0, {{0, 0, 0, 1, 0, 0}}, "tidb_tikv", "ti(db|kv)", 1, 2}};
        RegexpInstrCase::setVecsWithNullMap(
            4,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<Int64>(results, null_maps[RegexpInstrCase::OCCUR_NULL_MAP_IDX]),
            executeFunction(
                "regexp_instr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int64>(positions),
                createNullableVectorColumn<Int64>(occurs, null_maps[RegexpInstrCase::OCCUR_NULL_MAP_IDX])));

        // test regexp_instr(vector, vector, vector, vector, nullable vector)
        test_cases
            = {{10, {{0, 0, 0, 0, 0, 0}}, "tidb_tikv", "ti(db|kv)", 1, 2, 1},
               {0, {{0, 0, 0, 0, 1, 0}}, "tidb_tikv", "ti(db|kv)", 1, 2, 1}};
        RegexpInstrCase::setVecsWithNullMap(
            5,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<Int64>(results, null_maps[RegexpInstrCase::RET_OP_NULL_MAP_IDX]),
            executeFunction(
                "regexp_instr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int64>(positions),
                createColumn<Int64>(occurs),
                createNullableVectorColumn<Int64>(return_options, null_maps[RegexpInstrCase::RET_OP_NULL_MAP_IDX])));

        // test regexp_instr(vector, vector, vector, vector, vector, nullable vector)
        test_cases
            = {{1, {{0, 0, 0, 0, 0, 0}}, "b", "B", 1, 1, 0, "i"}, {0, {{0, 0, 0, 0, 0, 1}}, "b", "B", 1, 1, 0, "i"}};
        RegexpInstrCase::setVecsWithNullMap(
            6,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<Int64>(results, null_maps[RegexpInstrCase::MATCH_TYPE_NULL_MAP_IDX]),
            executeFunction(
                "regexp_instr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int64>(positions),
                createColumn<Int64>(occurs),
                createColumn<Int64>(return_options),
                createNullableVectorColumn<String>(match_types, null_maps[RegexpInstrCase::MATCH_TYPE_NULL_MAP_IDX])));
    }

    // Test: const, nullable and pure vector columns appear together
    {
        // test regexp_instr(nullable vector, vector, nullable vector, vector, const vector, vector)
        test_cases
            = {{1, {{0, 0, 0, 0, 0, 0}}, "tidb_tikv", "ti(db|Kv)", 1, 1, 0, "i"},
               {0, {{1, 0, 0, 0, 0, 0}}, "tidb_tikv", "ti(db|Kv)", 1, 1, 0, "i"},
               {0, {{0, 0, 1, 0, 0, 0}}, "tidb_tikv", "ti(db|Kv)", 1, 1, 0, "i"},
               {0, {{1, 0, 1, 0, 0, 0}}, "tidb_tikv", "ti(db|Kv)", 1, 1, 0, "i"}};
        RegexpInstrCase::setVecsWithNullMap(
            6,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<Int64>(results, {0, 1, 1, 1}),
            executeFunction(
                "regexp_instr",
                createNullableVectorColumn<String>(exprs, null_maps[RegexpInstrCase::EXPR_NULL_MAP_IDX]),
                createColumn<String>(patterns),
                createNullableVectorColumn<Int64>(positions, null_maps[RegexpInstrCase::POS_NULL_MAP_IDX]),
                createColumn<Int64>(occurs),
                createConstColumn<Int32>(test_cases.size(), 0),
                createColumn<String>(match_types)));
    }

    // Test: empty column tests
    {
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(0, 1),
            executeFunction(
                "regexp_instr",
                createConstColumn<String>(0, "m"),
                createConstColumn<String>(0, "m"),
                createConstColumn<Int32>(0, 1),
                createConstColumn<Int32>(0, 1),
                createConstColumn<Int32>(0, 1),
                createConstColumn<String>(0, "m")));

        ASSERT_COLUMN_EQ(
            createColumn<Int64>({}),
            executeFunction(
                "regexp_instr",
                createColumn<String>({}),
                createColumn<String>({}),
                createColumn<Int32>({}),
                createColumn<Int32>({}),
                createColumn<Int32>({}),
                createColumn<String>({})));

        ASSERT_COLUMN_EQ(
            createColumn<Int64>({}),
            executeFunction(
                "regexp_instr",
                createColumn<String>({}),
                createColumn<String>({}),
                createConstColumn<Int32>(0, 1),
                createColumn<Int32>({}),
                createColumn<Int32>({}),
                createConstColumn<String>(0, "")));
    }

    // Test: Invalid parameter handling
    {
        // test empty pattern
        test_cases = {{0, "ttt", ""}};
        RegexpInstrCase::setVecsWithoutNullMap(
            2,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_THROW(
            executeFunction("regexp_instr", createColumn<String>(exprs), createColumn<String>(patterns)),
            Exception);

        // test invalid ret_option
        test_cases = {{0, "ttt", "t", 1, 1, 2}};
        RegexpInstrCase::setVecsWithoutNullMap(
            5,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_THROW(
            executeFunction(
                "regexp_instr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int64>(positions),
                createColumn<Int64>(occurs),
                createColumn<Int64>(return_options)),
            Exception);

        // test invalid match type
        test_cases = {{0, "ttt", "t", 1, 1, 1, "p"}};
        RegexpInstrCase::setVecsWithoutNullMap(
            6,
            test_cases,
            results,
            exprs,
            patterns,
            positions,
            occurs,
            return_options,
            match_types);
        ASSERT_THROW(
            executeFunction(
                "regexp_instr",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<Int64>(positions),
                createColumn<Int64>(occurs),
                createColumn<Int64>(return_options),
                createColumn<String>(match_types)),
            Exception);
    }
}
} // namespace tests
} // namespace DB
