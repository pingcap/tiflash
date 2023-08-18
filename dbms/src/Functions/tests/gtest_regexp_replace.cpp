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

#include <Functions/FunctionsRegexpReplace.h>
#include <Functions/tests/regexp_test_util.h>

namespace DB
{
namespace tests
{
class RegexpReplace : public Regexp
{
protected:
    const char * url1 = "https://go.mail/folder-1/online/ru-en/#lingvo/#1О 50000&price_ashka/rav4/page=/check.xml";
    const char * url2
        = "http://saint-peters-total=меньше "
          "1000-rublyayusche/catalogue/kolasuryat-v-2-kadyirovka-personal/serial_id=0&input_state/apartments/"
          "mokrotochki.net/upravda.ru/yandex.ru/GameMain.aspx?mult]/on/orders/50195&text=мыс и орелка в Балаш смотреть "
          "онлайн бесплатно в хорошем камбалакс&lr=20030393833539353862643188&op_promo=C-Teaser_id=06d162.html";

    const char * url1_repl = "a\\12\\13";
    const char * url1_res = "ago.mail2go.mail3";

    const char * url2_repl = "aaa\\1233";
    const char * url2_res = "aaasaint-peters-total=меньше 1000-rublyayusche233";

    const char * url_pat = "^https?://(?:www\\.)?([^/]+)/.*$";
};

struct RegexpReplaceCase
{
    RegexpReplaceCase(
        const String & res,
        const String & expr,
        const String & pat,
        const String & repl,
        Int64 pos = 1,
        Int64 occur = 1,
        const String & mt = "")
        : result(res)
        , expression(expr)
        , pattern(pat)
        , replacement(repl)
        , position(pos)
        , occurrence(occur)
        , match_type(mt)
    {}

    RegexpReplaceCase(
        const String & res,
        const std::vector<UInt8> & null_map_,
        const String & expr,
        const String & pat,
        const String & repl,
        Int64 pos = 1,
        Int64 occur = 1,
        const String & mt = "")
        : result(res)
        , null_map(null_map_)
        , expression(expr)
        , pattern(pat)
        , replacement(repl)
        , position(pos)
        , occurrence(occur)
        , match_type(mt)
    {}

    static void setVecsWithoutNullMap(
        int param_num,
        const std::vector<RegexpReplaceCase> test_cases,
        std::vector<String> & results,
        std::vector<String> & exprs,
        std::vector<String> & pats,
        std::vector<String> & repls,
        std::vector<Int64> & positions,
        std::vector<Int64> & occurs,
        std::vector<String> & match_types)
    {
        results = getResultVec<String>(test_cases);
        switch (param_num)
        {
        case 6:
            match_types = getMatchTypeVec(test_cases);
        case 5:
            occurs = getOccurVec(test_cases);
        case 4:
            positions = getPosVec(test_cases);
        case 3:
            repls = getReplVec(test_cases);
            pats = getPatVec(test_cases);
            exprs = getExprVec(test_cases);
            break;
        default:
            throw DB::Exception("Invalid param_num");
        }
    }

    static void setVecsWithNullMap(
        int param_num,
        const std::vector<RegexpReplaceCase> test_cases,
        std::vector<String> & results,
        std::vector<std::vector<UInt8>> & null_map,
        std::vector<String> & exprs,
        std::vector<String> & pats,
        std::vector<String> & repls,
        std::vector<Int64> & positions,
        std::vector<Int64> & occurs,
        std::vector<String> & match_types)
    {
        null_map.clear();
        null_map.resize(REGEXP_REPLACE_MAX_PARAM_NUM);
        for (const auto & elem : test_cases)
        {
            null_map[EXPR_NULL_MAP_IDX].push_back(elem.null_map[EXPR_NULL_MAP_IDX]);
            null_map[PAT_NULL_MAP_IDX].push_back(elem.null_map[PAT_NULL_MAP_IDX]);
            null_map[POS_NULL_MAP_IDX].push_back(elem.null_map[POS_NULL_MAP_IDX]);
            null_map[REPLACE_NULL_MAP_IDX].push_back(elem.null_map[REPLACE_NULL_MAP_IDX]);
            null_map[OCCUR_NULL_MAP_IDX].push_back(elem.null_map[OCCUR_NULL_MAP_IDX]);
            null_map[MATCH_TYPE_NULL_MAP_IDX].push_back(elem.null_map[MATCH_TYPE_NULL_MAP_IDX]);
        }

        setVecsWithoutNullMap(param_num, test_cases, results, exprs, pats, repls, positions, occurs, match_types);
    }

    const static UInt8 REGEXP_REPLACE_MAX_PARAM_NUM = 6;
    const static UInt8 EXPR_NULL_MAP_IDX = 0;
    const static UInt8 PAT_NULL_MAP_IDX = 1;
    const static UInt8 REPLACE_NULL_MAP_IDX = 2;
    const static UInt8 POS_NULL_MAP_IDX = 3;
    const static UInt8 OCCUR_NULL_MAP_IDX = 4;
    const static UInt8 MATCH_TYPE_NULL_MAP_IDX = 5;

    String result;
    std::vector<UInt8> null_map;
    String expression;
    String pattern;
    String replacement;
    Int64 position;
    Int64 occurrence;
    String match_type;
};

TEST_F(RegexpReplace, RegexpReplaceTest)
{
    const auto * binary_collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY);
    auto string_type = std::make_shared<DataTypeString>();
    auto nullable_string_type = makeNullable(string_type);
    auto uint8_type = std::make_shared<DataTypeUInt8>();
    auto nullable_uint8_type = makeNullable(uint8_type);

    std::vector<String> input_strings{"abb\nabbabb", "abbcabbabb", "abbabbabb", "ABBABBABB", "ABB\nABBABB", url1, url2};
    std::vector<UInt8> input_string_nulls{0, 1, 0, 0, 0, 0, 0};

    std::vector<String> patterns{"^a.*", "bb", "abc", "abb", "abb.abb", url_pat, url_pat};
    std::vector<UInt8> pattern_nulls{0, 0, 1, 0, 0, 0, 0};

    std::vector<String> replacements{"xxx", "xxx", "xxx", "xxx", "xxx", url1_repl, url2_repl};
    std::vector<UInt8> replacement_nulls{0, 0, 1, 0, 0, 0, 0};

    std::vector<Int64> pos{1, 3, 2, 2, 1, 1, 1};
    std::vector<UInt8> pos_nulls{0, 0, 0, 1, 0, 0, 0};

    std::vector<Int64> occ{0, 2, 0, 0, 0, 1, 0};
    std::vector<UInt8> occ_nulls{1, 0, 0, 0, 0, 0, 0};

    std::vector<String> match_types{"is", "", "", "i", "ism", "", ""};
    std::vector<UInt8> match_type_nulls{1, 0, 0, 0, 0, 0, 0};

    std::vector<String>
        results{"xxx\nabbabb", "axxxcaxxxaxxx", "abbabbabb", "ABBABBABB", "ABB\nABBABB", url1_res, url2_res};
    std::vector<String>
        results_with_pos{"xxx\nabbabb", "abbcaxxxaxxx", "abbabbabb", "ABBABBABB", "ABB\nABBABB", url1_res, url2_res};
    std::vector<String>
        results_with_pos_occ{"xxx\nabbabb", "abbcabbaxxx", "abbabbabb", "ABBABBABB", "ABB\nABBABB", url1_res, url2_res};
    std::vector<String>
        results_with_pos_occ_match_type{"xxx", "abbcabbaxxx", "abbabbabb", "ABBxxxxxx", "xxxABB", url1_res, url2_res};
    std::vector<String> results_with_pos_occ_match_type_binary{
        "xxx",
        "abbcabbaxxx",
        "abbabbabb",
        "ABBABBABB",
        "ABB\nABBABB",
        url1_res,
        url2_res};

    std::vector<String> vec_results{"xxx\nabbabb", "xxx", "xxx", "ABBABBABB", "ABB\nABBABB", url1, url2};
    std::vector<String> vec_results_with_pos{"xxx\nabbabb", "xxx", "xxx", "ABBABBABB", "ABB\nABBABB", url1, url2};
    std::vector<String> vec_results_with_pos_occ{"xxx\nabbabb", "xxx", "xxx", "ABBABBABB", "ABB\nABBABB", url1, url2};
    std::vector<String> vec_results_with_pos_occ_match_type{"xxx", "xxx", "xxx", "xxx", "xxx", url1, url2};
    std::vector<String>
        vec_results_with_pos_occ_match_type_binary{"xxx", "xxx", "xxx", "ABBABBABB", "ABB\nABBABB", url1, url2};

    size_t row_size = input_strings.size();
    auto const_string_null_column = createConstColumn<Nullable<String>>(row_size, {});
    auto const_int64_null_column = createConstColumn<Nullable<Int64>>(row_size, {});

    /// regexp_replace is not supported in TiDB yet, so use raw function test
    /// case 1. regexp_replace(const, const, const [, const, const ,const])
    for (size_t i = 0; i < match_types.size(); i++)
    {
        /// test regexp_replace(str, pattern, replacement)
        ASSERT_COLUMN_EQ(
            createConstColumn<String>(row_size, results[i]),
            executeFunction(
                "regexp_replace",
                {createConstColumn<String>(row_size, input_strings[i]),
                 createConstColumn<String>(row_size, patterns[i]),
                 createConstColumn<String>(row_size, replacements[i])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos)
        ASSERT_COLUMN_EQ(
            createConstColumn<String>(row_size, results_with_pos[i]),
            executeFunction(
                "regexp_replace",
                {createConstColumn<String>(row_size, input_strings[i]),
                 createConstColumn<String>(row_size, patterns[i]),
                 createConstColumn<String>(row_size, replacements[i]),
                 createConstColumn<Int64>(row_size, pos[i])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos, occ)
        ASSERT_COLUMN_EQ(
            createConstColumn<String>(row_size, results_with_pos_occ[i]),
            executeFunction(
                "regexp_replace",
                {createConstColumn<String>(row_size, input_strings[i]),
                 createConstColumn<String>(row_size, patterns[i]),
                 createConstColumn<String>(row_size, replacements[i]),
                 createConstColumn<Int64>(row_size, pos[i]),
                 createConstColumn<Int64>(row_size, occ[i])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type)
        ASSERT_COLUMN_EQ(
            createConstColumn<String>(row_size, results_with_pos_occ_match_type[i]),
            executeFunction(
                "regexp_replace",
                {createConstColumn<String>(row_size, input_strings[i]),
                 createConstColumn<String>(row_size, patterns[i]),
                 createConstColumn<String>(row_size, replacements[i]),
                 createConstColumn<Int64>(row_size, pos[i]),
                 createConstColumn<Int64>(row_size, occ[i]),
                 createConstColumn<String>(row_size, match_types[i])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type) with binary collator
        ASSERT_COLUMN_EQ(
            createConstColumn<String>(row_size, results_with_pos_occ_match_type_binary[i]),
            executeFunction(
                "regexp_replace",
                {createConstColumn<String>(row_size, input_strings[i]),
                 createConstColumn<String>(row_size, patterns[i]),
                 createConstColumn<String>(row_size, replacements[i]),
                 createConstColumn<Int64>(row_size, pos[i]),
                 createConstColumn<Int64>(row_size, occ[i]),
                 createConstColumn<String>(row_size, match_types[i])},
                binary_collator,
                true));
    }

    /// case 2. regexp_replace(const, const, const [, const, const ,const]) with null value
    for (size_t i = 0; i < match_types.size(); i++)
    {
        /// test regexp_replace(str, pattern, replacement)
        bool null_result = input_string_nulls[i] || pattern_nulls[i] || replacement_nulls[i];
        ASSERT_COLUMN_EQ(
            null_result ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, results[i]),
            executeFunction(
                "regexp_replace",
                {input_string_nulls[i] ? const_string_null_column
                                       : createConstColumn<Nullable<String>>(row_size, input_strings[i]),
                 pattern_nulls[i] ? const_string_null_column
                                  : createConstColumn<Nullable<String>>(row_size, patterns[i]),
                 replacement_nulls[i] ? const_string_null_column
                                      : createConstColumn<Nullable<String>>(row_size, replacements[i])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos)
        null_result = null_result || pos_nulls[i];
        ASSERT_COLUMN_EQ(
            null_result ? const_string_null_column : createConstColumn<Nullable<String>>(row_size, results_with_pos[i]),
            executeFunction(
                "regexp_replace",
                {input_string_nulls[i] ? const_string_null_column
                                       : createConstColumn<Nullable<String>>(row_size, input_strings[i]),
                 pattern_nulls[i] ? const_string_null_column
                                  : createConstColumn<Nullable<String>>(row_size, patterns[i]),
                 replacement_nulls[i] ? const_string_null_column
                                      : createConstColumn<Nullable<String>>(row_size, replacements[i]),
                 pos_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, pos[i])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos, occ)
        null_result = null_result || occ_nulls[i];
        ASSERT_COLUMN_EQ(
            null_result ? const_string_null_column
                        : createConstColumn<Nullable<String>>(row_size, results_with_pos_occ[i]),
            executeFunction(
                "regexp_replace",
                {input_string_nulls[i] ? const_string_null_column
                                       : createConstColumn<Nullable<String>>(row_size, input_strings[i]),
                 pattern_nulls[i] ? const_string_null_column
                                  : createConstColumn<Nullable<String>>(row_size, patterns[i]),
                 replacement_nulls[i] ? const_string_null_column
                                      : createConstColumn<Nullable<String>>(row_size, replacements[i]),
                 pos_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, pos[i]),
                 occ_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, occ[i])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type)
        null_result = null_result || match_type_nulls[i];
        ASSERT_COLUMN_EQ(
            null_result ? const_string_null_column
                        : createConstColumn<Nullable<String>>(row_size, results_with_pos_occ_match_type[i]),
            executeFunction(
                "regexp_replace",
                {input_string_nulls[i] ? const_string_null_column
                                       : createConstColumn<Nullable<String>>(row_size, input_strings[i]),
                 pattern_nulls[i] ? const_string_null_column
                                  : createConstColumn<Nullable<String>>(row_size, patterns[i]),
                 replacement_nulls[i] ? const_string_null_column
                                      : createConstColumn<Nullable<String>>(row_size, replacements[i]),
                 pos_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, pos[i]),
                 occ_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, occ[i]),
                 match_type_nulls[i] ? const_string_null_column
                                     : createConstColumn<Nullable<String>>(row_size, match_types[i])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type) with binary collator
        ASSERT_COLUMN_EQ(
            null_result ? const_string_null_column
                        : createConstColumn<Nullable<String>>(row_size, results_with_pos_occ_match_type_binary[i]),
            executeFunction(
                "regexp_replace",
                {input_string_nulls[i] ? const_string_null_column
                                       : createConstColumn<Nullable<String>>(row_size, input_strings[i]),
                 pattern_nulls[i] ? const_string_null_column
                                  : createConstColumn<Nullable<String>>(row_size, patterns[i]),
                 replacement_nulls[i] ? const_string_null_column
                                      : createConstColumn<Nullable<String>>(row_size, replacements[i]),
                 pos_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, pos[i]),
                 occ_nulls[i] ? const_int64_null_column : createConstColumn<Nullable<Int64>>(row_size, occ[i]),
                 match_type_nulls[i] ? const_string_null_column
                                     : createConstColumn<Nullable<String>>(row_size, match_types[i])},
                binary_collator,
                true));
    }

    /// case 3 regexp_replace(vector, const, const[, const, const, const])
    {
        /// test regexp_replace(str, pattern, replacement)
        ASSERT_COLUMN_EQ(
            createColumn<String>(vec_results),
            executeFunction(
                "regexp_replace",
                {createColumn<String>(input_strings),
                 createConstColumn<String>(row_size, patterns[0]),
                 createConstColumn<String>(row_size, replacements[0])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos)
        ASSERT_COLUMN_EQ(
            createColumn<String>(vec_results_with_pos),
            executeFunction(
                "regexp_replace",
                {createColumn<String>(input_strings),
                 createConstColumn<String>(row_size, patterns[0]),
                 createConstColumn<String>(row_size, replacements[0]),
                 createConstColumn<Int64>(row_size, pos[0])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos, occ)
        ASSERT_COLUMN_EQ(
            createColumn<String>(vec_results_with_pos_occ),
            executeFunction(
                "regexp_replace",
                {createColumn<String>(input_strings),
                 createConstColumn<String>(row_size, patterns[0]),
                 createConstColumn<String>(row_size, replacements[0]),
                 createConstColumn<Int64>(row_size, pos[0]),
                 createConstColumn<Int64>(row_size, occ[0])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type)
        ASSERT_COLUMN_EQ(
            createColumn<String>(vec_results_with_pos_occ_match_type),
            executeFunction(
                "regexp_replace",
                {createColumn<String>(input_strings),
                 createConstColumn<String>(row_size, patterns[0]),
                 createConstColumn<String>(row_size, replacements[0]),
                 createConstColumn<Int64>(row_size, pos[0]),
                 createConstColumn<Int64>(row_size, occ[0]),
                 createConstColumn<String>(row_size, match_types[0])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type) with binary collator
        ASSERT_COLUMN_EQ(
            createColumn<String>(vec_results_with_pos_occ_match_type_binary),
            executeFunction(
                "regexp_replace",
                {createColumn<String>(input_strings),
                 createConstColumn<String>(row_size, patterns[0]),
                 createConstColumn<String>(row_size, replacements[0]),
                 createConstColumn<Int64>(row_size, pos[0]),
                 createConstColumn<Int64>(row_size, occ[0]),
                 createConstColumn<String>(row_size, match_types[0])},
                binary_collator,
                true));
    }

    /// case 4 regexp_replace(vector, const, const[, const, const, const]) with null value
    {
        /// test regexp_replace(str, pattern, replacement)
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(vec_results, input_string_nulls),
            executeFunction(
                "regexp_replace",
                {createNullableVectorColumn<String>(input_strings, input_string_nulls),
                 createConstColumn<String>(row_size, patterns[0]),
                 createConstColumn<String>(row_size, replacements[0])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos)
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(vec_results_with_pos, input_string_nulls),
            executeFunction(
                "regexp_replace",
                {createNullableVectorColumn<String>(input_strings, input_string_nulls),
                 createConstColumn<String>(row_size, patterns[0]),
                 createConstColumn<String>(row_size, replacements[0]),
                 createConstColumn<Int64>(row_size, pos[0])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos, occ)
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(vec_results_with_pos_occ, input_string_nulls),
            executeFunction(
                "regexp_replace",
                {createNullableVectorColumn<String>(input_strings, input_string_nulls),
                 createConstColumn<String>(row_size, patterns[0]),
                 createConstColumn<String>(row_size, replacements[0]),
                 createConstColumn<Int64>(row_size, pos[0]),
                 createConstColumn<Int64>(row_size, occ[0])},
                nullptr,
                true));


        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type)
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(vec_results_with_pos_occ_match_type, input_string_nulls),
            executeFunction(
                "regexp_replace",
                {createNullableVectorColumn<String>(input_strings, input_string_nulls),
                 createConstColumn<String>(row_size, patterns[0]),
                 createConstColumn<String>(row_size, replacements[0]),
                 createConstColumn<Int64>(row_size, pos[0]),
                 createConstColumn<Int64>(row_size, occ[0]),
                 createConstColumn<String>(row_size, match_types[0])},
                nullptr,
                true));

        /// test regexp_replace(str, pattern, replacement, pos, occ, match_type) with binary collator
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(vec_results_with_pos_occ_match_type_binary, input_string_nulls),
            executeFunction(
                "regexp_replace",
                {createNullableVectorColumn<String>(input_strings, input_string_nulls),
                 createConstColumn<String>(row_size, patterns[0]),
                 createConstColumn<String>(row_size, replacements[0]),
                 createConstColumn<Int64>(row_size, pos[0]),
                 createConstColumn<Int64>(row_size, occ[0]),
                 createConstColumn<String>(row_size, match_types[0])},
                binary_collator,
                true));
    }

    std::vector<RegexpReplaceCase> test_cases;
    std::vector<std::vector<UInt8>> null_maps;
    std::vector<String> exprs;
    std::vector<String> repls;
    std::vector<Int64> positions;
    std::vector<Int64> occurs;

    /// case 5 regexp_replace(vector, vector, vector[, vector, vector, vector])
    {
        test_cases
            = {{"taa", "ttifl", "tifl", "aa", 1, 0, ""},
               {"aaaaaa", "121212", "1.", "aa", 1, 0, ""},
               {"aa1212", "121212", "1.", "aa", 1, 1, ""},
               {"12aa12", "121212", "1.", "aa", 1, 2, ""},
               {"1212aa", "121212", "1.", "aa", 1, 3, ""},
               {"121212", "121212", "1.", "aa", 1, 4, ""},
               {"sea1dad8 1lal8", "seafood fool", "foo(.?)", "1\\1a\\18", 1, 0, ""},
               {"啊ah好a哈哈", "啊a哈a哈哈", "哈", "h好", 1, 1, ""},
               {"啊a哈ah好哈", "啊a哈a哈哈", "哈", "h好", 4, 1, ""},
               {"啊a哈a哈哈", "啊a哈a哈哈", "哈", "h好", 4, 5, ""},
               {"aa", "\n", ".", "aa", 1, 0, "s"},
               {"12aa34", "12\n34", ".", "aa", 3, 1, "s"},
               {R"(\+overflow+stack+overflow+stack)", "stackoverflow", "(.{5})(.*)", R"(\\+\2+\1+\2+\1\)", 1, 0, ""},
               {R"(\i\h-g\f-e\d-c\b-a\ \I\H-G\F-E\D-C\B-A\)",
                "fooabcdefghij fooABCDEFGHIJ",
                "foo(.)(.)(.)(.)(.)(.)(.)(.)(.)(.)",
                R"(\\\9\\\8-\7\\\6-\5\\\4-\3\\\2-\1\\)",
                1,
                0,
                ""},
               {"apbc", "abc", "\\d*", "p", 1, 2, ""},
               {"abcp", "abc", "\\d*", "p", 1, 4, ""},
               {"papbpcp", "abc", "\\d*", "p", 1, 0, ""},
               {"abcp", "abc", "\\d*$", "p", 1, 1, ""},
               {"我p们", "我们", "\\d*", "p", 1, 2, ""},
               {"p我p们p", "我们", "\\d*", "p", 1, 0},
               {"我们p", "我们", "\\d*", "p", 1, 3},
               {"p", "123", "(123)||(^$)", "p", 1, 1, ""},
               {"123p", "123", "(123)||(^$)", "p", 1, 2, ""},
               {"pp", "123", "(123)||(^$)", "p", 1, 0, ""}};
        RegexpReplaceCase::setVecsWithoutNullMap(
            6,
            test_cases,
            results,
            exprs,
            patterns,
            repls,
            positions,
            occurs,
            match_types);
        results = getResultVec<String>(test_cases);
        ASSERT_COLUMN_EQ(
            createColumn<String>(results),
            executeFunction(
                "regexp_replace",
                createColumn<String>(exprs),
                createColumn<String>(patterns),
                createColumn<String>(repls),
                createColumn<Int32>(positions),
                createColumn<Int32>(occurs),
                createColumn<String>(match_types)));
    }

    /// case 6 regexp_replace(vector, vector, const[, const, const, vector]) with null value
    {
        test_cases
            = {{"taa", {0, 0, 1, 0, 0, 0}, "ttifl", "tifl", "aa", 1, 0, ""},
               {"aaaaaa", {0, 0, 0, 0, 0, 0}, "121212", "1.", "aa", 1, 0, ""},
               {"aa1212", {0, 1, 0, 0, 0, 0}, "121212", "1.", "aa", 1, 1, ""},
               {"12aa12", {0, 0, 0, 0, 0, 0}, "121212", "1.", "aa", 1, 2, ""},
               {"1212aa", {0, 1, 0, 0, 0, 0}, "121212", "1.", "aa", 1, 3, ""},
               {"121212", {0, 0, 0, 0, 0, 0}, "121212", "1.", "aa", 1, 4, ""},
               {"seafood 1lal8", {0, 0, 0, 0, 0, 0}, "seafood fool", "foo(.?)", "1\\1a\\18", 1, 2, ""},
               {"啊ah好a哈哈", {0, 1, 0, 0, 0, 0}, "啊a哈a哈哈", "哈", "h好", 1, 1, ""},
               {"啊a哈ah好哈", {0, 0, 0, 0, 0, 0}, "啊a哈a哈哈", "哈", "h好", 4, 1, ""},
               {"啊a哈a哈哈", {0, 1, 0, 0, 0, 0}, "啊a哈a哈哈", "哈", "h好", 4, 5, ""},
               {"aa", {0, 1, 0, 0, 0, 0}, "\n", ".", "aa", 1, 0, "s"},
               {"12aa34", {0, 0, 0, 0, 0, 0}, "12\n34", ".", "aa", 3, 1, "s"}};
        RegexpReplaceCase::setVecsWithNullMap(
            6,
            test_cases,
            results,
            null_maps,
            exprs,
            patterns,
            repls,
            positions,
            occurs,
            match_types);
        results = getResultVec<String>(test_cases);
        ASSERT_COLUMN_EQ(
            createNullableVectorColumn<String>(results, null_maps[RegexpReplaceCase::PAT_NULL_MAP_IDX]),
            executeFunction(
                "regexp_replace",
                createColumn<String>(exprs),
                createNullableVectorColumn<String>(patterns, null_maps[RegexpReplaceCase::PAT_NULL_MAP_IDX]),
                createColumn<String>(repls),
                createColumn<Int32>(positions),
                createColumn<Int32>(occurs),
                createColumn<String>(match_types)));
    }

    /// case 7: test some special cases
    {
        // test empty expr
        ASSERT_COLUMN_EQ(
            createColumn<String>({"aa", "aa", "aa", "", ""}),
            executeFunction(
                "regexp_replace",
                {createColumn<String>({"", "", "", "", ""}),
                 createColumn<String>({"^$", "^$", "^$", "^$", "12"}),
                 createColumn<String>({"aa", "aa", "aa", "aa", "aa"}),
                 createColumn<Int64>({1, 1, 1, 1, 1}),
                 createColumn<Int64>({-1, 0, 1, 2, 3})},
                nullptr,
                true));
    }
}
} // namespace tests
} // namespace DB
