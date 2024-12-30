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
#include <common/types.h>
#include <tipb/expression.pb.h>

namespace DB::tests
{
std::unordered_map<String, tipb::ScalarFuncSig> func_name_to_sig({
    {"plusint", tipb::ScalarFuncSig::PlusInt},
    {"plusreal", tipb::ScalarFuncSig::PlusReal},
    {"plusdecimal", tipb::ScalarFuncSig::PlusDecimal},
    {"minusint", tipb::ScalarFuncSig::MinusInt},
    {"minusreal", tipb::ScalarFuncSig::MinusReal},
    {"minusdecimal", tipb::ScalarFuncSig::MinusDecimal},
    {"equals", tipb::ScalarFuncSig::EQInt},
    {"notEquals", tipb::ScalarFuncSig::NEInt},
    {"and", tipb::ScalarFuncSig::LogicalAnd},
    {"or", tipb::ScalarFuncSig::LogicalOr},
    {"xor", tipb::ScalarFuncSig::LogicalXor},
    {"not", tipb::ScalarFuncSig::UnaryNotInt},
    {"greater", tipb::ScalarFuncSig::GTInt},
    {"greaterorequals", tipb::ScalarFuncSig::GEInt},
    {"less", tipb::ScalarFuncSig::LTInt},
    {"lessorequals", tipb::ScalarFuncSig::LEInt},
    {"in", tipb::ScalarFuncSig::InInt},
    {"notin", tipb::ScalarFuncSig::InInt},
    {"isnull", tipb::ScalarFuncSig::IntIsNull},
    {"date_format", tipb::ScalarFuncSig::DateFormatSig},
    {"if", tipb::ScalarFuncSig::IfInt},
    {"from_unixtime", tipb::ScalarFuncSig::FromUnixTime2Arg},
    /// bit_and/bit_or/bit_xor is aggregated function in clickhouse/mysql
    {"bitand", tipb::ScalarFuncSig::BitAndSig},
    {"bitor", tipb::ScalarFuncSig::BitOrSig},
    {"bitxor", tipb::ScalarFuncSig::BitXorSig},
    {"bitnot", tipb::ScalarFuncSig::BitNegSig},
    {"notequals", tipb::ScalarFuncSig::NEInt},
    {"like", tipb::ScalarFuncSig::LikeSig},
    {"cast_int_int", tipb::ScalarFuncSig::CastIntAsInt},
    {"cast_int_real", tipb::ScalarFuncSig::CastIntAsReal},
    {"cast_real_int", tipb::ScalarFuncSig::CastRealAsInt},
    {"cast_real_real", tipb::ScalarFuncSig::CastRealAsReal},
    {"cast_decimal_int", tipb::ScalarFuncSig::CastDecimalAsInt},
    {"cast_time_int", tipb::ScalarFuncSig::CastTimeAsInt},
    {"cast_string_int", tipb::ScalarFuncSig::CastStringAsInt},
    {"cast_int_decimal", tipb::ScalarFuncSig::CastIntAsDecimal},
    {"cast_real_decimal", tipb::ScalarFuncSig::CastRealAsDecimal},
    {"cast_decimal_decimal", tipb::ScalarFuncSig::CastDecimalAsDecimal},
    {"cast_time_decimal", tipb::ScalarFuncSig::CastTimeAsDecimal},
    {"cast_string_decimal", tipb::ScalarFuncSig::CastStringAsDecimal},
    {"cast_int_string", tipb::ScalarFuncSig::CastIntAsString},
    {"cast_real_string", tipb::ScalarFuncSig::CastRealAsString},
    {"cast_decimal_string", tipb::ScalarFuncSig::CastDecimalAsString},
    {"cast_time_string", tipb::ScalarFuncSig::CastTimeAsString},
    {"cast_string_string", tipb::ScalarFuncSig::CastStringAsString},
    {"cast_int_date", tipb::ScalarFuncSig::CastIntAsTime},
    {"cast_real_date", tipb::ScalarFuncSig::CastRealAsTime},
    {"cast_decimal_date", tipb::ScalarFuncSig::CastDecimalAsTime},
    {"cast_time_date", tipb::ScalarFuncSig::CastTimeAsTime},
    {"cast_string_date", tipb::ScalarFuncSig::CastStringAsTime},
    {"cast_int_datetime", tipb::ScalarFuncSig::CastIntAsTime},
    {"cast_real_datetime", tipb::ScalarFuncSig::CastRealAsTime},
    {"cast_decimal_datetime", tipb::ScalarFuncSig::CastDecimalAsTime},
    {"cast_time_datetime", tipb::ScalarFuncSig::CastTimeAsTime},
    {"cast_string_datetime", tipb::ScalarFuncSig::CastStringAsTime},
    {"concat", tipb::ScalarFuncSig::Concat},
    {"round_int", tipb::ScalarFuncSig::RoundInt},
    {"round_uint", tipb::ScalarFuncSig::RoundInt},
    {"round_dec", tipb::ScalarFuncSig::RoundDec},
    {"round_real", tipb::ScalarFuncSig::RoundReal},
    {"round_with_frac_int", tipb::ScalarFuncSig::RoundWithFracInt},
    {"round_with_frac_uint", tipb::ScalarFuncSig::RoundWithFracInt},
    {"round_with_frac_dec", tipb::ScalarFuncSig::RoundWithFracDec},
    {"round_with_frac_real", tipb::ScalarFuncSig::RoundWithFracReal},
});

std::unordered_map<String, tipb::ExprType> agg_func_name_to_sig({
    {"min", tipb::ExprType::Min},
    {"max", tipb::ExprType::Max},
    {"min_for_window", tipb::ExprType::Min},
    {"max_for_window", tipb::ExprType::Max},
    {"count", tipb::ExprType::Count},
    {"sum", tipb::ExprType::Sum},
    {"first_row", tipb::ExprType::First},
    {"uniqRawRes", tipb::ExprType::ApproxCountDistinct},
    {"group_concat", tipb::ExprType::GroupConcat},
    {"avg", tipb::ExprType::Avg},
});

std::unordered_map<String, tipb::ExprType> window_func_name_to_sig({
    {"RowNumber", tipb::ExprType::RowNumber},
    {"Rank", tipb::ExprType::Rank},
    {"DenseRank", tipb::ExprType::DenseRank},
    {"Lead", tipb::ExprType::Lead},
    {"Lag", tipb::ExprType::Lag},
    {"FirstValue", tipb::ExprType::FirstValue},
    {"LastValue", tipb::ExprType::LastValue},
});
} // namespace DB::tests
