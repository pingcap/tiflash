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

#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/FieldToDataType.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <TiDB/Decode/Datum.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>
#include <TiDB/Schema/TiDBTypes.h>

#include <unordered_map>

namespace DB
{
const Int8 VAR_SIZE = 0;

extern const String uniq_raw_res_name;

namespace
{
const std::unordered_map<tipb::ExprType, String> window_func_map({
    {tipb::ExprType::Rank, "rank"},
    {tipb::ExprType::DenseRank, "dense_rank"},
    {tipb::ExprType::RowNumber, "row_number"},
    {tipb::ExprType::Lead, "lead"},
    {tipb::ExprType::Lag, "lag"},
    {tipb::ExprType::FirstValue, "first_value"},
    {tipb::ExprType::LastValue, "last_value"},
});

const std::unordered_map<tipb::ExprType, String> agg_func_map({
    {tipb::ExprType::Count, "count"},
    {tipb::ExprType::Sum, "sum"},
    {tipb::ExprType::Min, "min"},
    {tipb::ExprType::Max, "max"},
    {tipb::ExprType::First, "first_row"},
    {tipb::ExprType::ApproxCountDistinct, uniq_raw_res_name},
    {tipb::ExprType::GroupConcat, "groupArray"},
    {tipb::ExprType::Avg, "avg"},
    //{tipb::ExprType::Agg_BitAnd, ""},
    //{tipb::ExprType::Agg_BitOr, ""},
    //{tipb::ExprType::Agg_BitXor, ""},
    //{tipb::ExprType::Std, ""},
    //{tipb::ExprType::Stddev, ""},
    //{tipb::ExprType::StddevPop, ""},
    //{tipb::ExprType::StddevSamp, ""},
    //{tipb::ExprType::VarPop, ""},
    //{tipb::ExprType::VarSamp, ""},
    //{tipb::ExprType::Variance, ""},
    //{tipb::ExprType::JsonArrayAgg, ""},
    //{tipb::ExprType::JsonObjectAgg, ""},
});

const std::unordered_map<tipb::ExprType, String> distinct_agg_func_map({
    {tipb::ExprType::Count, "countDistinct"},
    {tipb::ExprType::GroupConcat, "groupUniqArray"},
});

const std::unordered_map<tipb::ScalarFuncSig, String> scalar_func_map({
    {tipb::ScalarFuncSig::CastIntAsInt, "tidb_cast"},
    {tipb::ScalarFuncSig::CastIntAsReal, "tidb_cast"},
    {tipb::ScalarFuncSig::CastIntAsString, "tidb_cast"},
    {tipb::ScalarFuncSig::CastIntAsDecimal, "tidb_cast"},
    {tipb::ScalarFuncSig::CastIntAsTime, "tidb_cast"},
    //{tipb::ScalarFuncSig::CastIntAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastIntAsJson, "cast_int_as_json"},

    {tipb::ScalarFuncSig::CastRealAsInt, "tidb_cast"},
    {tipb::ScalarFuncSig::CastRealAsReal, "tidb_cast"},
    {tipb::ScalarFuncSig::CastRealAsString, "tidb_cast"},
    {tipb::ScalarFuncSig::CastRealAsDecimal, "tidb_cast"},
    {tipb::ScalarFuncSig::CastRealAsTime, "tidb_cast"},
    //{tipb::ScalarFuncSig::CastRealAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastRealAsJson, "cast_real_as_json"},

    {tipb::ScalarFuncSig::CastDecimalAsInt, "tidb_cast"},
    {tipb::ScalarFuncSig::CastDecimalAsReal, "tidb_cast"},
    {tipb::ScalarFuncSig::CastDecimalAsString, "tidb_cast"},
    {tipb::ScalarFuncSig::CastDecimalAsDecimal, "tidb_cast"},
    {tipb::ScalarFuncSig::CastDecimalAsTime, "tidb_cast"},
    //{tipb::ScalarFuncSig::CastDecimalAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastDecimalAsJson, "cast_decimal_as_json"},

    {tipb::ScalarFuncSig::CastStringAsInt, "tidb_cast"},
    {tipb::ScalarFuncSig::CastStringAsReal, "tidb_cast"},
    {tipb::ScalarFuncSig::CastStringAsString, "tidb_cast"},
    {tipb::ScalarFuncSig::CastStringAsDecimal, "tidb_cast"},
    {tipb::ScalarFuncSig::CastStringAsTime, "tidb_cast"},
    //{tipb::ScalarFuncSig::CastStringAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastStringAsJson, "cast_string_as_json"},

    {tipb::ScalarFuncSig::CastTimeAsInt, "tidb_cast"},
    {tipb::ScalarFuncSig::CastTimeAsReal, "tidb_cast"},
    {tipb::ScalarFuncSig::CastTimeAsString, "tidb_cast"},
    {tipb::ScalarFuncSig::CastTimeAsDecimal, "tidb_cast"},
    {tipb::ScalarFuncSig::CastTimeAsTime, "tidb_cast"},
    {tipb::ScalarFuncSig::CastTimeAsDuration, "tidb_cast"},
    {tipb::ScalarFuncSig::CastTimeAsJson, "cast_time_as_json"},

    //{tipb::ScalarFuncSig::CastDurationAsInt, "cast"},
    //{tipb::ScalarFuncSig::CastDurationAsReal, "cast"},
    //{tipb::ScalarFuncSig::CastDurationAsString, "cast"},
    //{tipb::ScalarFuncSig::CastDurationAsDecimal, "cast"},
    //{tipb::ScalarFuncSig::CastDurationAsTime, "cast"},
    {tipb::ScalarFuncSig::CastDurationAsDuration, "tidb_cast"},
    {tipb::ScalarFuncSig::CastDurationAsJson, "cast_duration_as_json"},

    //{tipb::ScalarFuncSig::CastJsonAsInt, "cast"},
    //{tipb::ScalarFuncSig::CastJsonAsReal, "cast"},
    {tipb::ScalarFuncSig::CastJsonAsString, "cast_json_as_string"},
    //{tipb::ScalarFuncSig::CastJsonAsDecimal, "cast"},
    //{tipb::ScalarFuncSig::CastJsonAsTime, "cast"},
    //{tipb::ScalarFuncSig::CastJsonAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastJsonAsJson, "cast_json_as_json"},

    {tipb::ScalarFuncSig::CastVectorFloat32AsString, "cast_vector_float32_as_string"},
    {tipb::ScalarFuncSig::CastVectorFloat32AsVectorFloat32, "cast_vector_float32_as_vector_float32"},

    {tipb::ScalarFuncSig::CoalesceInt, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceReal, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceString, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceDecimal, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceTime, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceDuration, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceJson, "coalesce"},

    {tipb::ScalarFuncSig::LTInt, "less"},
    {tipb::ScalarFuncSig::LTReal, "less"},
    {tipb::ScalarFuncSig::LTString, "less"},
    {tipb::ScalarFuncSig::LTDecimal, "less"},
    {tipb::ScalarFuncSig::LTTime, "less"},
    {tipb::ScalarFuncSig::LTDuration, "less"},
    //{tipb::ScalarFuncSig::LTJson, "less"},
    {tipb::ScalarFuncSig::LTVectorFloat32, "less"},

    {tipb::ScalarFuncSig::LEInt, "lessOrEquals"},
    {tipb::ScalarFuncSig::LEReal, "lessOrEquals"},
    {tipb::ScalarFuncSig::LEString, "lessOrEquals"},
    {tipb::ScalarFuncSig::LEDecimal, "lessOrEquals"},
    {tipb::ScalarFuncSig::LETime, "lessOrEquals"},
    {tipb::ScalarFuncSig::LEDuration, "lessOrEquals"},
    //{tipb::ScalarFuncSig::LEJson, "lessOrEquals"},
    {tipb::ScalarFuncSig::LEVectorFloat32, "lessOrEquals"},

    {tipb::ScalarFuncSig::GTInt, "greater"},
    {tipb::ScalarFuncSig::GTReal, "greater"},
    {tipb::ScalarFuncSig::GTString, "greater"},
    {tipb::ScalarFuncSig::GTDecimal, "greater"},
    {tipb::ScalarFuncSig::GTTime, "greater"},
    {tipb::ScalarFuncSig::GTDuration, "greater"},
    //{tipb::ScalarFuncSig::GTJson, "greater"},
    {tipb::ScalarFuncSig::GTVectorFloat32, "greater"},

    {tipb::ScalarFuncSig::GreatestInt, "tidbGreatest"},
    {tipb::ScalarFuncSig::GreatestReal, "tidbGreatest"},
    {tipb::ScalarFuncSig::GreatestString, "tidbGreatestString"},
    {tipb::ScalarFuncSig::GreatestDecimal, "greatest"},
    {tipb::ScalarFuncSig::GreatestTime, "greatest"},

    {tipb::ScalarFuncSig::LeastInt, "tidbLeast"},
    {tipb::ScalarFuncSig::LeastReal, "tidbLeast"},
    {tipb::ScalarFuncSig::LeastString, "tidbLeastString"},
    {tipb::ScalarFuncSig::LeastDecimal, "least"},
    {tipb::ScalarFuncSig::LeastTime, "least"},

    //{tipb::ScalarFuncSig::IntervalInt, "cast"},
    //{tipb::ScalarFuncSig::IntervalReal, "cast"},

    {tipb::ScalarFuncSig::GEInt, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GEReal, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GEString, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GEDecimal, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GETime, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GEDuration, "greaterOrEquals"},
    //{tipb::ScalarFuncSig::GEJson, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GEVectorFloat32, "greaterOrEquals"},

    {tipb::ScalarFuncSig::EQInt, "equals"},
    {tipb::ScalarFuncSig::EQReal, "equals"},
    {tipb::ScalarFuncSig::EQString, "equals"},
    {tipb::ScalarFuncSig::EQDecimal, "equals"},
    {tipb::ScalarFuncSig::EQTime, "equals"},
    {tipb::ScalarFuncSig::EQDuration, "equals"},
    //{tipb::ScalarFuncSig::EQJson, "equals"},
    {tipb::ScalarFuncSig::EQVectorFloat32, "equals"},

    {tipb::ScalarFuncSig::NEInt, "notEquals"},
    {tipb::ScalarFuncSig::NEReal, "notEquals"},
    {tipb::ScalarFuncSig::NEString, "notEquals"},
    {tipb::ScalarFuncSig::NEDecimal, "notEquals"},
    {tipb::ScalarFuncSig::NETime, "notEquals"},
    {tipb::ScalarFuncSig::NEDuration, "notEquals"},
    //{tipb::ScalarFuncSig::NEJson, "notEquals"},
    {tipb::ScalarFuncSig::NEVectorFloat32, "notEquals"},

    //{tipb::ScalarFuncSig::NullEQInt, "cast"},
    //{tipb::ScalarFuncSig::NullEQReal, "cast"},
    //{tipb::ScalarFuncSig::NullEQString, "cast"},
    //{tipb::ScalarFuncSig::NullEQDecimal, "cast"},
    //{tipb::ScalarFuncSig::NullEQTime, "cast"},
    //{tipb::ScalarFuncSig::NullEQDuration, "cast"},
    //{tipb::ScalarFuncSig::NullEQJson, "cast"},

    {tipb::ScalarFuncSig::PlusReal, "plus"},
    {tipb::ScalarFuncSig::PlusDecimal, "plus"},
    {tipb::ScalarFuncSig::PlusInt, "plus"},

    {tipb::ScalarFuncSig::MinusReal, "minus"},
    {tipb::ScalarFuncSig::MinusDecimal, "minus"},
    {tipb::ScalarFuncSig::MinusInt, "minus"},

    {tipb::ScalarFuncSig::MultiplyReal, "multiply"},
    {tipb::ScalarFuncSig::MultiplyDecimal, "multiply"},
    {tipb::ScalarFuncSig::MultiplyInt, "multiply"},

    {tipb::ScalarFuncSig::DivideReal, "tidbDivide"},
    {tipb::ScalarFuncSig::DivideDecimal, "tidbDivide"},
    //{tipb::ScalarFuncSig::IntDivideInt, "intDiv"},
    //{tipb::ScalarFuncSig::IntDivideDecimal, "divide"},

    {tipb::ScalarFuncSig::ModReal, "modulo"},
    {tipb::ScalarFuncSig::ModDecimal, "modulo"},
    {tipb::ScalarFuncSig::ModIntUnsignedUnsigned, "modulo"},
    {tipb::ScalarFuncSig::ModIntUnsignedSigned, "modulo"},
    {tipb::ScalarFuncSig::ModIntSignedUnsigned, "modulo"},
    {tipb::ScalarFuncSig::ModIntSignedSigned, "modulo"},

    {tipb::ScalarFuncSig::MultiplyIntUnsigned, "multiply"},
    {tipb::ScalarFuncSig::MinusIntUnsignedUnsigned, "minus"},
    {tipb::ScalarFuncSig::MinusIntUnsignedSigned, "minus"},
    {tipb::ScalarFuncSig::MinusIntSignedUnsigned, "minus"},
    {tipb::ScalarFuncSig::MinusIntSignedSigned, "minus"},
    {tipb::ScalarFuncSig::MinusIntForcedUnsignedUnsigned, "minus"},
    {tipb::ScalarFuncSig::MinusIntForcedUnsignedSigned, "minus"},
    {tipb::ScalarFuncSig::MinusIntForcedSignedUnsigned, "minus"},
    {tipb::ScalarFuncSig::IntDivideIntUnsignedUnsigned, "intDiv"},
    {tipb::ScalarFuncSig::IntDivideIntUnsignedSigned, "intDiv"},
    {tipb::ScalarFuncSig::IntDivideIntSignedUnsigned, "intDiv"},
    {tipb::ScalarFuncSig::IntDivideIntSignedSigned, "intDiv"},

    {tipb::ScalarFuncSig::AbsInt, "abs"},
    {tipb::ScalarFuncSig::AbsUInt, "abs"},
    {tipb::ScalarFuncSig::AbsReal, "abs"},
    {tipb::ScalarFuncSig::AbsDecimal, "abs"},

    {tipb::ScalarFuncSig::CeilIntToDec, "ceil"},
    {tipb::ScalarFuncSig::CeilIntToInt, "ceil"},
    {tipb::ScalarFuncSig::CeilDecToInt, "ceilDecimalToInt"},
    {tipb::ScalarFuncSig::CeilDecToDec, "ceil"},
    {tipb::ScalarFuncSig::CeilReal, "ceil"},

    {tipb::ScalarFuncSig::FloorIntToDec, "floor"},
    {tipb::ScalarFuncSig::FloorIntToInt, "floor"},
    {tipb::ScalarFuncSig::FloorDecToInt, "floorDecimalToInt"},
    {tipb::ScalarFuncSig::FloorDecToDec, "floor"},
    {tipb::ScalarFuncSig::FloorReal, "floor"},

    {tipb::ScalarFuncSig::RoundReal, "tidbRound"},
    {tipb::ScalarFuncSig::RoundInt, "tidbRound"},
    {tipb::ScalarFuncSig::RoundDec, "tidbRound"},
    {tipb::ScalarFuncSig::RoundWithFracReal, "tidbRoundWithFrac"},
    {tipb::ScalarFuncSig::RoundWithFracInt, "tidbRoundWithFrac"},
    {tipb::ScalarFuncSig::RoundWithFracDec, "tidbRoundWithFrac"},

    {tipb::ScalarFuncSig::Log1Arg, "log"},
    {tipb::ScalarFuncSig::Log2Args, "log2args"},
    {tipb::ScalarFuncSig::Log2, "log2"},
    {tipb::ScalarFuncSig::Log10, "log10"},

    {tipb::ScalarFuncSig::Rand, "rand"},
    //{tipb::ScalarFuncSig::RandWithSeedFirstGen, "cast"},

    {tipb::ScalarFuncSig::Pow, "pow"},
    {tipb::ScalarFuncSig::Conv, "conv"},
    {tipb::ScalarFuncSig::CRC32, "crc32"},
    {tipb::ScalarFuncSig::Sign, "sign"},

    {tipb::ScalarFuncSig::Sqrt, "sqrt"},
    {tipb::ScalarFuncSig::Acos, "acos"},
    {tipb::ScalarFuncSig::Asin, "asin"},
    {tipb::ScalarFuncSig::Atan1Arg, "atan"},
    //{tipb::ScalarFuncSig::Atan2Args, "cast"},
    {tipb::ScalarFuncSig::Cos, "cos"},
    //{tipb::ScalarFuncSig::Cot, "cast"},
    {tipb::ScalarFuncSig::Degrees, "degrees"},
    {tipb::ScalarFuncSig::Exp, "exp"},
    //{tipb::ScalarFuncSig::PI, "cast"},
    {tipb::ScalarFuncSig::Radians, "radians"},
    {tipb::ScalarFuncSig::Sin, "sin"},
    {tipb::ScalarFuncSig::Tan, "tan"},
    {tipb::ScalarFuncSig::TruncateInt, "trunc"},
    {tipb::ScalarFuncSig::TruncateReal, "trunc"},
    //{tipb::ScalarFuncSig::TruncateDecimal, "cast"},
    {tipb::ScalarFuncSig::TruncateUint, "trunc"},

    {tipb::ScalarFuncSig::LogicalAnd, "and"},
    {tipb::ScalarFuncSig::LogicalOr, "or"},
    {tipb::ScalarFuncSig::LogicalXor, "xor"},
    {tipb::ScalarFuncSig::UnaryNotDecimal, "not"},
    {tipb::ScalarFuncSig::UnaryNotInt, "not"},
    {tipb::ScalarFuncSig::UnaryNotReal, "not"},
    {tipb::ScalarFuncSig::UnaryMinusInt, "negate"},
    {tipb::ScalarFuncSig::UnaryMinusReal, "negate"},
    {tipb::ScalarFuncSig::UnaryMinusDecimal, "negate"},
    {tipb::ScalarFuncSig::DecimalIsNull, "isNull"},
    {tipb::ScalarFuncSig::DurationIsNull, "isNull"},
    {tipb::ScalarFuncSig::RealIsNull, "isNull"},
    {tipb::ScalarFuncSig::StringIsNull, "isNull"},
    {tipb::ScalarFuncSig::TimeIsNull, "isNull"},
    {tipb::ScalarFuncSig::IntIsNull, "isNull"},
    //{tipb::ScalarFuncSig::JsonIsNull, "isNull"},
    {tipb::ScalarFuncSig::VectorFloat32IsNull, "isNull"},

    {tipb::ScalarFuncSig::BitAndSig, "bitAnd"},
    {tipb::ScalarFuncSig::BitOrSig, "bitOr"},
    {tipb::ScalarFuncSig::BitXorSig, "bitXor"},
    {tipb::ScalarFuncSig::BitNegSig, "bitNot"},
    {tipb::ScalarFuncSig::IntIsTrue, "isTrue"},
    {tipb::ScalarFuncSig::IntIsTrueWithNull, "isTrueWithNull"},
    {tipb::ScalarFuncSig::RealIsTrue, "isTrue"},
    {tipb::ScalarFuncSig::RealIsTrueWithNull, "isTrueWithNull"},
    {tipb::ScalarFuncSig::DecimalIsTrue, "isTrue"},
    {tipb::ScalarFuncSig::DecimalIsTrueWithNull, "isTrueWithNull"},
    {tipb::ScalarFuncSig::IntIsFalse, "isFalse"},
    {tipb::ScalarFuncSig::IntIsFalseWithNull, "isFalseWithNull"},
    {tipb::ScalarFuncSig::RealIsFalse, "isFalse"},
    {tipb::ScalarFuncSig::RealIsFalseWithNull, "isFalseWithNull"},
    {tipb::ScalarFuncSig::DecimalIsFalse, "isFalse"},
    {tipb::ScalarFuncSig::DecimalIsFalseWithNull, "isFalseWithNull"},

    {tipb::ScalarFuncSig::LeftShift, "bitShiftLeft"},
    {tipb::ScalarFuncSig::RightShift, "bitShiftRight"},

    //{tipb::ScalarFuncSig::BitCount, "cast"},
    //{tipb::ScalarFuncSig::GetParamString, "cast"},
    //{tipb::ScalarFuncSig::GetVar, "cast"},
    //{tipb::ScalarFuncSig::RowSig, "cast"},
    //{tipb::ScalarFuncSig::SetVar, "cast"},
    //{tipb::ScalarFuncSig::ValuesDecimal, "cast"},
    //{tipb::ScalarFuncSig::ValuesDuration, "cast"},
    //{tipb::ScalarFuncSig::ValuesInt, "cast"},
    //{tipb::ScalarFuncSig::ValuesJSON, "cast"},
    //{tipb::ScalarFuncSig::ValuesReal, "cast"},
    //{tipb::ScalarFuncSig::ValuesString, "cast"},
    //{tipb::ScalarFuncSig::ValuesTime, "cast"},

    {tipb::ScalarFuncSig::InInt, "tidbIn"},
    {tipb::ScalarFuncSig::InReal, "tidbIn"},
    {tipb::ScalarFuncSig::InString, "tidbIn"},
    {tipb::ScalarFuncSig::InDecimal, "tidbIn"},
    {tipb::ScalarFuncSig::InTime, "tidbIn"},
    {tipb::ScalarFuncSig::InDuration, "tidbIn"},
    //{tipb::ScalarFuncSig::InJson, "tidbIn"},

    {tipb::ScalarFuncSig::IfNullInt, "ifNull"},
    {tipb::ScalarFuncSig::IfNullReal, "ifNull"},
    {tipb::ScalarFuncSig::IfNullString, "ifNull"},
    {tipb::ScalarFuncSig::IfNullDecimal, "ifNull"},
    {tipb::ScalarFuncSig::IfNullTime, "ifNull"},
    {tipb::ScalarFuncSig::IfNullDuration, "ifNull"},
    {tipb::ScalarFuncSig::IfNullJson, "ifNull"},

    /// Do not use If because ClickHouse's implementation is not compatible with TiDB
    /// ClickHouse: If(null, a, b) returns null
    /// TiDB: If(null, a, b) returns b
    {tipb::ScalarFuncSig::IfInt, "multiIf"},
    {tipb::ScalarFuncSig::IfReal, "multiIf"},
    {tipb::ScalarFuncSig::IfString, "multiIf"},
    {tipb::ScalarFuncSig::IfDecimal, "multiIf"},
    {tipb::ScalarFuncSig::IfTime, "multiIf"},
    {tipb::ScalarFuncSig::IfDuration, "multiIf"},
    {tipb::ScalarFuncSig::IfJson, "multiIf"},

    {tipb::ScalarFuncSig::CaseWhenInt, "multiIf"},
    {tipb::ScalarFuncSig::CaseWhenReal, "multiIf"},
    {tipb::ScalarFuncSig::CaseWhenString, "multiIf"},
    {tipb::ScalarFuncSig::CaseWhenDecimal, "multiIf"},
    {tipb::ScalarFuncSig::CaseWhenTime, "multiIf"},
    {tipb::ScalarFuncSig::CaseWhenDuration, "multiIf"},
    {tipb::ScalarFuncSig::CaseWhenJson, "multiIf"},

    //{tipb::ScalarFuncSig::AesDecrypt, "cast"},
    //{tipb::ScalarFuncSig::AesEncrypt, "cast"},
    //{tipb::ScalarFuncSig::Compress, "cast"},
    //{tipb::ScalarFuncSig::MD5, "cast"},
    //{tipb::ScalarFuncSig::Password, "cast"},
    //{tipb::ScalarFuncSig::RandomBytes, "cast"},
    //{tipb::ScalarFuncSig::SHA1, "cast"},
    //{tipb::ScalarFuncSig::SHA2, "cast"},
    //{tipb::ScalarFuncSig::Uncompress, "cast"},
    //{tipb::ScalarFuncSig::UncompressedLength, "cast"},
    //{tipb::ScalarFuncSig::AesDecryptIV, "cast"},
    //{tipb::ScalarFuncSig::AesEncryptIV, "cast"},
    //{tipb::ScalarFuncSig::Encode, "cast"},
    //{tipb::ScalarFuncSig::Decode, "cast"},

    //{tipb::ScalarFuncSig::Database, "cast"},
    //{tipb::ScalarFuncSig::FoundRows, "cast"},
    //{tipb::ScalarFuncSig::CurrentUser, "cast"},
    //{tipb::ScalarFuncSig::User, "cast"},
    //{tipb::ScalarFuncSig::ConnectionID, "cast"},
    //{tipb::ScalarFuncSig::LastInsertID, "cast"},
    //{tipb::ScalarFuncSig::LastInsertIDWithID, "cast"},
    //{tipb::ScalarFuncSig::Version, "cast"},
    //{tipb::ScalarFuncSig::TiDBVersion, "cast"},
    //{tipb::ScalarFuncSig::RowCount, "cast"},

    //{tipb::ScalarFuncSig::Sleep, "cast"},
    //{tipb::ScalarFuncSig::Lock, "cast"},
    //{tipb::ScalarFuncSig::ReleaseLock, "cast"},
    //{tipb::ScalarFuncSig::DecimalAnyValue, "cast"},
    //{tipb::ScalarFuncSig::DurationAnyValue, "cast"},
    //{tipb::ScalarFuncSig::IntAnyValue, "cast"},
    //{tipb::ScalarFuncSig::JSONAnyValue, "cast"},
    //{tipb::ScalarFuncSig::RealAnyValue, "cast"},
    //{tipb::ScalarFuncSig::StringAnyValue, "cast"},
    //{tipb::ScalarFuncSig::TimeAnyValue, "cast"},
    {tipb::ScalarFuncSig::InetAton, "tiDBIPv4StringToNum"},
    {tipb::ScalarFuncSig::InetNtoa, "IPv4NumToString"},
    {tipb::ScalarFuncSig::Inet6Aton, "tiDBIPv6StringToNum"},
    {tipb::ScalarFuncSig::Inet6Ntoa, "tiDBIPv6NumToString"},
    {tipb::ScalarFuncSig::IsIPv4, "tiDBIsIPv4"},
    //{tipb::ScalarFuncSig::IsIPv4Compat, "cast"},
    //{tipb::ScalarFuncSig::IsIPv4Mapped, "cast"},
    {tipb::ScalarFuncSig::IsIPv6, "tiDBIsIPv6"},
    //{tipb::ScalarFuncSig::UUID, "cast"},

    {tipb::ScalarFuncSig::IlikeSig, "ilike3Args"},
    {tipb::ScalarFuncSig::LikeSig, "like3Args"},
    {tipb::ScalarFuncSig::RegexpSig, "regexp"},
    {tipb::ScalarFuncSig::RegexpUTF8Sig, "regexp"},
    {tipb::ScalarFuncSig::RegexpLikeSig, "regexp_like"},
    {tipb::ScalarFuncSig::RegexpInStrSig, "regexp_instr"},
    {tipb::ScalarFuncSig::RegexpReplaceSig, "regexp_replace"},
    {tipb::ScalarFuncSig::RegexpSubstrSig, "regexp_substr"},

    {tipb::ScalarFuncSig::JsonExtractSig, "json_extract"},
    {tipb::ScalarFuncSig::JsonUnquoteSig, "json_unquote"},
    //{tipb::ScalarFuncSig::JsonTypeSig, "cast"},
    //{tipb::ScalarFuncSig::JsonSetSig, "cast"},
    //{tipb::ScalarFuncSig::JsonInsertSig, "cast"},
    //{tipb::ScalarFuncSig::JsonReplaceSig, "cast"},
    //{tipb::ScalarFuncSig::JsonRemoveSig, "cast"},
    //{tipb::ScalarFuncSig::JsonMergeSig, "cast"},
    //{tipb::ScalarFuncSig::JsonObjectSig, "cast"},
    {tipb::ScalarFuncSig::JsonArraySig, "json_array"},
    {tipb::ScalarFuncSig::JsonValidJsonSig, "json_valid_json"},
    {tipb::ScalarFuncSig::JsonValidOthersSig, "json_valid_others"},
    //{tipb::ScalarFuncSig::JsonContainsSig, "cast"},
    //{tipb::ScalarFuncSig::JsonArrayAppendSig, "cast"},
    //{tipb::ScalarFuncSig::JsonArrayInsertSig, "cast"},
    //{tipb::ScalarFuncSig::JsonMergePatchSig, "cast"},
    //{tipb::ScalarFuncSig::JsonMergePreserveSig, "cast"},
    {tipb::ScalarFuncSig::JsonContainsPathSig, "json_contains_path"},
    //{tipb::ScalarFuncSig::JsonPrettySig, "cast"},
    //{tipb::ScalarFuncSig::JsonQuoteSig, "cast"},
    //{tipb::ScalarFuncSig::JsonSearchSig, "cast"},
    //{tipb::ScalarFuncSig::JsonStorageSizeSig, "cast"},
    {tipb::ScalarFuncSig::JsonDepthSig, "json_depth"},
    {tipb::ScalarFuncSig::JsonKeysSig, "json_keys"},
    {tipb::ScalarFuncSig::JsonLengthSig, "jsonLength"},
    {tipb::ScalarFuncSig::JsonKeys2ArgsSig, "json_keys_2_args"},
    {tipb::ScalarFuncSig::JsonValidStringSig, "json_valid_string"},

    {tipb::ScalarFuncSig::DateFormatSig, "dateFormat"},
    //{tipb::ScalarFuncSig::DateLiteral, "cast"},
    {tipb::ScalarFuncSig::DateDiff, "tidbDateDiff"},
    //{tipb::ScalarFuncSig::NullTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::TimeStringTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::DurationDurationTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::DurationDurationTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::StringTimeTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::StringDurationTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::StringStringTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::TimeTimeTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::SubDateStringReal, "cast"},
    //{tipb::ScalarFuncSig::SubDateIntReal, "cast"},
    //{tipb::ScalarFuncSig::SubDateIntDecimal, "cast"},
    //{tipb::ScalarFuncSig::SubDateDatetimeReal, "cast"},
    //{tipb::ScalarFuncSig::SubDateDatetimeDecimal, "cast"},
    //{tipb::ScalarFuncSig::SubDateDurationString, "cast"},
    //{tipb::ScalarFuncSig::SubDateDurationInt, "cast"},
    //{tipb::ScalarFuncSig::SubDateDatetimeReal, "cast"},
    //{tipb::ScalarFuncSig::SubDateDatetimeDecimal, "cast"},
    //{tipb::ScalarFuncSig::AddDateIntReal, "cast"},
    //{tipb::ScalarFuncSig::AddDateIntDecimal, "cast"},
    //{tipb::ScalarFuncSig::AddDateDatetimeReal, "cast"},
    //{tipb::ScalarFuncSig::AddDateDatetimeDecimal, "cast"},
    //{tipb::ScalarFuncSig::AddDateDurationString, "cast"},
    //{tipb::ScalarFuncSig::AddDateDurationInt, "cast"},
    //{tipb::ScalarFuncSig::AddDateDurationInt, "cast"},
    //{tipb::ScalarFuncSig::AddDateDurationDecimal, "cast"},

    {tipb::ScalarFuncSig::Date, "toMyDate"},
    {tipb::ScalarFuncSig::Hour, "hour"},
    {tipb::ScalarFuncSig::Minute, "minute"},
    {tipb::ScalarFuncSig::Second, "second"},
    {tipb::ScalarFuncSig::MicroSecond, "microSecond"},
    {tipb::ScalarFuncSig::Month, "toMonth"},
    {tipb::ScalarFuncSig::MonthName, "toMonthName"},

    //{tipb::ScalarFuncSig::NowWithArg, "cast"},
    //{tipb::ScalarFuncSig::NowWithoutArg, "cast"},

    {tipb::ScalarFuncSig::DayName, "toDayName"},
    {tipb::ScalarFuncSig::DayOfMonth, "toDayOfMonth"},
    {tipb::ScalarFuncSig::DayOfWeek, "tidbDayOfWeek"},
    {tipb::ScalarFuncSig::DayOfYear, "tidbDayOfYear"},

    //{tipb::ScalarFuncSig::WeekWithMode, "cast"},
    //{tipb::ScalarFuncSig::WeekWithoutMode, "cast"},
    //{tipb::ScalarFuncSig::WeekDay, "cast"},
    {tipb::ScalarFuncSig::WeekOfYear, "tidbWeekOfYear"},

    {tipb::ScalarFuncSig::Year, "toYear"},
    //{tipb::ScalarFuncSig::YearWeekWithMode, "cast"},
    //{tipb::ScalarFuncSig::YearWeekWithoutMode, "cast"},

    {tipb::ScalarFuncSig::GetFormat, "getFormat"},
    {tipb::ScalarFuncSig::SysDateWithFsp, "sysDateWithFsp"},
    {tipb::ScalarFuncSig::SysDateWithoutFsp, "sysDateWithoutFsp"},
    //{tipb::ScalarFuncSig::CurrentDate, "cast"},
    //{tipb::ScalarFuncSig::CurrentTime0Arg, "cast"},
    //{tipb::ScalarFuncSig::CurrentTime1Arg, "cast"},

    //{tipb::ScalarFuncSig::Time, "cast"},
    //{tipb::ScalarFuncSig::TimeLiteral, "cast"},
    //{tipb::ScalarFuncSig::UTCDate, "cast"},
    //{tipb::ScalarFuncSig::UTCTimestampWithArg, "cast"},
    //{tipb::ScalarFuncSig::UTCTimestampWithoutArg, "cast"},

    //{tipb::ScalarFuncSig::AddDatetimeAndDuration, "cast"},
    //{tipb::ScalarFuncSig::AddDatetimeAndString, "cast"},
    //{tipb::ScalarFuncSig::AddTimeDateTimeNull, "cast"},
    //{tipb::ScalarFuncSig::AddStringAndDuration, "cast"},
    //{tipb::ScalarFuncSig::AddStringAndString, "cast"},
    //{tipb::ScalarFuncSig::AddTimeStringNull, "cast"},
    //{tipb::ScalarFuncSig::AddDurationAndDuration, "cast"},
    //{tipb::ScalarFuncSig::AddDurationAndString, "cast"},
    //{tipb::ScalarFuncSig::AddTimeDurationNull, "cast"},
    //{tipb::ScalarFuncSig::AddDateAndDuration, "cast"},
    //{tipb::ScalarFuncSig::AddDateAndString, "cast"},

    //{tipb::ScalarFuncSig::SubDateAndDuration, "cast"},
    //{tipb::ScalarFuncSig::SubDateAndString, "cast"},
    //{tipb::ScalarFuncSig::SubTimeDateTimeNull, "cast"},
    //{tipb::ScalarFuncSig::SubStringAndDuration, "cast"},
    //{tipb::ScalarFuncSig::SubStringAndString, "cast"},
    //{tipb::ScalarFuncSig::SubTimeStringNull, "cast"},
    //{tipb::ScalarFuncSig::SubDurationAndDuration, "cast"},
    //{tipb::ScalarFuncSig::SubDurationAndString, "cast"},
    //{tipb::ScalarFuncSig::SubDateAndDuration, "cast"},
    //{tipb::ScalarFuncSig::SubDateAndString, "cast"},

    //{tipb::ScalarFuncSig::UnixTimestampCurrent, "cast"},
    {tipb::ScalarFuncSig::UnixTimestampInt, "tidbUnixTimeStampInt"},
    {tipb::ScalarFuncSig::UnixTimestampDec, "tidbUnixTimeStampDec"},

    //{tipb::ScalarFuncSig::ConvertTz, "cast"},
    //{tipb::ScalarFuncSig::MakeDate, "cast"},
    //{tipb::ScalarFuncSig::MakeTime, "cast"},
    //{tipb::ScalarFuncSig::PeriodAdd, "cast"},
    //{tipb::ScalarFuncSig::PeriodDiff, "cast"},
    {tipb::ScalarFuncSig::Quarter, "toQuarter"},

    //{tipb::ScalarFuncSig::SecToTime, "cast"},
    {tipb::ScalarFuncSig::TimeToSec, "tidbTimeToSec"},
    //{tipb::ScalarFuncSig::TimestampAdd, "cast"},
    {tipb::ScalarFuncSig::ToDays, "tidbToDays"},
    {tipb::ScalarFuncSig::ToSeconds, "tidbToSeconds"},
    //{tipb::ScalarFuncSig::UTCTimeWithArg, "cast"},
    //{tipb::ScalarFuncSig::UTCTimestampWithoutArg, "cast"},
    //{tipb::ScalarFuncSig::Timestamp1Arg, "cast"},
    //{tipb::ScalarFuncSig::Timestamp2Args, "cast"},
    //{tipb::ScalarFuncSig::TimestampLiteral, "cast"},

    {tipb::ScalarFuncSig::LastDay, "tidbLastDay"},
    {tipb::ScalarFuncSig::StrToDateDate, "strToDateDate"},
    {tipb::ScalarFuncSig::StrToDateDatetime, "strToDateDatetime"},
    // {tipb::ScalarFuncSig::StrToDateDuration, "cast"},
    {tipb::ScalarFuncSig::FromUnixTime1Arg, "fromUnixTime"},
    {tipb::ScalarFuncSig::FromUnixTime2Arg, "fromUnixTime"},
    {tipb::ScalarFuncSig::ExtractDatetime, "extractMyDateTime"},
    {tipb::ScalarFuncSig::ExtractDatetimeFromString, "extractMyDateTimeFromString"},
    {tipb::ScalarFuncSig::ExtractDuration, "extractMyDuration"},

    //{tipb::ScalarFuncSig::AddDateStringString, "cast"},
    {tipb::ScalarFuncSig::AddDateStringInt, "date_add"},
    {tipb::ScalarFuncSig::AddDateStringReal, "date_add"},
    //{tipb::ScalarFuncSig::AddDateStringDecimal, "cast"},
    //{tipb::ScalarFuncSig::AddDateIntString, "cast"},
    //{tipb::ScalarFuncSig::AddDateIntInt, "cast"},
    //{tipb::ScalarFuncSig::AddDateDatetimeString, "date_add"},
    {tipb::ScalarFuncSig::AddDateDatetimeInt, "date_add"},

    //{tipb::ScalarFuncSig::SubDateStringString, "cast"},
    {tipb::ScalarFuncSig::SubDateStringInt, "date_sub"},
    {tipb::ScalarFuncSig::SubDateStringReal, "date_sub"},
    //{tipb::ScalarFuncSig::SubDateStringDecimal, "cast"},
    //{tipb::ScalarFuncSig::SubDateIntString, "cast"},
    //{tipb::ScalarFuncSig::SubDateIntInt, "cast"},
    //{tipb::ScalarFuncSig::SubDateDatetimeString, "cast"},
    {tipb::ScalarFuncSig::SubDateDatetimeInt, "date_sub"},

    {tipb::ScalarFuncSig::FromDays, "tidbFromDays"},
    //{tipb::ScalarFuncSig::TimeFormat, "cast"},
    {tipb::ScalarFuncSig::TimestampDiff, "tidbTimestampDiff"},

    //{tipb::ScalarFuncSig::BitLength, "cast"},
    {tipb::ScalarFuncSig::Bin, "bin"},
    {tipb::ScalarFuncSig::ASCII, "ascii"},
    //{tipb::ScalarFuncSig::Char, "cast"},
    {tipb::ScalarFuncSig::CharLengthUTF8, "lengthUTF8"},
    {tipb::ScalarFuncSig::Concat, "tidbConcat"},
    {tipb::ScalarFuncSig::ConcatWS, "tidbConcatWS"},
    //{tipb::ScalarFuncSig::Convert, "cast"},
    {tipb::ScalarFuncSig::Elt, "elt"},
    //{tipb::ScalarFuncSig::ExportSet3Arg, "cast"},
    //{tipb::ScalarFuncSig::ExportSet4Arg, "cast"},
    //{tipb::ScalarFuncSig::ExportSet5Arg, "cast"},
    //{tipb::ScalarFuncSig::FieldInt, "cast"},
    //{tipb::ScalarFuncSig::FieldReal, "cast"},
    //{tipb::ScalarFuncSig::FieldString, "cast"},

    //{tipb::ScalarFuncSig::FindInSet, "cast"},
    //{tipb::ScalarFuncSig::Format, "cast"},
    //{tipb::ScalarFuncSig::FormatWithLocale, "cast"},
    //{tipb::ScalarFuncSig::FromBase64, "cast"},
    {tipb::ScalarFuncSig::HexIntArg, "hexInt"},
    {tipb::ScalarFuncSig::HexStrArg, "hexStr"},
    //{tipb::ScalarFuncSig::InsertUTF8, "cast"},
    //{tipb::ScalarFuncSig::Insert, "cast"},
    //{tipb::ScalarFuncSig::InstrUTF8, "cast"},
    //{tipb::ScalarFuncSig::Instr, "cast"},

    {tipb::ScalarFuncSig::LeftUTF8, "leftUTF8"},
    //{tipb::ScalarFuncSig::Left, "cast"},
    {tipb::ScalarFuncSig::Length, "length"},
    {tipb::ScalarFuncSig::Locate2ArgsUTF8, "positionUTF8"},
    //{tipb::ScalarFuncSig::Locate3ArgsUTF8, "cast"},
    {tipb::ScalarFuncSig::Locate2Args, "position"},
    //{tipb::ScalarFuncSig::Locate3Args, "cast"},

    {tipb::ScalarFuncSig::Lower, "lowerBinary"},
    {tipb::ScalarFuncSig::LowerUTF8, "lowerUTF8"},
    {tipb::ScalarFuncSig::LpadUTF8, "lpadUTF8"},
    {tipb::ScalarFuncSig::Lpad, "lpad"},
    //{tipb::ScalarFuncSig::MakeSet, "cast"},
    //{tipb::ScalarFuncSig::OctInt, "cast"},
    //{tipb::ScalarFuncSig::OctString, "cast"},
    //{tipb::ScalarFuncSig::Ord, "cast"},
    //{tipb::ScalarFuncSig::Quote, "cast"},
    {tipb::ScalarFuncSig::Repeat, "repeat"},
    {tipb::ScalarFuncSig::Replace, "replaceAll"},
    {tipb::ScalarFuncSig::ReverseUTF8, "reverseUTF8"},
    {tipb::ScalarFuncSig::Reverse, "reverse"},
    {tipb::ScalarFuncSig::RightUTF8, "rightUTF8"},
    //{tipb::ScalarFuncSig::Right, "cast"},
    {tipb::ScalarFuncSig::RpadUTF8, "rpadUTF8"},
    {tipb::ScalarFuncSig::Rpad, "rpad"},
    {tipb::ScalarFuncSig::Space, "space"},
    {tipb::ScalarFuncSig::Strcmp, "strcmp"},
    {tipb::ScalarFuncSig::Substring2ArgsUTF8, "substringUTF8"},
    {tipb::ScalarFuncSig::Substring3ArgsUTF8, "substringUTF8"},
    //{tipb::ScalarFuncSig::Substring2Args, "cast"},
    //{tipb::ScalarFuncSig::Substring3Args, "cast"},
    {tipb::ScalarFuncSig::SubstringIndex, "substringIndex"},
    {tipb::ScalarFuncSig::Format, "format"},
    {tipb::ScalarFuncSig::FormatWithLocale, "formatWithLocale"},

    //{tipb::ScalarFuncSig::ToBase64, "cast"},
    {tipb::ScalarFuncSig::Trim1Arg, "tidbTrim"},
    {tipb::ScalarFuncSig::Trim2Args, "tidbTrim"},
    {tipb::ScalarFuncSig::Trim3Args, "tidbTrim"},
    {tipb::ScalarFuncSig::LTrim, "tidbLTrim"},
    {tipb::ScalarFuncSig::RTrim, "tidbRTrim"},
    {tipb::ScalarFuncSig::UnHex, "tidbUnHex"},
    {tipb::ScalarFuncSig::UpperUTF8, "upperUTF8"},
    {tipb::ScalarFuncSig::Upper, "upperBinary"},
    //{tipb::ScalarFuncSig::CharLength, "upper"},

    {tipb::ScalarFuncSig::GroupingSig, "grouping"},

    {tipb::ScalarFuncSig::VecAsTextSig, "vecAsText"},
    {tipb::ScalarFuncSig::VecDimsSig, "vecDims"},
    {tipb::ScalarFuncSig::VecL1DistanceSig, "vecL1Distance"},
    {tipb::ScalarFuncSig::VecL2DistanceSig, "vecL2Distance"},
    {tipb::ScalarFuncSig::VecNegativeInnerProductSig, "vecNegativeInnerProduct"},
    {tipb::ScalarFuncSig::VecCosineDistanceSig, "vecCosineDistance"},
    {tipb::ScalarFuncSig::VecL2NormSig, "vecL2Norm"},
});

template <typename GetColumnsFunc, typename GetDataTypeFunc>
void assertBlockSchema(
    GetColumnsFunc && get_columns,
    GetDataTypeFunc && get_datatype,
    const Block & block,
    const String & context_description)
{
    size_t columns = get_columns();
    if (block.columns() != columns)
        throw Exception(fmt::format(
            "Block schema mismatch in {}: different number of columns: expected {} columns, got {} columns",
            context_description,
            columns,
            block.columns()));

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & actual = block.getByPosition(i).type;
        const auto & expected = get_datatype(i);

        if (!expected->equals(*actual))
            throw Exception(fmt::format(
                "Block schema mismatch in {}: different types: expected {}, got {}; col_index: {}; col_name: {}",
                context_description,
                expected->getName(),
                actual->getName(),
                i,
                block.getByPosition(i).name));
    }
}
/// used by test
std::unordered_map<String, tipb::ScalarFuncSig> getFuncNameToSigMap()
{
    std::unordered_map<String, tipb::ScalarFuncSig> ret;
    for (const auto & element : scalar_func_map)
    {
        ret[element.second] = element.first;
    }
    return ret;
}
} // namespace

bool isScalarFunctionExpr(const tipb::Expr & expr)
{
    return expr.tp() == tipb::ExprType::ScalarFunc;
}

bool isFunctionExpr(const tipb::Expr & expr)
{
    return isScalarFunctionExpr(expr) || isAggFunctionExpr(expr) || isWindowFunctionExpr(expr);
}

const String & getAggFunctionName(const tipb::Expr & expr)
{
    if (expr.has_distinct())
    {
        auto it = distinct_agg_func_map.find(expr.tp());
        if (it != distinct_agg_func_map.end())
            return it->second;
    }
    else
    {
        auto it = agg_func_map.find(expr.tp());
        if (it != agg_func_map.end())
            return it->second;
    }

    throw TiFlashException(
        Errors::Coprocessor::Unimplemented,
        "{}(distinct={}) is not supported.",
        tipb::ExprType_Name(expr.tp()),
        expr.has_distinct());
}

const String & getWindowFunctionName(const tipb::Expr & expr)
{
    auto it = window_func_map.find(expr.tp());
    if (it != window_func_map.end())
        return it->second;

    throw TiFlashException(Errors::Coprocessor::Unimplemented, "{} is not supported.", tipb::ExprType_Name(expr.tp()));
}


const String & getFunctionName(const tipb::Expr & expr)
{
    if (isAggFunctionExpr(expr))
    {
        return getAggFunctionName(expr);
    }
    else if (isWindowFunctionExpr(expr))
    {
        return getWindowFunctionName(expr);
    }
    else
    {
        auto it = scalar_func_map.find(expr.sig());
        if (it == scalar_func_map.end())
            throw TiFlashException(
                Errors::Coprocessor::Unimplemented,
                "{} is not supported.",
                tipb::ScalarFuncSig_Name(expr.sig()));
        return it->second;
    }
}

String getExchangeTypeName(const tipb::ExchangeType & tp)
{
    switch (tp)
    {
    case tipb::ExchangeType::Broadcast:
        return "Broadcast";
    case tipb::ExchangeType::PassThrough:
        return "PassThrough";
    case tipb::ExchangeType::Hash:
        return "Hash";
    default:
        throw TiFlashException(Errors::Coprocessor::Internal, "Not supported Exchange type: {}", fmt::underlying(tp));
    }
}

String getJoinTypeName(const tipb::JoinType & tp)
{
    switch (tp)
    {
    case tipb::JoinType::TypeAntiLeftOuterSemiJoin:
        return "AntiLeftOuterSemiJoin";
    case tipb::JoinType::TypeLeftOuterJoin:
        return "LeftOuterJoin";
    case tipb::JoinType::TypeRightOuterJoin:
        return "RightOuterJoin";
    case tipb::JoinType::TypeLeftOuterSemiJoin:
        return "LeftOuterSemiJoin";
    case tipb::JoinType::TypeAntiSemiJoin:
        return "AntiSemiJoin";
    case tipb::JoinType::TypeInnerJoin:
        return "InnerJoin";
    case tipb::JoinType::TypeSemiJoin:
        return "SemiJoin";
    default:
        throw TiFlashException(Errors::Coprocessor::Internal, "Not supported Join type: {}", fmt::underlying(tp));
    }
}

String getJoinExecTypeName(const tipb::JoinExecType & tp)
{
    switch (tp)
    {
    case tipb::JoinExecType::TypeHashJoin:
        return "HashJoin";
    default:
        throw TiFlashException(
            Errors::Coprocessor::Internal,
            "Not supported Join exectution type: {}",
            fmt::underlying(tp));
    }
}

String getFieldTypeName(Int32 tp)
{
    switch (tp)
    {
    case TiDB::TypeTiny:
        return "Tiny";
    case TiDB::TypeShort:
        return "Short";
    case TiDB::TypeInt24:
        return "Int24";
    case TiDB::TypeLong:
        return "Long";
    case TiDB::TypeLongLong:
        return "Longlong";
    case TiDB::TypeYear:
        return "Year";
    case TiDB::TypeDouble:
        return "Double";
    case TiDB::TypeTime:
        return "Time";
    case TiDB::TypeDate:
        return "Date";
    case TiDB::TypeDatetime:
        return "Datetime";
    case TiDB::TypeNewDate:
        return "NewDate";
    case TiDB::TypeTimestamp:
        return "Timestamp";
    case TiDB::TypeFloat:
        return "Float";
    case TiDB::TypeDecimal:
        return "Decimal";
    case TiDB::TypeNewDecimal:
        return "NewDecimal";
    case TiDB::TypeVarchar:
        return "Varchar";
    case TiDB::TypeString:
        return "String";
    default:
        throw TiFlashException(Errors::Coprocessor::Internal, "Not supported field type: {}", tp);
    }
}

String exprToString(const tipb::Expr & expr, const std::vector<NameAndTypePair> & input_col)
{
    FmtBuffer fmt_buf;
    String func_name;
    Field f;
    switch (expr.tp())
    {
    case tipb::ExprType::Null:
        return "NULL";
    case tipb::ExprType::Int64:
        return std::to_string(decodeDAGInt64(expr.val()));
    case tipb::ExprType::Uint64:
        return std::to_string(decodeDAGUInt64(expr.val()));
    case tipb::ExprType::Float32:
        return std::to_string(decodeDAGFloat32(expr.val()));
    case tipb::ExprType::Float64:
        return std::to_string(decodeDAGFloat64(expr.val()));
    case tipb::ExprType::String:
        return decodeDAGString(expr.val());
    case tipb::ExprType::Bytes:
        return decodeDAGBytes(expr.val());
    case tipb::ExprType::MysqlDecimal:
    {
        auto field = decodeDAGDecimal(expr.val());
        if (field.getType() == Field::Types::Decimal32)
            return field.get<DecimalField<Decimal32>>().toString();
        else if (field.getType() == Field::Types::Decimal64)
            return field.get<DecimalField<Decimal64>>().toString();
        else if (field.getType() == Field::Types::Decimal128)
            return field.get<DecimalField<Decimal128>>().toString();
        else if (field.getType() == Field::Types::Decimal256)
            return field.get<DecimalField<Decimal256>>().toString();
        else
            throw TiFlashException("Not decimal literal" + expr.DebugString(), Errors::Coprocessor::BadRequest);
    }
    case tipb::ExprType::MysqlTime:
    {
        if (!expr.has_field_type())
            throw TiFlashException(
                "MySQL Time literal without field_type" + expr.DebugString(),
                Errors::Coprocessor::BadRequest);
        auto t = decodeDAGUInt64(expr.val());
        auto ret
            = std::to_string(TiDB::DatumFlat(t, static_cast<TiDB::TP>(expr.field_type().tp())).field().get<UInt64>());
        if (expr.field_type().tp() == TiDB::TypeTimestamp)
            ret = ret + "_ts";
        return ret;
    }
    case tipb::ExprType::MysqlDuration:
    {
        if (!expr.has_field_type())
            throw TiFlashException(
                "MySQL Duration literal without field_type" + expr.DebugString(),
                Errors::Coprocessor::BadRequest);
        auto t = decodeDAGInt64(expr.val());
        auto ret
            = std::to_string(TiDB::DatumFlat(t, static_cast<TiDB::TP>(expr.field_type().tp())).field().get<Int64>());
        return ret;
    }
    case tipb::ExprType::TiDBVectorFloat32:
    {
        if (!expr.has_field_type())
            throw TiFlashException(
                "MySQL Duration literal without field_type" + expr.DebugString(),
                Errors::Coprocessor::BadRequest);
        auto t = decodeDAGVectorFloat32(expr.val());
        auto arr = t.safeGet<Array>();
        String ret = "[";
        for (size_t i = 0; i < arr.size(); ++i)
        {
            if (i > 0)
                ret += ",";
            ret += std::to_string(arr[i].safeGet<NearestFieldType<Float32>::Type>());
        }
        ret += "]";
        return ret;
    }
    case tipb::ExprType::ColumnRef:
        return getColumnNameForColumnExpr(expr, input_col);
    case tipb::ExprType::Count:
    case tipb::ExprType::Sum:
    case tipb::ExprType::Avg:
    case tipb::ExprType::Min:
    case tipb::ExprType::Max:
    case tipb::ExprType::First:
    case tipb::ExprType::ApproxCountDistinct:
    case tipb::ExprType::GroupConcat:
        func_name = getAggFunctionName(expr);
        break;
    case tipb::ExprType::ScalarFunc:
        if (scalar_func_map.find(expr.sig()) == scalar_func_map.end())
        {
            throw TiFlashException(
                tipb::ScalarFuncSig_Name(expr.sig()) + " not supported",
                Errors::Coprocessor::Unimplemented);
        }
        func_name = scalar_func_map.find(expr.sig())->second;
        break;
    default:
        throw TiFlashException(tipb::ExprType_Name(expr.tp()) + " not supported", Errors::Coprocessor::Unimplemented);
    }
    // build function expr
    if (functionIsInOrGlobalInOperator(func_name))
    {
        // for in, we could not represent the function expr using func_name(param1, param2, ...)
        fmt_buf.fmtAppend("{} {} (", exprToString(expr.children(0), input_col), func_name);
        fmt_buf.joinStr(
            expr.children().begin() + 1,
            expr.children().end(),
            [input_col](const auto & arg, FmtBuffer & fb) { fb.append(exprToString(arg, input_col)); },
            ", ");
        fmt_buf.append(")");
    }
    else
    {
        fmt_buf.fmtAppend("{}(", func_name);
        fmt_buf.joinStr(
            expr.children().begin(),
            expr.children().end(),
            [input_col](const auto & arg, FmtBuffer & fb) { fb.append(exprToString(arg, input_col)); },
            ", ");
        fmt_buf.append(")");
    }
    return fmt_buf.toString();
}

const String & getTypeName(const tipb::Expr & expr)
{
    return tipb::ExprType_Name(expr.tp());
}

bool isAggFunctionExpr(const tipb::Expr & expr)
{
    switch (expr.tp())
    {
    case tipb::ExprType::Count:
    case tipb::ExprType::Sum:
    case tipb::ExprType::Avg:
    case tipb::ExprType::Min:
    case tipb::ExprType::Max:
    case tipb::ExprType::First:
    case tipb::ExprType::GroupConcat:
    case tipb::ExprType::Agg_BitAnd:
    case tipb::ExprType::Agg_BitOr:
    case tipb::ExprType::Agg_BitXor:
    case tipb::ExprType::Std:
    case tipb::ExprType::Stddev:
    case tipb::ExprType::StddevPop:
    case tipb::ExprType::StddevSamp:
    case tipb::ExprType::VarPop:
    case tipb::ExprType::VarSamp:
    case tipb::ExprType::Variance:
    case tipb::ExprType::JsonArrayAgg:
    case tipb::ExprType::JsonObjectAgg:
    case tipb::ExprType::ApproxCountDistinct:
        return true;
    default:
        return false;
    }
}

bool isWindowFunctionExpr(const tipb::Expr & expr)
{
    switch (expr.tp())
    {
    case tipb::ExprType::RowNumber:
    case tipb::ExprType::Rank:
    case tipb::ExprType::DenseRank:
    case tipb::ExprType::Lead:
    case tipb::ExprType::Lag:
    case tipb::ExprType::FirstValue:
    case tipb::ExprType::LastValue:
        //    case tipb::ExprType::CumeDist:
        //    case tipb::ExprType::PercentRank:
        //    case tipb::ExprType::Ntile:
        //    case tipb::ExprType::NthValue:
        return true;
    default:
        return false;
    }
}

bool isWindowLagOrLeadFunctionExpr(const tipb::Expr & expr)
{
    switch (expr.tp())
    {
    case tipb::ExprType::Lead:
    case tipb::ExprType::Lag:
        return true;
    default:
        return false;
    }
}

bool isLiteralExpr(const tipb::Expr & expr)
{
    switch (expr.tp())
    {
    case tipb::ExprType::Null:
    case tipb::ExprType::Int64:
    case tipb::ExprType::Uint64:
    case tipb::ExprType::Float32:
    case tipb::ExprType::Float64:
    case tipb::ExprType::String:
    case tipb::ExprType::Bytes:
    case tipb::ExprType::MysqlBit:
    case tipb::ExprType::MysqlDecimal:
    case tipb::ExprType::MysqlDuration:
    case tipb::ExprType::MysqlEnum:
    case tipb::ExprType::MysqlHex:
    case tipb::ExprType::MysqlSet:
    case tipb::ExprType::MysqlTime:
    case tipb::ExprType::MysqlJson:
    case tipb::ExprType::ValueList:
    case tipb::ExprType::TiDBVectorFloat32:
        return true;
    default:
        return false;
    }
}

bool isColumnExpr(const tipb::Expr & expr)
{
    return expr.tp() == tipb::ExprType::ColumnRef;
}

Field decodeLiteral(const tipb::Expr & expr)
{
    switch (expr.tp())
    {
    case tipb::ExprType::Null:
        return Field();
    case tipb::ExprType::Int64:
        return decodeDAGInt64(expr.val());
    case tipb::ExprType::Uint64:
        return decodeDAGUInt64(expr.val());
    case tipb::ExprType::Float32:
        return static_cast<Float64>(decodeDAGFloat32(expr.val()));
    case tipb::ExprType::Float64:
        return decodeDAGFloat64(expr.val());
    case tipb::ExprType::String:
        return decodeDAGString(expr.val());
    case tipb::ExprType::Bytes:
        return decodeDAGBytes(expr.val());
    case tipb::ExprType::MysqlDecimal:
        return decodeDAGDecimal(expr.val());
    case tipb::ExprType::MysqlTime:
    {
        if (!expr.has_field_type())
            throw TiFlashException(
                "MySQL Time literal without field_type" + expr.DebugString(),
                Errors::Coprocessor::BadRequest);
        auto t = decodeDAGUInt64(expr.val());
        return TiDB::DatumFlat(t, static_cast<TiDB::TP>(expr.field_type().tp())).field();
    }
    case tipb::ExprType::MysqlDuration:
    {
        if (!expr.has_field_type())
            throw TiFlashException(
                "MySQL Duration literal without field_type" + expr.DebugString(),
                Errors::Coprocessor::BadRequest);
        auto t = decodeDAGInt64(expr.val());
        return TiDB::DatumFlat(t, static_cast<TiDB::TP>(expr.field_type().tp())).field();
    }
    case tipb::ExprType::TiDBVectorFloat32:
    {
        if (!expr.has_field_type())
            throw TiFlashException(
                "MySQL Duration literal without field_type" + expr.DebugString(),
                Errors::Coprocessor::BadRequest);
        return decodeDAGVectorFloat32(expr.val());
    }
    case tipb::ExprType::MysqlBit:
    case tipb::ExprType::MysqlEnum:
    case tipb::ExprType::MysqlHex:
    case tipb::ExprType::MysqlSet:
    case tipb::ExprType::MysqlJson:
    case tipb::ExprType::ValueList:
        throw TiFlashException(
            tipb::ExprType_Name(expr.tp()) + " is not supported yet",
            Errors::Coprocessor::Unimplemented);
    default:
        throw TiFlashException("Should not reach here: not a literal expression", Errors::Coprocessor::Internal);
    }
}

String getColumnNameForColumnExpr(const tipb::Expr & expr, const std::vector<NameAndTypePair> & input_col)
{
    auto column_index = decodeDAGInt64(expr.val());
    if (column_index < 0 || column_index >= static_cast<Int64>(input_col.size()))
    {
        throw TiFlashException(
            Errors::Coprocessor::BadRequest,
            "Column index out of bound, expr: {}, size of input columns: {}",
            expr.DebugString(),
            input_col.size());
    }
    return input_col[column_index].name;
}

ColumnID getColumnIDForColumnExpr(const tipb::Expr & expr, const std::vector<TiDB::ColumnInfo> & input_col)
{
    auto column_index = decodeDAGInt64(expr.val());
    if (column_index < 0 || column_index >= static_cast<Int64>(input_col.size()))
    {
        throw TiFlashException(
            Errors::Coprocessor::BadRequest,
            "Column index out of bound, expr: {}, size of input columns: {}",
            expr.DebugString(),
            input_col.size());
    }
    return input_col[column_index].id;
}

void getColumnIDsFromExpr(
    const tipb::Expr & expr,
    const std::vector<TiDB::ColumnInfo> & input_col,
    std::unordered_set<ColumnID> & col_id_set)
{
    if (expr.children_size() == 0)
    {
        if (isColumnExpr(expr))
        {
            col_id_set.insert(getColumnIDForColumnExpr(expr, input_col));
        }
    }
    else
    {
        for (const auto & child : expr.children())
        {
            getColumnIDsFromExpr(child, input_col, col_id_set);
        }
    }
}

NameAndTypePair getColumnNameAndTypeForColumnExpr(
    const tipb::Expr & expr,
    const std::vector<NameAndTypePair> & input_col)
{
    auto column_index = decodeDAGInt64(expr.val());
    if (column_index < 0 || column_index >= static_cast<Int64>(input_col.size()))
    {
        throw TiFlashException("Column index out of bound", Errors::Coprocessor::BadRequest);
    }
    return input_col[column_index];
}

// For some historical or unknown reasons, TiDB might set an invalid
// field type. This function checks if the expr has a valid field type.
// So far the known invalid field types are:
// 1. decimal type with scale == -1
// 2. decimal type with precision == 0
// 3. decimal type with precision == -1
bool exprHasValidFieldType(const tipb::Expr & expr)
{
    return expr.has_field_type()
        && (expr.field_type().tp() != TiDB::TP::TypeNewDecimal
            || (expr.field_type().decimal() != -1 && expr.field_type().flen() != 0 && expr.field_type().flen() != -1));
}

bool isUnsupportedEncodeType(const std::vector<tipb::FieldType> & types, tipb::EncodeType encode_type)
{
    const static std::unordered_map<tipb::EncodeType, std::unordered_set<Int32>> unsupported_types_map({
        {tipb::EncodeType::TypeCHBlock,
         {TiDB::TypeSet, TiDB::TypeGeometry, TiDB::TypeNull, TiDB::TypeEnum, TiDB::TypeJSON, TiDB::TypeBit}},
        {tipb::EncodeType::TypeChunk, {TiDB::TypeSet, TiDB::TypeGeometry, TiDB::TypeNull}},
    });

    auto unsupported_set = unsupported_types_map.find(encode_type);
    if (unsupported_set == unsupported_types_map.end())
        return false;
    for (const auto & type : types)
    {
        if (unsupported_set->second.find(type.tp()) != unsupported_set->second.end())
            return true;
    }
    return false;
}

DataTypePtr inferDataType4Literal(const tipb::Expr & expr)
{
    Field value = decodeLiteral(expr);
    DataTypePtr flash_type = applyVisitor(FieldToDataType(), value);
    /// need to extract target_type from expr.field_type() because the flash_type derived from
    /// value is just a `memory type`, which does not have enough information, for example:
    /// for date literal, the flash_type is `UInt64`
    DataTypePtr target_type{};
    if (expr.tp() == tipb::ExprType::Null)
    {
        // todo We should use DataTypeNothing as NULL literal's TiFlash Type, because TiFlash has a lot of
        //  optimization for DataTypeNothing, but there are still some bugs when using DataTypeNothing: when
        //  TiFlash try to return data to TiDB or exchange data between TiFlash node, since codec only recognize
        //  TiDB type, use DataTypeNothing will meet error in the codec, so do not use DataTypeNothing until
        //  we fix the codec issue.
        if (exprHasValidFieldType(expr))
        {
            target_type = getDataTypeByFieldTypeForComputingLayer(expr.field_type());
        }
        else
        {
            if (expr.has_field_type() && expr.field_type().tp() == TiDB::TP::TypeNewDecimal)
                target_type = createDecimal(1, 0);
            else
                target_type = flash_type;
        }
        target_type = makeNullable(target_type);
    }
    else
    {
        if (expr.tp() == tipb::ExprType::MysqlDecimal)
        {
            /// to fix https://github.com/pingcap/tics/issues/1425, when TiDB push down
            /// a decimal literal, it contains two types: one is the type that encoded
            /// in Decimal value itself(i.e. expr.val()), the other is the type that in
            /// expr.field_type(). According to TiDB and Mysql behavior, the computing
            /// layer should use the type in expr.val(), which means we should ignore
            /// the type in expr.field_type()
            target_type = flash_type;
        }
        else
        {
            target_type
                = exprHasValidFieldType(expr) ? getDataTypeByFieldTypeForComputingLayer(expr.field_type()) : flash_type;
        }
        // We should remove nullable for constant value since TiDB may not set NOT_NULL flag for literal expression.
        target_type = removeNullable(target_type);
    }
    return target_type;
}

UInt8 getFieldLengthForArrowEncode(Int32 tp)
{
    switch (tp)
    {
    case TiDB::TypeTiny:
    case TiDB::TypeShort:
    case TiDB::TypeInt24:
    case TiDB::TypeLong:
    case TiDB::TypeLongLong:
    case TiDB::TypeYear:
    case TiDB::TypeDouble:
    case TiDB::TypeTime:
    case TiDB::TypeDate:
    case TiDB::TypeDatetime:
    case TiDB::TypeNewDate:
    case TiDB::TypeTimestamp:
        return 8;
    case TiDB::TypeFloat:
        return 4;
    case TiDB::TypeDecimal:
    case TiDB::TypeNewDecimal:
        return 40;
    case TiDB::TypeVarchar:
    case TiDB::TypeVarString:
    case TiDB::TypeString:
    case TiDB::TypeBlob:
    case TiDB::TypeTinyBlob:
    case TiDB::TypeMediumBlob:
    case TiDB::TypeLongBlob:
    case TiDB::TypeBit:
    case TiDB::TypeEnum:
    case TiDB::TypeJSON:
    case TiDB::TypeTiDBVectorFloat32:
        return VAR_SIZE;
    default:
        throw TiFlashException(
            "not supported field type in arrow encode: " + std::to_string(tp),
            Errors::Coprocessor::Internal);
    }
}

tipb::Expr constructStringLiteralTiExpr(const String & value)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::String);
    expr.set_val(value);
    auto * field_type = expr.mutable_field_type();
    field_type->set_tp(TiDB::TypeString);
    field_type->set_flag(TiDB::ColumnFlagNotNull);
    return expr;
}

tipb::Expr constructInt64LiteralTiExpr(Int64 value)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::Int64);
    WriteBufferFromOwnString ss;
    encodeDAGInt64(value, ss);
    expr.set_val(ss.releaseStr());
    auto * field_type = expr.mutable_field_type();
    field_type->set_tp(TiDB::TypeLongLong);
    field_type->set_flag(TiDB::ColumnFlagNotNull);
    return expr;
}

tipb::Expr constructDateTimeLiteralTiExpr(UInt64 packed_value)
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::MysqlTime);
    WriteBufferFromOwnString ss;
    encodeDAGUInt64(packed_value, ss);
    expr.set_val(ss.releaseStr());
    auto * field_type = expr.mutable_field_type();
    field_type->set_tp(TiDB::TypeDatetime);
    field_type->set_flag(TiDB::ColumnFlagNotNull);
    return expr;
}

tipb::Expr constructNULLLiteralTiExpr()
{
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::Null);
    auto * field_type = expr.mutable_field_type();
    field_type->set_tp(TiDB::TypeNull);
    return expr;
}

tipb::Expr constructZeroVectorFloat32TiExpr()
{
    RUNTIME_CHECK(boost::endian::order::native == boost::endian::order::little);
    tipb::Expr expr;
    expr.set_tp(tipb::ExprType::TiDBVectorFloat32);
    WriteBufferFromOwnString ss;
    writeIntBinary(static_cast<UInt32>(0), ss);
    expr.set_val(ss.releaseStr());
    auto * field_type = expr.mutable_field_type();
    field_type->set_tp(TiDB::TypeTiDBVectorFloat32);
    field_type->set_flag(TiDB::ColumnFlagNotNull);
    return expr;
}

TiDB::TiDBCollatorPtr getCollatorFromExpr(const tipb::Expr & expr)
{
    if (expr.has_field_type())
        return getCollatorFromFieldType(expr.field_type());
    return nullptr;
}

SortDescription getSortDescription(
    const std::vector<NameAndTypePair> & order_columns,
    const google::protobuf::RepeatedPtrField<tipb::ByItem> & by_items)
{
    SortDescription order_descr;
    order_descr.reserve(by_items.size());
    for (int i = 0; i < by_items.size(); i++)
    {
        const auto & name = order_columns[i].name;
        int direction = by_items[i].desc() ? -1 : 1;
        // MySQL/TiDB treats NULL as "minimum".
        int nulls_direction = -1;
        TiDB::TiDBCollatorPtr collator = nullptr;
        if (removeNullable(order_columns[i].type)->isString())
            collator = getCollatorFromExpr(by_items[i].expr());

        order_descr.emplace_back(name, direction, nulls_direction, collator);
    }
    return order_descr;
}

String genFuncString(
    const String & func_name,
    const Names & argument_names,
    const TiDB::TiDBCollators & collators,
    const std::vector<const tipb::FieldType *> & field_types)
{
    FmtBuffer buf;
    buf.fmtAppend("{}({})_collator", func_name, fmt::join(argument_names.begin(), argument_names.end(), ", "));
    for (const auto & collator : collators)
    {
        if (collator)
            buf.fmtAppend("_{}", collator->getCollatorId());
        else
            buf.append("_0");
    }
    buf.append(" ");
    buf.joinStr(
        field_types.begin(),
        field_types.end(),
        [](const auto & field_type, FmtBuffer & buffer) {
            if likely (field_type)
                buffer.fmtAppend("{}|{}", field_type->flag(), field_type->flen());
        },
        ", ");
    return buf.toString();
}

TiDB::TiDBCollatorPtr getCollatorFromFieldType(const tipb::FieldType & field_type)
{
    if (field_type.collate() < 0)
        return TiDB::ITiDBCollator::getCollator(-field_type.collate());
    return nullptr;
}

bool hasUnsignedFlag(const tipb::FieldType & tp)
{
    return tp.flag() & TiDB::ColumnFlagUnsigned;
}

bool hasIsBooleanFlag(const tipb::FieldType & tp)
{
    return tp.flag() & TiDB::ColumnFlagIsBooleanFlag;
}

bool hasParseToJSONFlag(const tipb::FieldType & tp)
{
    return tp.flag() & TiDB::ColumnFlagParseToJSON;
}

void assertBlockSchema(const DataTypes & expected_types, const Block & block, const String & context_description)
{
    assertBlockSchema(
        [&] { return expected_types.size(); },
        [&](auto i) { return expected_types[i]; },
        block,
        context_description);
}

void assertBlockSchema(const Block & header, const Block & block, const String & context_description)
{
    assertBlockSchema(
        [&] { return header.columns(); },
        [&](auto i) { return header.getByPosition(i).type; },
        block,
        context_description);
}

tipb::DAGRequest getDAGRequestFromStringWithRetry(const String & s)
{
    tipb::DAGRequest dag_req;
    if (!dag_req.ParseFromString(s))
    {
        /// ParseFromString will use the default recursion limit, which is 100 to decode the plan, if the plan tree is too deep,
        /// it may exceed this limit, so just try again by double the recursion limit
        ::google::protobuf::io::CodedInputStream coded_input_stream(
            reinterpret_cast<const UInt8 *>(s.data()),
            s.size());
        coded_input_stream.SetRecursionLimit(::google::protobuf::io::CodedInputStream::GetDefaultRecursionLimit() * 2);
        if (!dag_req.ParseFromCodedStream(&coded_input_stream))
        {
            /// just return error if decode failed this time, because it's really a corner case, and even if we can decode the plan
            /// successfully by using a very large value of the recursion limit, it is kinds of meaningless because the runtime
            /// performance of this task may be very bad if the plan tree is too deep
            throw TiFlashException(
                std::string(__PRETTY_FUNCTION__)
                    + ": Invalid encoded plan, the most likely is that the plan/expression tree is too deep",
                Errors::Coprocessor::BadRequest);
        }
    }
    return dag_req;
}

tipb::ScalarFuncSig reverseGetFuncSigByFuncName(const String & name)
{
    static std::unordered_map<String, tipb::ScalarFuncSig> func_name_sig_map = getFuncNameToSigMap();
    if (func_name_sig_map.find(name) == func_name_sig_map.end())
        throw Exception(fmt::format("Unsupported function {}", name));
    return func_name_sig_map[name];
}
} // namespace DB
