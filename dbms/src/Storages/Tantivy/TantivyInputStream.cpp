// Copyright 2025 PingCAP, Ltd.
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

#include <Storages/Tantivy/TantivyInputStream.h>

namespace DB::TS
{

rust::Vec<rust::String> getFields(NamesAndTypes & columns)
{
    rust::Vec<rust::String> fields;
    for (auto & name_and_type : columns)
    {
        fields.push_back(name_and_type.name);
    }
    return fields;
}

rust::Vec<::Range> getKeyRanges(ShardInfo::KeyRanges & key_ranges)
{
    rust::Vec<::Range> res;
    for (const auto & range : key_ranges)
    {
        rust::Slice<::std::uint8_t const> start(
            reinterpret_cast<const unsigned char *>(range.start().c_str()),
            range.start().size());
        rust::Slice<::std::uint8_t const> end(
            reinterpret_cast<const unsigned char *>(range.end().c_str()),
            range.end().size());
        res.push_back({
            .start = std::move(start),
            .end = std::move(end),
        });
    }
    return res;
}

std::tuple<::Expr, std::vector<ColumnID>> tipbToTiCIExpr(const tipb::Expr & expr, const TimezoneInfo & timezone_info)
{
    ::Expr ret;
    switch (expr.tp())
    {
    case tipb::ExprType::ScalarFunc:
    {
        std::vector<ColumnID> children_cids;
        ret.tp = tipb::ExprType::ScalarFunc;
        for (const auto & child : expr.children())
        {
            auto [child_expr, child_cids] = tipbToTiCIExpr(child, timezone_info);
            ret.children.push_back(child_expr);
            children_cids.insert(children_cids.end(), child_cids.begin(), child_cids.end());
        }
        switch (expr.sig())
        {
        case tipb::ScalarFuncSig::FTSMatchWord:
        case tipb::ScalarFuncSig::FTSMatchPrefix:
        case tipb::ScalarFuncSig::LogicalAnd:
        case tipb::ScalarFuncSig::LogicalOr:
        case tipb::ScalarFuncSig::UnaryNotInt:
        case tipb::ScalarFuncSig::UnaryNotReal:
        case tipb::ScalarFuncSig::EQInt:
        case tipb::ScalarFuncSig::NEInt:
        case tipb::ScalarFuncSig::LTInt:
        case tipb::ScalarFuncSig::LEInt:
        case tipb::ScalarFuncSig::GTInt:
        case tipb::ScalarFuncSig::GEInt:
        case tipb::ScalarFuncSig::EQString:
        case tipb::ScalarFuncSig::NEString:
        case tipb::ScalarFuncSig::LTString:
        case tipb::ScalarFuncSig::LEString:
        case tipb::ScalarFuncSig::GTString:
        case tipb::ScalarFuncSig::GEString:
        case tipb::ScalarFuncSig::EQReal:
        case tipb::ScalarFuncSig::NEReal:
        case tipb::ScalarFuncSig::LTReal:
        case tipb::ScalarFuncSig::LEReal:
        case tipb::ScalarFuncSig::GTReal:
        case tipb::ScalarFuncSig::GEReal:
        case tipb::ScalarFuncSig::EQDecimal:
        case tipb::ScalarFuncSig::NEDecimal:
        case tipb::ScalarFuncSig::LTDecimal:
        case tipb::ScalarFuncSig::LEDecimal:
        case tipb::ScalarFuncSig::GTDecimal:
        case tipb::ScalarFuncSig::GEDecimal:
        case tipb::ScalarFuncSig::InInt:
        case tipb::ScalarFuncSig::InString:
        case tipb::ScalarFuncSig::InReal:
        case tipb::ScalarFuncSig::InDecimal:
            ret.sig = expr.sig();
            break;
        case tipb::ScalarFuncSig::EQTime:
        case tipb::ScalarFuncSig::NETime:
        case tipb::ScalarFuncSig::LTTime:
        case tipb::ScalarFuncSig::LETime:
        case tipb::ScalarFuncSig::GTTime:
        case tipb::ScalarFuncSig::GETime:
        {
            ret.sig = expr.sig();
            size_t col_idx = 0, val_idx = 1;
            if (isColumnExpr(expr.children(1)))
                std::swap(col_idx, val_idx);
            if (expr.children(col_idx).field_type().tp() == TiDB::TypeTimestamp)
            {
                const auto & child_expr = expr.children(val_idx);
                if (isLiteralExpr(child_expr))
                {
                    UInt64 val = decodeDAGUInt64(child_expr.val());
                    val = convertPackedU64WithTimezone(val, timezone_info);
                    WriteBufferFromOwnString ss;
                    encodeDAGUInt64(val, ss);
                    ret.children[val_idx].val.clear();
                    auto str = ss.releaseStr();
                    std::copy(str.begin(), str.end(), std::back_inserter(ret.children[val_idx].val));
                }
            }
            break;
        }
        case tipb::ScalarFuncSig::InTime:
        {
            ret.sig = expr.sig();
            if (expr.children(0).field_type().tp() == TiDB::TypeTimestamp)
            {
                for (int val_idx = 1; val_idx < expr.children_size(); ++val_idx)
                {
                    const auto & child_expr = expr.children(val_idx);
                    if (isLiteralExpr(child_expr))
                    {
                        UInt64 val = decodeDAGUInt64(child_expr.val());
                        val = convertPackedU64WithTimezone(val, timezone_info);
                        WriteBufferFromOwnString ss;
                        encodeDAGUInt64(val, ss);
                        ret.children[val_idx].val.clear();
                        auto str = ss.releaseStr();
                        std::copy(str.begin(), str.end(), std::back_inserter(ret.children[val_idx].val));
                    }
                    else
                    {
                        throw TiFlashException("InTime only support literal values", Errors::Coprocessor::BadRequest);
                    }
                }
            }
            break;
        }
        default:
            throw std::runtime_error("Unsupported expression sig");
        }

        return {ret, children_cids};
    }
    case tipb::ExprType::ColumnRef:
    {
        ret.tp = expr.tp();
        auto id = decodeDAGInt64(expr.val());
        auto str = fmt::format("column_{}", id);
        std::copy(str.begin(), str.end(), std::back_inserter(ret.val));
        return {ret, {id}};
    }
    case tipb::ExprType::String:
    case tipb::ExprType::Int64:
    case tipb::ExprType::Uint64:
    case tipb::ExprType::Float32:
    case tipb::ExprType::Float64:
    case tipb::ExprType::MysqlTime:
    {
        ret.tp = expr.tp();
        std::copy(expr.val().begin(), expr.val().end(), std::back_inserter(ret.val));
        return {ret, {}};
    }
    case tipb::ExprType::MysqlDecimal:
    {
        ret.tp = expr.tp();
        auto field = decodeDAGDecimal(expr.val());
        String str;
        if (field.getType() == Field::Types::Decimal32)
            str = field.get<DecimalField<Decimal32>>().toString();
        else if (field.getType() == Field::Types::Decimal64)
            str = field.get<DecimalField<Decimal64>>().toString();
        else if (field.getType() == Field::Types::Decimal128)
            str = field.get<DecimalField<Decimal128>>().toString();
        else if (field.getType() == Field::Types::Decimal256)
            str = field.get<DecimalField<Decimal256>>().toString();
        else
            throw TiFlashException("Not decimal literal" + expr.DebugString(), Errors::Coprocessor::BadRequest);
        std::copy(str.begin(), str.end(), std::back_inserter(ret.val));
        return {ret, {}};
    }
    default:
        throw std::runtime_error("Unsupported expression type");
    }
}

std::tuple<::Expr, std::vector<ColumnID>> tipbToTiCIExpr(
    google::protobuf::RepeatedPtrField<tipb::Expr> exprs,
    const TimezoneInfo & tz)
{
    if (exprs.empty())
    {
        throw std::runtime_error("Empty match expression");
    }
    auto [ret, cids] = tipbToTiCIExpr(exprs[0], tz);
    for (auto i = 1; i < exprs.size(); ++i)
    {
        auto [child_expr, child_cids] = tipbToTiCIExpr(exprs[i], tz);
        ret = {
            .tp = tipb::ExprType::ScalarFunc,
            .children = {ret, child_expr},
            .sig = tipb::ScalarFuncSig::LogicalAnd,
        };
        cids.insert(cids.end(), child_cids.begin(), child_cids.end());
    }
    return {ret, cids};
}

Block TantivyInputStream::readImpl()
{
    if (done)
    {
        return {};
    }
    Block ret = readFromS3(is_count);
    done = true;
    return ret;
}

Block TantivyInputStream::readFromS3(bool is_count)
{
    auto return_fields = getFields(return_columns);
    auto shard_info = query_shard_info;
    LOG_DEBUG(log, "shard info: {}", shard_info.toString());
    auto key_ranges = getKeyRanges(shard_info.key_ranges);

    auto search_param = SearchParam{static_cast<size_t>(limit)};
    if (is_count)
        return_fields = {};

    SearchResult search_result = search(
        {
            .keyspace_id = keyspace_id,
            .table_id = table_id,
            .index_id = index_id,
            .shard_id = shard_info.shard_id,
            .shard_epoch = shard_info.shard_epoch,
        },
        key_ranges,
        return_fields,
        match_expr,
        search_param,
        read_ts);

    Block res(return_columns);
    if (is_count)
    {
        if (search_result.count == 0)
        {
            return res;
        }
        // Construct const columns with search_result.count items, only used by count(*)
        for (auto & name_and_type : return_columns)
        {
            auto type_default_value = name_and_type.type->getDefault();
            auto col
                = name_and_type.type->createColumnConst(static_cast<size_t>(search_result.count), type_default_value);
            res.getByName(name_and_type.name).column = std::move(col);
        }
        return res;
    }


    auto documents = search_result.rows;
    if (documents.empty())
    {
        return res;
    }
    for (auto & name_and_type : return_columns)
    {
        int idx = -1;
        for (size_t j = 0; j < documents[0].fieldValues.size(); j++)
        {
            if (documents[0].fieldValues[j].field_name == name_and_type.name)
            {
                idx = j;
                break;
            }
        }
        if (idx == -1)
        {
            for (size_t j = 0; j < documents.size(); j++)
            {
                // Insert default value for missing fields
                res.getByName(name_and_type.name).column->assumeMutable()->insertDefault();
            }
            continue;
        }

        auto col = res.getByName(name_and_type.name).column->assumeMutable();
        if (removeNullable(name_and_type.type)->isStringOrFixedString())
        {
            for (auto & doc : documents)
            {
                const auto & v = doc.fieldValues[idx].string_value;
                col->insert(Field(String(v.begin(), v.end())));
            }
        }
        if (removeNullable(name_and_type.type)->isInteger())
        {
            for (auto & doc : documents)
            {
                col->insert(Field(doc.fieldValues[idx].int_value));
            }
        }
        if (removeNullable(name_and_type.type)->isDateOrDateTime())
        {
            for (auto & doc : documents)
            {
                auto t = static_cast<UInt64>(doc.fieldValues[idx].int_value);
                col->insert(Field(t));
            }
        }
    }
    return res;
}

} // namespace DB::TS
