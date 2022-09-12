// Copyright 2022 PingCAP, Ltd.
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
#include <Common/types ̰.h>
#include <Poco/Types.h>
#include <TiDB/Schema/DBInfo.h>
#include <TiDB/Schema/IndexInfo.h>

namespace TiDB {
ColumnInfo::ColumnInfo(Poco::JSON::Object::Ptr json)
{
    deserialize(json);
}

Field ColumnInfo::defaultValueToField() const
{
    const auto & value = origin_default_value;
    if (value.isEmpty())
    {
        if (hasNotNullFlag())
            return DB::GenDefaultField(*this);
        return Field();
    }
    switch (tp)
    {
    // Integer Type.
    case TypeTiny:
    case TypeShort:
    case TypeLong:
    case TypeLongLong:
    case TypeInt24:
    {
        // In c++, cast a unsigned integer to signed integer will not change the value.
        // like 9223372036854775808 which is larger than the maximum value of Int64,
        // static_cast<UInt64>(static_cast<Int64>(9223372036854775808)) == 9223372036854775808
        // so we don't need consider unsigned here.
        try
        {
            return value.convert<Int64>();
        }
        catch (...)
        {
            // due to https://github.com/pingcap/tidb/issues/34881
            // we do this to avoid exception in older version of TiDB.
            return static_cast<Int64>(std::llround(value.convert<double>()));
        }
    }
    case TypeBit:
    {
        // TODO: We shall use something like `orig_default_bit`, which will never change once created,
        //  rather than `default_bit`, which could be altered.
        //  See https://github.com/pingcap/tidb/issues/17641 and https://github.com/pingcap/tidb/issues/17642
        const auto & bit_value = default_bit_value;
        // TODO: There might be cases that `orig_default` is not null but `default_bit` is null,
        //  i.e. bit column added with an default value but later modified to another.
        //  For these cases, neither `orig_default` (may get corrupted) nor `default_bit` (modified) is correct.
        //  This is a bug anyway, we choose to make it simple, i.e. use `default_bit`.
        if (bit_value.isEmpty())
        {
            if (hasNotNullFlag())
                return DB::GenDefaultField(*this);
            return Field();
        }
        return getBitValue(bit_value.convert<String>());
    }
    // Floating type.
    case TypeFloat:
    case TypeDouble:
        return value.convert<double>();
    case TypeDate:
    case TypeDatetime:
    case TypeTimestamp:
        return DB::parseMyDateTime(value.convert<String>());
    case TypeVarchar:
    case TypeTinyBlob:
    case TypeMediumBlob:
    case TypeLongBlob:
    case TypeBlob:
    case TypeVarString:
    case TypeString:
    {
        auto v = value.convert<String>();
        if (hasBinaryFlag())
        {
            // For some binary column(like varchar(20)), we have to pad trailing zeros according to the specified type length.
            // User may define default value `0x1234` for a `BINARY(4)` column, TiDB stores it in a string "\u12\u34" (sized 2).
            // But it actually means `0x12340000`.
            // And for some binary column(like longblob), we do not need to pad trailing zeros.
            // And the `Flen` is set to -1, therefore we need to check `Flen >= 0` here.
            if (Int32 vlen = v.length(); flen >= 0 && vlen < flen)
                v.append(flen - vlen, '\0');
        }
        return v;
    }
    case TypeJSON:
        // JSON can't have a default value
        return genJsonNull();
    case TypeEnum:
        return getEnumIndex(value.convert<String>());
    case TypeNull:
        return Field();
    case TypeDecimal:
    case TypeNewDecimal:
        return getDecimalValue(value.convert<String>());
    case TypeTime:
        return getTimeValue(value.convert<String>());
    case TypeYear:
        return getYearValue(value.convert<String>());
    case TypeSet:
        return getSetValue(value.convert<String>());
    default:
        throw Exception("Have not processed type: " + std::to_string(tp));
    }
    return Field();
}

DB::Field ColumnInfo::getDecimalValue(const String & decimal_text) const
{
    DB::ReadBufferFromString buffer(decimal_text);
    auto precision = flen;
    auto scale = decimal;

    auto type = DB::createDecimal(precision, scale);
    if (DB::checkDecimal<Decimal32>(*type))
    {
        DB::Decimal32 result;
        DB::readDecimalText(result, buffer, precision, scale);
        return DecimalField<Decimal32>(result, scale);
    }
    else if (DB::checkDecimal<Decimal64>(*type))
    {
        DB::Decimal64 result;
        DB::readDecimalText(result, buffer, precision, scale);
        return DecimalField<Decimal64>(result, scale);
    }
    else if (DB::checkDecimal<Decimal128>(*type))
    {
        DB::Decimal128 result;
        DB::readDecimalText(result, buffer, precision, scale);
        return DecimalField<Decimal128>(result, scale);
    }
    else
    {
        DB::Decimal256 result;
        DB::readDecimalText(result, buffer, precision, scale);
        return DecimalField<Decimal256>(result, scale);
    }
}

// FIXME it still has bug: https://github.com/pingcap/tidb/issues/11435
Int64 ColumnInfo::getEnumIndex(const String & enum_id_or_text) const
{
    const auto * collator = ITiDBCollator::getCollator(collate.isEmpty() ? "binary" : collate.convert<String>());
    if (!collator)
        // todo if new collation is enabled, should use "utf8mb4_bin"
        collator = ITiDBCollator::getCollator("binary");
    for (const auto & elem : elems)
    {
        if (collator->compare(elem.first.data(), elem.first.size(), enum_id_or_text.data(), enum_id_or_text.size()) == 0)
        {
            return elem.second;
        }
    }
    int num = std::stoi(enum_id_or_text);
    return num;
}

UInt64 ColumnInfo::getSetValue(const String & set_str) const
{
    const auto * collator = ITiDBCollator::getCollator(collate.isEmpty() ? "binary" : collate.convert<String>());
    if (!collator)
        // todo if new collation is enabled, should use "utf8mb4_bin"
        collator = ITiDBCollator::getCollator("binary");
    std::string sort_key_container;
    Poco::StringTokenizer string_tokens(set_str, ",");
    std::set<String> marked;
    for (const auto & s : string_tokens)
        marked.insert(collator->sortKey(s.data(), s.length(), sort_key_container).toString());

    UInt64 value = 0;
    for (size_t i = 0; i < elems.size(); i++)
    {
        String key = collator->sortKey(elems.at(i).first.data(), elems.at(i).first.length(), sort_key_container).toString();
        auto it = marked.find(key);
        if (it != marked.end())
        {
            value |= 1ULL << i;
            marked.erase(it);
        }
    }

    if (marked.empty())
        return value;

    throw DB::Exception(std::string(__PRETTY_FUNCTION__) + ": can't parse set type value.");
}

Int64 ColumnInfo::getTimeValue(const String & time_str)
{
    const static int64_t fractional_seconds_multiplier[] = {1000000000, 100000000, 10000000, 1000000, 100000, 10000, 1000, 100, 10, 1};
    bool negative = time_str[0] == '-';
    Poco::StringTokenizer second_and_fsp(time_str, ".");
    Poco::StringTokenizer string_tokens(second_and_fsp[0], ":");
    Int64 ret = 0;
    for (auto const & s : string_tokens)
        ret = ret * 60 + std::abs(std::stoi(s));
    Int32 fs_length = 0;
    Int64 fs_value = 0;
    if (second_and_fsp.count() == 2)
    {
        fs_length = second_and_fsp[1].length();
        fs_value = std::stol(second_and_fsp[1]);
    }
    ret = ret * fractional_seconds_multiplier[0] + fs_value * fractional_seconds_multiplier[fs_length];
    return negative ? -ret : ret;
}

Int64 ColumnInfo::getYearValue(const String & val)
{
    // do not check validation of the val because TiDB will do it
    Int64 year = std::stol(val);
    if (0 < year && year < 70)
        return 2000 + year;
    if (70 <= year && year < 100)
        return 1900 + year;
    if (year == 0 && val.length() <= 2)
        return 2000;
    return year;
}

UInt64 ColumnInfo::getBitValue(const String & val)
{
    // The `default_bit` is a base64 encoded, big endian byte array.
    Poco::MemoryInputStream istr(val.data(), val.size());
    Poco::Base64Decoder decoder(istr);
    std::string decoded;
    Poco::StreamCopier::copyToString(decoder, decoded);
    UInt64 result = 0;
    for (auto c : decoded)
    {
        result = result << 8 | c;
    }
    return result;
}

Poco::JSON::Object::Ptr ColumnInfo::getJSONObject() const
try
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();

    json->set("id", id);
    Poco::JSON::Object::Ptr name_json = new Poco::JSON::Object();
    name_json->set("O", name);
    name_json->set("L", name);
    json->set("name", name_json);
    json->set("offset", offset);
    json->set("origin_default", origin_default_value);
    json->set("default", default_value);
    json->set("default_bit", default_bit_value);
    Poco::JSON::Object::Ptr tp_json = new Poco::JSON::Object();
    tp_json->set("Tp", static_cast<Int32>(tp));
    tp_json->set("Flag", flag);
    tp_json->set("Flen", flen);
    tp_json->set("Decimal", decimal);
    tp_json->set("Charset", charset);
    tp_json->set("Collate", collate);
    if (!elems.empty())
    {
        Poco::JSON::Array::Ptr elem_arr = new Poco::JSON::Array();
        for (const auto & elem : elems)
            elem_arr->add(elem.first);
        tp_json->set("Elems", elem_arr);
    }
    else
    {
        tp_json->set("Elems", Poco::Dynamic::Var());
    }
    json->set("type", tp_json);
    json->set("state", static_cast<Int32>(state));
    json->set("comment", comment);

#ifndef NDEBUG
    // Check stringify in Debug mode
    std::stringstream str;
    json->stringify(str);
#endif

    return json;
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Serialize TiDB schema JSON failed (ColumnInfo): " + e.displayText(),
        DB::Exception(e));
}

void ColumnInfo::deserialize(Poco::JSON::Object::Ptr json)
try
{
    id = json->getValue<ColumnID>("id");
    name = json->getObject("name")->getValue<String>("L");
    offset = json->getValue<Int32>("offset");
    if (!json->isNull("origin_default"))
        origin_default_value = json->get("origin_default");
    if (!json->isNull("default"))
        default_value = json->get("default");
    if (!json->isNull("default_bit"))
        default_bit_value = json->get("default_bit");
    auto type_json = json->getObject("type");
    tp = static_cast<TP>(type_json->getValue<Int32>("Tp"));
    flag = type_json->getValue<UInt32>("Flag");
    flen = type_json->getValue<Int64>("Flen");
    decimal = type_json->getValue<Int64>("Decimal");
    if (!type_json->isNull("Elems"))
    {
        auto elems_arr = type_json->getArray("Elems");
        size_t elems_size = elems_arr->size();
        for (size_t i = 1; i <= elems_size; i++)
        {
            elems.push_back(std::make_pair(elems_arr->getElement<String>(i - 1), static_cast<Int16>(i)));
        }
    }
    /// need to do this check for forward compatibility
    if (!type_json->isNull("Charset"))
        charset = type_json->get("Charset");
    /// need to do this check for forward compatibility
    if (!type_json->isNull("Collate"))
        collate = type_json->get("Collate");
    state = static_cast<SchemaState>(json->getValue<Int32>("state"));
    comment = json->getValue<String>("comment");
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (ColumnInfo): " + e.displayText(),
        DB::Exception(e));
}

}