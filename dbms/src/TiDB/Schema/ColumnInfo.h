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
#pragma once

#include <Common/Decimal.h>
#include <Storages/Transaction/Types.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#pragma GCC diagnostic pop

namespace TiDB
{
using DB::ColumnID;

struct ColumnInfo
{
    ColumnInfo() = default;

    explicit ColumnInfo(Poco::JSON::Object::Ptr json);

    Poco::JSON::Object::Ptr getJSONObject() const;

    void deserialize(Poco::JSON::Object::Ptr json);

    ColumnID id = -1;
    String name;
    Poco::Dynamic::Var origin_default_value;
    Poco::Dynamic::Var default_value;
    Poco::Dynamic::Var default_bit_value;
    TP tp = TypeDecimal; // TypeDecimal is not used by TiDB.
    UInt32 flag = 0;
    Int32 flen = 0;
    Int32 decimal = 0;
    Poco::Dynamic::Var charset;
    Poco::Dynamic::Var collate;
    // Elems is the element list for enum and set type.
    std::vector<std::pair<std::string, Int16>> elems;
    SchemaState state = StateNone;
    String comment;

#ifdef M
#error "Please undefine macro M first."
#endif
#define M(f, v)                      \
    inline bool has##f##Flag() const \
    {                                \
        return (flag & (v)) != 0;    \
    }                                \
    inline void set##f##Flag()       \
    {                                \
        flag |= (v);                 \
    }                                \
    inline void clear##f##Flag()     \
    {                                \
        flag &= (~(v));              \
    }
    COLUMN_FLAGS(M)
#undef M

    DB::Field defaultValueToField() const;
    CodecFlag getCodecFlag() const;
    DB::Field getDecimalValue(const String &) const;
    Int64 getEnumIndex(const String &) const;
    UInt64 getSetValue(const String &) const;
    static Int64 getTimeValue(const String &);
    static Int64 getYearValue(const String &);
    static UInt64 getBitValue(const String &);

private:
    /// please be very careful when you have to use offset,
    /// because we never update offset when DDL action changes.
    /// Thus, our offset will not exactly correspond the order of columns.
    Int32 offset = -1;
};
} // namespace TiDB