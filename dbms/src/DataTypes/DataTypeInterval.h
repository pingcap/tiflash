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

#include <DataTypes/DataTypeNumberBase.h>


namespace DB
{
/** Data type to deal with INTERVAL in SQL (arithmetic on time intervals).
  *
  * Mostly the same as Int64.
  * But also tagged with interval kind.
  *
  * Intended isage is for temporary elements in expressions,
  *  not for storing values in tables.
  */
class DataTypeInterval final : public DataTypeNumberBase<Int64>
{
public:
    enum Kind
    {
        Second,
        Minute,
        Hour,
        Day,
        Week,
        Month,
        Year
    };

private:
    Kind kind;

public:
    static constexpr bool is_parametric = true;

    Kind getKind() const { return kind; }

    const char * kindToString() const
    {
        switch (kind)
        {
        case Second:
            return "Second";
        case Minute:
            return "Minute";
        case Hour:
            return "Hour";
        case Day:
            return "Day";
        case Week:
            return "Week";
        case Month:
            return "Month";
        case Year:
            return "Year";
        default:
            __builtin_unreachable();
        }
    }

    DataTypeInterval(Kind kind)
        : kind(kind){};

    std::string getName() const override { return std::string("Interval") + kindToString(); }
    const char * getFamilyName() const override { return "Interval"; }

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool cannotBeStoredInTables() const override { return true; }
    bool isCategorial() const override { return false; }
    TypeIndex getTypeId() const override { return TypeIndex::Interval; }
};

} // namespace DB
