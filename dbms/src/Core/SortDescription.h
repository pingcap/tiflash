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

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace TiDB
{
class ITiDBCollator;
}

namespace DB
{
/// Description of the sorting rule by one column.
struct SortColumnDescription
{
    /// The name of the column.
    std::string column_name;
    /// Column number (used if no name is given).
    size_t column_number;
    /// 1 - ascending, -1 - descending.
    int direction;
    /// 1 - NULLs and NaNs are greater, -1 - less.
    /// To achieve NULLS LAST, set it equal to direction, to achieve NULLS FIRST, set it opposite.
    int nulls_direction;
    /// Collator for locale-specific comparison of strings
    TiDB::ITiDBCollator const * collator = nullptr;

    SortColumnDescription(
        size_t column_number_,
        int direction_,
        int nulls_direction_,
        TiDB::ITiDBCollator const * collator_ = nullptr)
        : column_number(column_number_)
        , direction(direction_)
        , nulls_direction(nulls_direction_)
        , collator(collator_)
    {}

    SortColumnDescription(
        const std::string & column_name_,
        int direction_,
        int nulls_direction_,
        TiDB::ITiDBCollator const * collator_ = nullptr)
        : column_name(column_name_)
        , column_number(0)
        , direction(direction_)
        , nulls_direction(nulls_direction_)
        , collator(collator_)
    {}

    /// For IBlockInputStream.
    std::string getID() const;
};

/// Description of the sorting rule for several columns.
using SortDescription = std::vector<SortColumnDescription>;

} // namespace DB
