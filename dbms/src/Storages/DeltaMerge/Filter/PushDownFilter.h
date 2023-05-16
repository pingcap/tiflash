// Copyright 2023 PingCAP, Ltd.
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

#include <Interpreters/ExpressionActions.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB::DM
{

class PushDownFilter;
using PushDownFilterPtr = std::shared_ptr<PushDownFilter>;
inline static const PushDownFilterPtr EMPTY_FILTER{};

class PushDownFilter : public std::enable_shared_from_this<PushDownFilter>
{
public:
    PushDownFilter(const RSOperatorPtr & rs_operator_,
                   const ExpressionActionsPtr & beofre_where_,
                   const ColumnDefines & filter_columns_,
                   const String filter_column_name_,
                   const ExpressionActionsPtr & extra_cast_,
<<<<<<< HEAD
                   const ColumnDefines & columns_after_cast_)
=======
                   const ColumnDefinesPtr & columns_after_cast_)
>>>>>>> 698fdde3ff (Fix query fail when there are timestamp or time columns after generated column (#7469))
        : rs_operator(rs_operator_)
        , before_where(beofre_where_)
        , filter_column_name(std::move(filter_column_name_))
        , filter_columns(std::move(filter_columns_))
        , extra_cast(extra_cast_)
<<<<<<< HEAD
        , columns_after_cast(std::move(columns_after_cast_))
=======
        , columns_after_cast(columns_after_cast_)
>>>>>>> 698fdde3ff (Fix query fail when there are timestamp or time columns after generated column (#7469))
    {}

    explicit PushDownFilter(const RSOperatorPtr & rs_operator_)
        : rs_operator(rs_operator_)
<<<<<<< HEAD
        , before_where(nullptr)
        , filter_columns({})
        , extra_cast(nullptr)
        , columns_after_cast({})
=======
>>>>>>> 698fdde3ff (Fix query fail when there are timestamp or time columns after generated column (#7469))
    {}

    // Rough set operator
    RSOperatorPtr rs_operator;
    // Filter expression actions and the name of the tmp filter column
    // Used construct the FilterBlockInputStream
    ExpressionActionsPtr before_where;
    String filter_column_name;
    // The columns needed by the filter expression
    ColumnDefines filter_columns;
    // The expression actions used to cast the timestamp/datetime column
    ExpressionActionsPtr extra_cast;
    // If the extra_cast is not null, the types of the columns may be changed
<<<<<<< HEAD
    ColumnDefines columns_after_cast;
=======
    ColumnDefinesPtr columns_after_cast;
>>>>>>> 698fdde3ff (Fix query fail when there are timestamp or time columns after generated column (#7469))
};

} // namespace DB::DM
