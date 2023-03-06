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

#include <Interpreters/ExpressionActions.h>

#include "RSOperator.h"

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
                   const String filter_column_name_)
        : rs_operator(rs_operator_)
        , beofre_where(beofre_where_)
        , filter_columns(std::move(filter_columns_))
        , filter_column_name(std::move(filter_column_name_))
    {}

    explicit PushDownFilter(const RSOperatorPtr & rs_operator_)
        : rs_operator(rs_operator_)
        , beofre_where(nullptr)
        , filter_columns({})
    {}

    RSOperatorPtr rs_operator;
    ExpressionActionsPtr beofre_where;
    ColumnDefines filter_columns;
    String filter_column_name;
};

} // namespace DB::DM