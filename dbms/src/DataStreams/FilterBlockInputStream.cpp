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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes


FilterBlockInputStream::FilterBlockInputStream(
    const BlockInputStreamPtr & input,
    const ExpressionActionsPtr & expression_,
    const String & filter_column_name,
    const String & req_id)
    : filter_transform_action(input->getHeader(), expression_, filter_column_name)
    , log(Logger::get(req_id))
{
    children.push_back(input);
}

Block FilterBlockInputStream::getTotals()
{
    if (auto * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
    {
        totals = child->getTotals();
        filter_transform_action.getExperssion()->executeOnTotals(totals);
    }

    return totals;
}


Block FilterBlockInputStream::getHeader() const
{
    return filter_transform_action.getHeader();
}

Block FilterBlockInputStream::readImpl()
{
    Block res;

    if (filter_transform_action.alwaysFalse())
        return res;

    /// Until non-empty block after filtering or end of stream.
    while (true)
    {
        res = children.back()->read();

        if (!res)
            return res;

        if (filter_transform_action.transform(res))
            return res;
    }
}

} // namespace DB
