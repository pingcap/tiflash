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

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <WindowFunctions/IWindowFunction.h>
#include <tipb/select.pb.h>

namespace DB
{
struct WindowFunctionDescription
{
    WindowFunctionPtr window_function;
    AggregateFunctionPtr aggregate_function;
    Array parameters;
    ColumnNumbers arguments;
    Names argument_names;
    String column_name;
};

using WindowFunctionDescriptions = std::vector<WindowFunctionDescription>;

struct WindowFrame
{
    enum class FrameType
    {
        Rows,
        Groups,
        Ranges
    };
    enum class BoundaryType
    {
        Unbounded,
        Current,
        Offset
    };

    // This flag signifies that the frame properties were not set explicitly by
    // user, but the fields of this structure still have to contain proper values
    // for the default frame of RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
    bool is_default = true;

    FrameType type = FrameType::Ranges;

    BoundaryType begin_type = BoundaryType::Unbounded;
    UInt64 begin_offset = 0;
    bool begin_preceding = true;
    Int32 begin_range_auxiliary_column_index = -1;
    tipb::RangeCmpDataType begin_cmp_data_type;

    BoundaryType end_type = BoundaryType::Unbounded;
    UInt64 end_offset = 0;
    bool end_preceding = false;
    Int32 end_range_auxiliary_column_index = -1;
    tipb::RangeCmpDataType end_cmp_data_type;

    bool operator==(const WindowFrame & other) const
    {
        // We don't compare is_default because it's not a real property of the
        // frame, and only influences how we display it.
        return other.type == type && other.begin_type == begin_type && other.begin_offset == begin_offset
            && other.begin_preceding == begin_preceding && other.end_type == end_type && other.end_offset == end_offset
            && other.end_preceding == end_preceding;
    }
};

String frameTypeToString(const WindowFrame::FrameType & type);
String boundaryTypeToString(const WindowFrame::BoundaryType & type);

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
struct WindowDescription
{
    ExpressionActionsPtr before_window;

    ExpressionActionsPtr after_window;

    NamesAndTypes add_columns;

    NamesAndTypes after_window_columns;

    // We don't care about the particular order of keys for PARTITION BY, only
    // that they are sorted. For now we always require ASC, but we could be more
    // flexible and match any direction, or even different order of columns.
    SortDescription partition_by;

    SortDescription order_by;

    WindowFrame frame;

    // The window functions that are calculated for this window.
    WindowFunctionDescriptions window_functions_descriptions;

    // Mark the order by column type to avoid type judge
    // each time we update the start/end frame position.
    TypeIndex order_by_col_type = TypeIndex::Nothing;

    TypeIndex begin_aux_col_type = TypeIndex::Nothing;
    TypeIndex end_aux_col_type = TypeIndex::Nothing;

    // ascending or descending for order by column
    // only used for range frame type
    bool is_desc;

    // only used for range frame type
    bool is_order_by_col_nullable;
    bool is_begin_aux_col_nullable;
    bool is_end_aux_col_nullable;

    void setWindowFrame(const tipb::WindowFrame & frame_);

    void fillArgColumnNumbers();
};

} // namespace DB
