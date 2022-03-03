#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/AggregateDescription.h>
#include <Parsers/IAST.h>
#include <WindowFunctions/WindowFunction.h>
#include <tipb/select.pb.h>


namespace DB
{
struct WindowFunctionDescription
{
    WindowFunctionPtr window_function;
    Array parameters;
    ColumnNumbers arguments;
    Names argument_names;
    std::string column_name;
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

    // UNBOUNDED FOLLOWING for the frame end is forbidden by the standard, but for
    // uniformity the begin_preceding still has to be set to true for UNBOUNDED
    // frame start.
    // Offset might be both preceding and following, controlled by begin_preceding,
    // but the offset value must be positive.
    BoundaryType begin_type = BoundaryType::Unbounded;
    Field begin_offset = Field(UInt64(0));
    bool begin_preceding = true;

    // Here as well, Unbounded can only be UNBOUNDED FOLLOWING, and end_preceding
    // must be false.
    BoundaryType end_type = BoundaryType::Current;
    Field end_offset = Field(UInt64(0));
    bool end_preceding = false;

    std::string getFrameTypeName(FrameType type) const;


    // Throws BAD_ARGUMENTS exception if the frame definition is incorrect, e.g.
    // the frame start comes later than the frame end.
    void checkValid() const;

    std::string toString() const;
    void toString(WriteBuffer & buf) const;

    bool operator==(const WindowFrame & other) const
    {
        // We don't compare is_default because it's not a real property of the
        // frame, and only influences how we display it.
        return other.type == type
            && other.begin_type == begin_type
            && other.begin_offset == begin_offset
            && other.begin_preceding == begin_preceding
            && other.end_type == end_type
            && other.end_offset == end_offset
            && other.end_preceding == end_preceding;
    }
};
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
struct WindowDescription
{
    std::string window_name;

    ExpressionActionsPtr before_window;

    ExpressionActionsPtr before_window_select;

    std::vector<NameAndTypePair> add_columns;

    // We don't care about the particular order of keys for PARTITION BY, only
    // that they are sorted. For now we always require ASC, but we could be more
    // flexible and match any direction, or even different order of columns.
    SortDescription partition_by;

    SortDescription order_by;

    // To calculate the window function, we sort input data first by PARTITION BY,
    // then by ORDER BY. This field holds this combined sort order.
    SortDescription full_sort_description;

    WindowFrame frame;

    // The window functions that are calculated for this window.
    WindowFunctionDescriptions window_functions_descriptions;

    AggregateDescriptions aggregate_descriptions;

    std::string dump() const;

    void checkValid() const;

    void setWindowFrame(tipb::WindowFrame frame);
};

using WindowDescriptions = std::vector<WindowDescription>;

} // namespace DB
