#include <Common/FieldVisitors.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <IO/Operators.h>
#include <Interpreters/WindowDescription.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

std::string WindowFrame::getFrameTypeName(FrameType type) const
{
    switch (type)
    {
    case FrameType::Rows:
        return "rows";
    case FrameType::Groups:
        return "groups";
    case FrameType::Ranges:
        return "range";
    default:
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid frame type", type);
    }
}

WindowFrame::BoundaryType getBoundaryTypeFromTipb(tipb::WindowFrameBound bound)
{
    if (bound.type() == tipb::WindowBoundType::CurrentRow)
        return WindowFrame::BoundaryType::Current;
    else if (bound.unbounded())
        return WindowFrame::BoundaryType::Unbounded;
    else
        return WindowFrame::BoundaryType::Offset;
}

WindowFrame::FrameType getFrameTypeFromTipb(tipb::WindowFrameType type)
{
    switch (type)
    {
    case tipb::WindowFrameType::Ranges:
        return WindowFrame::FrameType::Ranges;
    case tipb::WindowFrameType::Rows:
        return WindowFrame::FrameType::Rows;
    case tipb::WindowFrameType::Groups:
        return WindowFrame::FrameType::Groups;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Unknowed frame type {}",
                    type);
}

void WindowDescription::setWindowFrame(tipb::WindowFrame frame_)
{
    frame.type = getFrameTypeFromTipb(frame_.type());
    frame.begin_offset = frame_.start().offset();
    frame.begin_type = getBoundaryTypeFromTipb(frame_.start());
    frame.begin_preceding = (frame_.start().type() == tipb::WindowBoundType::Preceding);
    frame.end_offset = frame_.end().offset();
    frame.end_type = getBoundaryTypeFromTipb(frame_.end());
    frame.end_preceding = (frame_.end().type() == tipb::WindowBoundType::Preceding);
    frame.is_default = false;
}
} // namespace DB
