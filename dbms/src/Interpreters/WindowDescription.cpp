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

#include <Core/Field.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/WindowDescription.h>
#include <tipb/expression.pb.h>

#include <magic_enum.hpp>
#include <string>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

WindowFrame::BoundaryType getBoundaryTypeFromTipb(const tipb::WindowFrameBound & bound)
{
    if (bound.type() == tipb::WindowBoundType::CurrentRow)
        return WindowFrame::BoundaryType::Current;
    else if (bound.unbounded())
        return WindowFrame::BoundaryType::Unbounded;
    else
        return WindowFrame::BoundaryType::Offset;
}

WindowFrame::FrameType getFrameTypeFromTipb(const tipb::WindowFrameType & type)
{
    switch (type)
    {
    case tipb::WindowFrameType::Ranges:
        return WindowFrame::FrameType::Ranges;
    case tipb::WindowFrameType::Rows:
        return WindowFrame::FrameType::Rows;
    case tipb::WindowFrameType::Groups:
        return WindowFrame::FrameType::Groups;
    default:
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown frame type {}", fmt::underlying(type));
    }
}

void WindowDescription::setWindowFrame(const tipb::WindowFrame & frame_)
{
    frame.type = getFrameTypeFromTipb(frame_.type());
    frame.begin_offset = frame_.start().offset();
    frame.begin_type = getBoundaryTypeFromTipb(frame_.start());
    frame.begin_preceding = (frame_.start().type() == tipb::WindowBoundType::Preceding);
    frame.begin_cmp_data_type = frame_.start().cmp_data_type();
    frame.end_offset = frame_.end().offset();
    frame.end_type = getBoundaryTypeFromTipb(frame_.end());
    frame.end_preceding = (frame_.end().type() == tipb::WindowBoundType::Preceding);
    frame.end_cmp_data_type = frame_.end().cmp_data_type();
    frame.is_default = false;
}

void WindowDescription::fillArgColumnNumbers()
{
    const auto & before_window_header = before_window->getSampleBlock();
    for (auto & descr : window_functions_descriptions)
    {
        if (descr.arguments.empty())
        {
            for (const auto & name : descr.argument_names)
            {
                descr.arguments.emplace_back(before_window_header.getPositionByName(name));
            }
        }
    }
}

void WindowDescription::initNeedDecrease(bool has_agg)
{
    need_decrease = false;

    if (!has_agg)
        return;

    if (frame.begin_type == WindowFrame::BoundaryType::Unbounded
        && frame.end_type == WindowFrame::BoundaryType::Unbounded)
        return;

    if (frame.begin_type == WindowFrame::BoundaryType::Unbounded)
        return;

    if (frame.end_type == WindowFrame::BoundaryType::Unbounded)
    {
        need_decrease = true;
        return;
    }

    if (frame.type == WindowFrame::FrameType::Rows)
    {
        if (frame.begin_offset == 0 && frame.end_offset == 0)
            return;
        if (frame.begin_preceding == frame.end_preceding && frame.begin_offset == frame.end_offset)
            return;
    }
    need_decrease = true;
}

String frameTypeToString(const WindowFrame::FrameType & type)
{
    switch (type)
    {
    case WindowFrame::FrameType::Rows:
        return "Rows";
    case WindowFrame::FrameType::Groups:
        return "Groups";
    case WindowFrame::FrameType::Ranges:
        return "Ranges";
    default:
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown frame type {}", static_cast<Int32>(type));
    }
}

String boundaryTypeToString(const WindowFrame::BoundaryType & type)
{
    switch (type)
    {
    case WindowFrame::BoundaryType::Unbounded:
        return "Unbounded";
    case WindowFrame::BoundaryType::Current:
        return "Current";
    case WindowFrame::BoundaryType::Offset:
        return "Offset";
    default:
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown boundary type {}", magic_enum::enum_name(type));
    }
}
} // namespace DB
