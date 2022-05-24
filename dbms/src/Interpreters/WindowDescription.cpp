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

#include <Core/Field.h>
#include <Interpreters/WindowDescription.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

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
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unknowed frame type {}",
                        type);
    }
}

void WindowDescription::setWindowFrame(const tipb::WindowFrame & frame_)
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
