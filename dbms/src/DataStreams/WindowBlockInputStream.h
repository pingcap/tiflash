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

#include <Common/Decimal.h>
#include <Common/FmtUtils.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/WindowTransformAction.h>
#include <Interpreters/WindowDescription.h>
#include <WindowFunctions/WindowUtils.h>

namespace DB
{
class WindowBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "Window";

public:
    WindowBlockInputStream(
        const BlockInputStreamPtr & input,
        const WindowDescription & window_description_,
        const String & req_id);

    Block getHeader() const override { return action.output_header; }

    String getName() const override { return NAME; }

protected:
    Block readImpl() override;
    void appendInfo(FmtBuffer & buffer) const override;
    bool returnIfCancelledOrKilled();

private:
    WindowTransformAction action;
};
} // namespace DB
