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

#include <DataStreams/WindowBlockInputStream.h>
#include <Operators/Operator.h>

namespace DB
{
class WindowTransformOp : public TransformOp
{
public:
    WindowTransformOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id_,
        const WindowDescription & window_description_);

    String getName() const override { return "WindowTransformOp"; }

protected:
    void operateSuffixImpl() override;

    ReturnOpStatus transformImpl(Block & block) override;
    ReturnOpStatus tryOutputImpl(Block & block) override;

    void transformHeaderImpl(Block & header_) override;

private:
    WindowDescription window_description;
    std::unique_ptr<WindowTransformAction> action;
};
} // namespace DB
