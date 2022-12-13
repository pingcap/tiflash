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


#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
class RepeatSourceBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "RepeatSource";

public:
    RepeatSourceBlockInputStream(
        const BlockInputStreamPtr & input,
        ExpressionActionsPtr repeat_source_actions_)
        : repeat_source_actions(repeat_source_actions_)
    {
        children.push_back(input);
    }
    String getName() const override { return NAME; }
    Block getHeader() const override;
    void appendInfo(FmtBuffer & buffer) const override;

protected:
    Block readImpl() override;

private:
    ExpressionActionsPtr repeat_source_actions;
};

} // namespace DB

