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

#include <Flash/Pipeline/Transform.h>
#include <Flash/Pipeline/Sink.h>
#include <Flash/Pipeline/Source.h>

namespace DB
{
class Task
{
public:
private:
    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;
};
using TaskPtr = std::unique_ptr<Task>;
} // namespace DB
