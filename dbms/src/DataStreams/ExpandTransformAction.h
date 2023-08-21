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

#include <Interpreters/Expand2.h>

namespace DB
{

struct ExpandTransformAction
{
public:
    ExpandTransformAction(const Block & header_, const Expand2Ptr & expand_);

    void transform(Block & block);
    bool tryOutput(Block & block);
    Block getHeader() const;

private:
    Block header;
    Expand2Ptr expand;
    Block block_cache;
    size_t i_th_project;
};

} // namespace DB
