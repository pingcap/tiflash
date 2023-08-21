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

#include <Core/Block.h>

#include <atomic>
#include <memory>

namespace DB
{
class LocalLimitTransformAction
{
public:
    LocalLimitTransformAction(const Block & header_, size_t limit_)
        : header(header_)
        , limit(limit_)
    {}

    bool transform(Block & block);

    Block getHeader() const { return header; }
    size_t getLimit() const { return limit; }

private:
    const Block header;
    const size_t limit;
    size_t pos = 0;
};
using LocalLimitPtr = std::shared_ptr<LocalLimitTransformAction>;

class GlobalLimitTransformAction
{
public:
    GlobalLimitTransformAction(const Block & header_, size_t limit_)
        : header(header_)
        , limit(limit_)
    {}

    bool transform(Block & block);

    Block getHeader() const { return header; }
    size_t getLimit() const { return limit; }

private:
    const Block header;
    const size_t limit;
    std::atomic_size_t pos{0};
};
using GlobalLimitPtr = std::shared_ptr<GlobalLimitTransformAction>;

} // namespace DB
