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

#include <functional>

namespace DB
{
class ResultHandler
{
public:
    using Handler = std::function<void(const Block &)>;
    explicit ResultHandler(Handler handler_)
        : handler(handler_)
        , is_ignored(false)
    {}
    ResultHandler()
        : is_ignored(true)
    {}

    explicit operator bool() const noexcept { return !is_ignored; }

    void operator()(const Block & block) const { handler(block); }

private:
    Handler handler;

    bool is_ignored;
};
} // namespace DB
