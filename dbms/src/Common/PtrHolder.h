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

#include <mutex>
#include <optional>

namespace DB
{
template <typename Ptr>
class PtrHolder
{
public:
    void set(Ptr && obj_)
    {
        assert(obj_);
        std::lock_guard lock(mu);
        obj = std::move(obj_);
    }

    auto tryGet()
    {
        std::optional<decltype(obj.get())> res;
        std::lock_guard lock(mu);
        if (obj != nullptr)
            res.emplace(obj.get());
        return res;
    }

    auto * operator->()
    {
        std::lock_guard lock(mu);
        assert(obj != nullptr);
        return obj.get();
    }

    auto & operator*()
    {
        std::lock_guard lock(mu);
        assert(obj != nullptr);
        return *obj.get();
    }

private:
    std::mutex mu;
    Ptr obj;
};
} // namespace DB
