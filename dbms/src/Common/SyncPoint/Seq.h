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

#include <Common/SyncPoint/SeqOp.h>

#include <memory>
#include <vector>

namespace DB
{

class SyncPointSequence
{
public:
    SyncPointSequence() = default;

    ~SyncPointSequence()
    {
        disable();
    }

    void disable()
    {
        if (disabled)
            return;
        for (auto iter = ops.rbegin(); iter != ops.rend(); iter++)
            (*iter)->disable();
        disabled = true;
    }

    void execute()
    {
        for (auto & op : ops)
            (*op)();
    }

    template <class T>
    friend SyncPointSequence & operator<<(SyncPointSequence & out, T && v);

private:
    std::vector<std::unique_ptr<SyncPointOps::BaseOp>> ops{};
    bool disabled = false;
};

template <class T>
SyncPointSequence & operator<<(SyncPointSequence & out, T && v)
{
    static_assert(std::is_base_of<SyncPointOps::BaseOp, T>::value, "T must derive from SyncPointOps::BaseOp");

    auto op = std::make_unique<T>(v);
    op->enable();
    out.ops.push_back(std::move(op));
    return out;
}

template SyncPointSequence & operator<<(SyncPointSequence &, SyncPointOps::WaitAndPause &&);
template SyncPointSequence & operator<<(SyncPointSequence &, SyncPointOps::Next &&);
template SyncPointSequence & operator<<(SyncPointSequence &, SyncPointOps::WaitAndNext &&);
template SyncPointSequence & operator<<(SyncPointSequence &, SyncPointOps::Call &&);

} // namespace DB
