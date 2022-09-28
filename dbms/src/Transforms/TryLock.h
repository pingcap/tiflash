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

namespace DB
{
template <typename Mutex>
class TryLock
{
public:
    explicit TryLock(Mutex & m_) noexcept
        : m(m_)
    {
        is_locked = m.try_lock();
    }

    ~TryLock()
    {
        if (is_locked)
            m.unlock();
    }

    bool isLocked() const { return is_locked; }

private:
    Mutex & m;
    bool is_locked = false;
};
} // namespace DB
