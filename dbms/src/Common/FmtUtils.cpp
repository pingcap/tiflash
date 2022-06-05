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

#include <Common/FmtUtils.h>

namespace DB
{
FmtBuffer & FmtBuffer::append(std::string_view s)
{
    buffer.append(s.data(), s.data() + s.size());
    return *this;
}

std::string FmtBuffer::toString() const
{
    return fmt::to_string(buffer);
}

void FmtBuffer::resize(size_t count)
{
    buffer.resize(count);
}
void FmtBuffer::reserve(size_t capacity)
{
    buffer.reserve(capacity);
}
void FmtBuffer::clear()
{
    buffer.clear();
}

} // namespace DB
