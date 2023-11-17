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

#include <common/types.h>

namespace DB
{
template <typename VectorType, size_t initial_size = 32>
class VectorWriter
{
public:
    using Position = char *;

    explicit VectorWriter(VectorType & vector_)
        : vector(vector_)
    {
        if (vector.empty())
        {
            vector.resize(initial_size);
        }
        pos = reinterpret_cast<Position>(vector.data());
        end = reinterpret_cast<Position>(vector.data() + vector.size());
    }

    inline void write(char x)
    {
        reserve(1);
        *pos = x;
        ++pos;
    }

    void write(const char * from, size_t n)
    {
        size_t bytes_copied = 0;

        while (bytes_copied < n)
        {
            size_t remaining_bytes = n - bytes_copied;
            reserve(remaining_bytes);
            size_t bytes_to_copy = std::min(static_cast<size_t>(end - pos), remaining_bytes);
            std::memcpy(pos, from + bytes_copied, bytes_to_copy);
            pos += bytes_to_copy;
            bytes_copied += bytes_to_copy;
        }
    }

    void resizeFill(size_t n)
    {
        size_t old_cap = vector.size();
        size_t new_request_cap = count() + n;
        if (old_cap >= new_request_cap)
        {
            pos += n;
            return;
        }
        size_t pos_offset = offset();
        vector.resize(new_request_cap);
        pos = reinterpret_cast<Position>(vector.data() + pos_offset + n);
        end = reinterpret_cast<Position>(vector.data() + vector.size());
    }

    size_t offset() { return pos - reinterpret_cast<Position>(vector.data()); }

    size_t count() { return offset(); }

    void setOffset(size_t new_offset)
    {
        if (new_offset <= vector.size())
        {
            pos = reinterpret_cast<Position>(vector.data() + new_offset);
        }
        else
        {
            vector.resize(new_offset);
            pos = reinterpret_cast<Position>(vector.data() + vector.size());
            end = pos;
        }
    }

    ~VectorWriter()
    {
        vector.resize(count());
        pos = nullptr;
        end = nullptr;
    }

private:
    void reserve(size_t expect_new = 0)
    {
        if (pos == end)
        {
            size_t old_size = vector.size();
            size_t pos_offset = offset();
            size_t append_new = std::max(1, std::max(expect_new, old_size * (size_multiplier - 1)));
            vector.resize(old_size + append_new);
            pos = reinterpret_cast<Position>(vector.data() + pos_offset);
            end = reinterpret_cast<Position>(vector.data() + vector.size());
        }
    }

private:
    static_assert(sizeof(typename VectorType::value_type) == sizeof(char));
    VectorType & vector;

    Position pos = nullptr;
    Position end = nullptr;

    static constexpr size_t size_multiplier = 2;
};

template <typename VectorWriter>
inline void writeChar(char x, VectorWriter & writer)
{
    writer.write(x);
}

template <typename VectorWriter>
inline void writeVarUInt(UInt64 x, VectorWriter & writer)
{
    while (x >= 0x80)
    {
        writeChar(static_cast<UInt8>(x) | 0x80, writer);
        x >>= 7;
    }
    writeChar(x, writer);
}
} // namespace DB
