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

#include <common/likely.h>
#include <common/types.h>

#include <cmath>

namespace DB
{
template <typename VectorType>
class VectorWriter
{
public:
    using Position = char *;

    explicit VectorWriter(VectorType & vector_, size_t initial_size = 16)
        : vector(vector_)
    {
        if (vector.size() < initial_size)
            vector.resize(initial_size);
        pos = reinterpret_cast<Position>(vector.data());
        end = reinterpret_cast<Position>(vector.data() + vector.size());
    }

    inline void write(char x)
    {
        reserveForNextSize(1);
        *pos = x;
        ++pos;
    }

    void write(const char * from, size_t n)
    {
        if (unlikely(n == 0))
            return;
        reserveForNextSize(n);
        std::memcpy(pos, from, n);
        pos += n;
    }

    void setOffset(size_t new_offset)
    {
        if (new_offset > vector.size())
        {
            size_t request_size = (new_offset - count());
            reserveForNextSize(request_size);
        }
        pos = reinterpret_cast<Position>(vector.data() + new_offset);
    }

    void advance(size_t n) { setOffset(offset() + n); }

    size_t offset() { return pos - reinterpret_cast<Position>(vector.data()); }

    size_t count() { return offset(); }

    ~VectorWriter()
    {
        vector.resize(count());
        pos = nullptr;
        end = nullptr;
    }

private:
    size_t remainingSize() const { return static_cast<size_t>(end - pos); }

    void reserve(size_t new_size)
    {
        size_t pos_offset = offset();
        vector.resize(new_size);
        pos = reinterpret_cast<Position>(vector.data() + pos_offset);
        end = reinterpret_cast<Position>(vector.data() + vector.size());
    }

    void reserveForNextSize(size_t request_size = 1)
    {
        assert(request_size > 0);
        if (remainingSize() < request_size)
        {
            size_t old_size = vector.size();
            size_t new_size = std::max(old_size + request_size, std::ceil(old_size * 1.5));
            reserve(new_size);
        }
    }

private:
    static_assert(sizeof(typename VectorType::value_type) == sizeof(char));
    VectorType & vector;

    Position pos = nullptr;
    Position end = nullptr;
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
