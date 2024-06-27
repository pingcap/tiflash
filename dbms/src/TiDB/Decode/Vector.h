// Copyright 2024 PingCAP, Inc.
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

#include <IO/Buffer/WriteBuffer.h>
#include <common/StringRef.h>
#include <common/types.h>

#include <compare>

namespace DB
{

class VectorDistanceSIMDFeatures
{
public:
    static std::vector<std::string> get();
};

class VectorFloat32Ref
{
public:
    explicit VectorFloat32Ref(const Float32 * elements, size_t n);

    explicit VectorFloat32Ref(const StringRef & data)
        : VectorFloat32Ref(reinterpret_cast<const Float32 *>(data.data), data.size / sizeof(Float32))
    {}

    size_t size() const { return elements_n; }

    bool empty() const { return size() == 0; }

    const Float32 & operator[](size_t n) const { return elements[n]; }

    void checkDims(VectorFloat32Ref b) const;

    Float64 l2SquaredDistance(VectorFloat32Ref b) const;

    Float64 l2Distance(VectorFloat32Ref b) const { return std::sqrt(l2SquaredDistance(b)); }

    Float64 innerProduct(VectorFloat32Ref b) const;

    Float64 negativeInnerProduct(VectorFloat32Ref b) const { return innerProduct(b) * -1; }

    Float64 cosineDistance(VectorFloat32Ref b) const;

    Float64 l1Distance(VectorFloat32Ref b) const;

    Float64 l2Norm() const;

    std::strong_ordering operator<=>(const VectorFloat32Ref & b) const;

    String toString() const;

    void toStringInBuffer(WriteBuffer & write_buffer) const;

private:
    const Float32 * elements;
    const size_t elements_n;
};

} // namespace DB
