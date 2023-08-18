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

#include <Dictionaries/ComplexKeyCacheDictionary.h>

namespace DB
{

void ComplexKeyCacheDictionary::setAttributeValue(Attribute & attribute, const size_t idx, const Field & value) const
{
    switch (attribute.type)
    {
    case AttributeUnderlyingType::UInt8:
        std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = value.get<UInt64>();
        break;
    case AttributeUnderlyingType::UInt16:
        std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = value.get<UInt64>();
        break;
    case AttributeUnderlyingType::UInt32:
        std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = value.get<UInt64>();
        break;
    case AttributeUnderlyingType::UInt64:
        std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = value.get<UInt64>();
        break;
    case AttributeUnderlyingType::UInt128:
        std::get<ContainerPtrType<UInt128>>(attribute.arrays)[idx] = value.get<UInt128>();
        break;
    case AttributeUnderlyingType::Int8:
        std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = value.get<Int64>();
        break;
    case AttributeUnderlyingType::Int16:
        std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = value.get<Int64>();
        break;
    case AttributeUnderlyingType::Int32:
        std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = value.get<Int64>();
        break;
    case AttributeUnderlyingType::Int64:
        std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = value.get<Int64>();
        break;
    case AttributeUnderlyingType::Float32:
        std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = value.get<Float64>();
        break;
    case AttributeUnderlyingType::Float64:
        std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = value.get<Float64>();
        break;
    case AttributeUnderlyingType::String:
    {
        const auto & string = value.get<String>();
        auto & string_ref = std::get<ContainerPtrType<StringRef>>(attribute.arrays)[idx];
        const auto & null_value_ref = std::get<String>(attribute.null_values);

        /// free memory unless it points to a null_value
        if (string_ref.data && string_ref.data != null_value_ref.data())
            string_arena->free(const_cast<char *>(string_ref.data), string_ref.size);

        const auto size = string.size();
        if (size != 0)
        {
            auto string_ptr = string_arena->alloc(size + 1);
            std::copy(string.data(), string.data() + size + 1, string_ptr);
            string_ref = StringRef{string_ptr, size};
        }
        else
            string_ref = {};

        break;
    }
    }
}

} // namespace DB
