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

#include "ComplexKeyCacheDictionary.h"

namespace DB
{
namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
}

#define DECLARE(TYPE)                                                                                            \
    void ComplexKeyCacheDictionary::get##TYPE(                                                                   \
        const std::string & attribute_name,                                                                      \
        const Columns & key_columns,                                                                             \
        const DataTypes & key_types,                                                                             \
        PaddedPODArray<TYPE> & out) const                                                                        \
    {                                                                                                            \
        dict_struct.validateKeyTypes(key_types);                                                                 \
                                                                                                                 \
        auto & attribute = getAttribute(attribute_name);                                                         \
        if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))                        \
            throw Exception{                                                                                     \
                name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type), \
                ErrorCodes::TYPE_MISMATCH};                                                                      \
                                                                                                                 \
        const auto null_value = std::get<TYPE>(attribute.null_values);                                           \
                                                                                                                 \
        getItemsNumber<TYPE>(attribute, key_columns, out, [&](const size_t) { return null_value; });             \
    }
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(UInt128)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
#undef DECLARE
} // namespace DB
