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

#include <Common/FieldVisitors.h>
#include <Common/SipHash.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{


String ASTLiteral::getColumnNameImpl() const
{
    /// Special case for very large arrays. Instead of listing all elements, will use hash of them.
    /// (Otherwise column name will be too long, that will lead to significant slowdown of expression analysis.)
    if (value.getType() == Field::Types::Array
        && value.get<const Array &>().size() > 100) /// 100 - just arbitary value.
    {
        SipHash hash;
        applyVisitor(FieldVisitorHash(hash), value);
        UInt64 low, high;
        hash.get128(low, high);
        return "__array_" + toString(low) + "_" + toString(high);
    }

    return applyVisitor(FieldVisitorToString(), value);
}

} // namespace DB
