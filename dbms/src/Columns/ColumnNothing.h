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

#include <Columns/IColumnDummy.h>

namespace DB
{
class ColumnNothing final : public COWPtrHelper<IColumnDummy, ColumnNothing>
{
private:
    friend class COWPtrHelper<IColumnDummy, ColumnNothing>;

    explicit ColumnNothing(size_t s_) { s = s_; }

    ColumnNothing(const ColumnNothing &) = default;

public:
    const char * getFamilyName() const override { return "Nothing"; }
    MutableColumnPtr cloneDummy(size_t s) const override { return ColumnNothing::create(s); };

    bool canBeInsideNullable() const override { return true; }
};

} // namespace DB
