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

#include <Storages/DeltaMerge/Index/RSResult.h>

namespace DB::DM
{
const RSResult RSResult::Some(RSResult::ValueResult::Some, false);
const RSResult RSResult::None(RSResult::ValueResult::None, false);
const RSResult RSResult::All(RSResult::ValueResult::All, false);
const RSResult RSResult::SomeNull(RSResult::ValueResult::Some, true);
const RSResult RSResult::NoneNull(RSResult::ValueResult::None, true);
const RSResult RSResult::AllNull(RSResult::ValueResult::All, true);

RSResult::ValueResult RSResult::logicalNot(ValueResult v) noexcept
{
    switch (v)
    {
    case ValueResult::Some:
        return ValueResult::Some;
    case ValueResult::None:
        return ValueResult::All;
    case ValueResult::All:
        return ValueResult::None;
    }
}

RSResult::ValueResult RSResult::logicalAnd(ValueResult v0, ValueResult v1) noexcept
{
    if (v0 == ValueResult::None || v1 == ValueResult::None)
        return ValueResult::None;
    if (v0 == ValueResult::All && v1 == ValueResult::All)
        return ValueResult::All;
    return ValueResult::Some;
}

RSResult::ValueResult RSResult::logicalOr(ValueResult v0, ValueResult v1) noexcept
{
    if (v0 == ValueResult::All || v1 == ValueResult::All)
        return ValueResult::All;
    else if (v0 == ValueResult::Some || v1 == ValueResult::Some)
        return ValueResult::Some;
    return ValueResult::None;
}
} // namespace DB::DM
