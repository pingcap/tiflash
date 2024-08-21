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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/expression.pb.h>
#pragma GCC diagnostic pop

#include <Common/MyTime.h>
#include <Core/Types.h>
#include <TiDB/Schema/TiDBTypes.h>

namespace DB
{
class TiDBTime
{
public:
    TiDBTime(UInt64 packed, const tipb::FieldType & field_type)
        : my_date_time(packed)
    {
        time_type = field_type.tp();
        fsp = field_type.decimal();
    }
    MyDateTime my_date_time;
    UInt8 time_type;
    Int32 fsp;
    UInt64 toChunkTime() const
    {
        UInt64 ret = 0;
        ret |= (my_date_time.toCoreTime() & MyTimeBase::CORE_TIME_BIT_FIELD_MASK);
        if (time_type == TiDB::TypeDate)
        {
            ret |= MyTimeBase::FSPTT_FOR_DATE;
            return ret;
        }
        if (fsp > 0)
            ret |= static_cast<UInt64>(fsp) << 1u;
        if (time_type == TiDB::TypeTimestamp)
            ret |= 1u;
        return ret;
    }
};
} // namespace DB
