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

#include <Core/Types.h>
#include <common/DateLUT.h>

namespace tipb
{
class DAGRequest;
}

namespace DB
{
/// A class used to store timezone info, currently only used when handling coprocessor request
struct TimezoneInfo
{
    String timezone_name;
    Int64 timezone_offset = 0;
    bool is_utc_timezone = false;
    bool is_name_based = false;
    const DateLUTImpl * timezone = nullptr;

    void init()
    {
        is_name_based = true;
        timezone_offset = 0;
        timezone = &DateLUT::instance();
        timezone_name = timezone->getTimeZone();
        is_utc_timezone = timezone_name == "UTC";
    }

    void resetByDAGRequest(const tipb::DAGRequest & rqst);
    void resetByTimezoneName(const String & timezone_name);
    void resetByTimezoneOffset(Int64 timezone_offset);
};

} // namespace DB
