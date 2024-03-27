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

#include <Common/Exception.h>
#include <Operators/OperatorHelper.h>

#include <magic_enum.hpp>

namespace DB
{
void assertOperatorStatus(OperatorStatus status, std::initializer_list<OperatorStatus> expect_running_statuses)
{
    switch (status)
    {
    // cancel status, waiting and io status can be returned in all method of operator.
    case OperatorStatus::CANCELLED:
    case OperatorStatus::WAITING:
    case OperatorStatus::WAIT_FOR_NOTIFY:
    case OperatorStatus::IO_IN:
    case OperatorStatus::IO_OUT:
        return;
    default:
    {
        for (const auto & expect_running_status : expect_running_statuses)
        {
            if (expect_running_status == status)
                return;
        }
        RUNTIME_ASSERT(false, "Unexpected operator status {}", magic_enum::enum_name(status));
    }
    }
}
} // namespace DB
