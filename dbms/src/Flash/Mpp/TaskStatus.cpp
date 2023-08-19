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
#include <Flash/Mpp/TaskStatus.h>

namespace DB
{
StringRef taskStatusToString(const TaskStatus & status)
{
    switch (status)
    {
    case INITIALIZING:
        return "INITIALIZING";
    case RUNNING:
        return "RUNNING";
    case FINISHED:
        return "FINISHED";
    case CANCELLED:
        return "CANCELLED";
    default:
        throw Exception("Unknown TaskStatus");
    }
}
} // namespace DB
