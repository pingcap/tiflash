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


namespace DB
{
/// Up to what stage the SELECT query is executed or needs to be executed.
namespace QueryProcessingStage
{
/// Numbers matter - the later stage has a larger number.
enum Enum
{
    FetchColumns = 0, /// Only read/have been read the columns specified in the query.
    WithMergeableState = 1, /// Until the stage where the results of processing on different servers can be combined.
    Complete = 2, /// Completely.
};

inline const char * toString(UInt64 stage)
{
    static const char * data[] = {"FetchColumns", "WithMergeableState", "Complete"};
    return stage < 3
        ? data[stage]
        : "Unknown stage";
}
} // namespace QueryProcessingStage

} // namespace DB
