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

#include <DataStreams/BlockIO.h>


namespace DB
{
/** Interpreters interface for different queries.
  */
class IInterpreter
{
public:
    /** For queries that return a result (SELECT and similar), sets in BlockIO a stream from which you can read this result.
      * For queries that receive data (INSERT), sets a thread in BlockIO where you can write data.
      * For queries that do not require data and return nothing, BlockIO will be empty.
      */
    virtual BlockIO execute() = 0;

    virtual ~IInterpreter() {}
};

} // namespace DB
