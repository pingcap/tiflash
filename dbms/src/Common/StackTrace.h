// Copyright 2022 PingCAP, Ltd.
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

#include <string>

#define STACK_TRACE_MAX_DEPTH 32


/// Lets you get a stacktrace
class StackTrace
{
public:
    /// The stacktrace is captured when the object is created
    StackTrace();

    /// Print to string
    std::string toString() const;

private:
    using Frame = void *;
    Frame frames[STACK_TRACE_MAX_DEPTH];
    size_t frames_size;
};
