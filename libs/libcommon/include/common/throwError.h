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

#include <stdexcept>


/// Throw DB::Exception-like exception before its definition.
/// DB::Exception derived from Poco::Exception derived from std::exception.
/// DB::Exception generally caught as Poco::Exception. std::exception generally has other catch blocks and could lead to other outcomes.
/// DB::Exception is not defined yet. It'd better to throw Poco::Exception but we do not want to include any big header here, even <string>.
/// So we throw some std::exception instead in the hope its catch block is the same as DB::Exception one.
template <typename T>
[[noreturn]] inline void throwError(const T & err)
{
    throw std::runtime_error(err);
}
