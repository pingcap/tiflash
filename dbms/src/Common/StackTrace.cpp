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

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif
#include <Common/StackTrace.h>
#include <common/demangle.h>
#include <execinfo.h>
StackTrace::StackTrace()
{
    frames_size = backtrace(frames, STACK_TRACE_MAX_DEPTH);
}

std::string StackTrace::toString() const
{
    DB::FmtBuffer output;

    for (size_t f = 0; f < frames_size; ++f)
    {
        output.append("\n");
        auto demangle_func = [](const char * name) {
            int status = 0;
            auto result = demangle(name, status);
            return std::pair<std::string, int>{result, status};
        };
        addr2line(demangle_func, output, frames[f]);
    }

    return output.toString();
}
